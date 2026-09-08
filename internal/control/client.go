package control

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// PairingBundle is the one-time portable client credential document.
type PairingBundle struct {
	Version    int        `json:"version"`
	PairingID  string     `json:"pairing_id"`
	Endpoint   string     `json:"endpoint"`
	TrustMode  string     `json:"trust_mode"`
	CAPEM      string     `json:"ca_pem,omitempty"`
	ServerName string     `json:"server_name"`
	SPKIPin    string     `json:"spki_pin,omitempty"`
	TokenID    string     `json:"token_id"`
	Secret     string     `json:"token_secret"`
	Scopes     []string   `json:"scopes"`
	ClientCert string     `json:"client_certificate_pem,omitempty"`
	ClientKey  string     `json:"client_private_key_pem,omitempty"`
	ClientID   string     `json:"client_id,omitempty"`
	Created    time.Time  `json:"created,omitempty"`
	Expires    *time.Time `json:"expires,omitempty"`
}

func certificateDetails(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("certificate file contains no certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

// CreatePairingBundle issues a token and binds it to explicit TLS trust.
func CreatePairingBundle(store *TokenStore, endpoint, certFile, caFile, serverName string, systemCA bool, clientCertFile, clientKeyFile string, scopes []string, expires *time.Time) (PairingBundle, error) {
	if endpoint == "" || certFile == "" {
		return PairingBundle{}, fmt.Errorf("TCP endpoint and certificate are required")
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		return PairingBundle{}, fmt.Errorf("endpoint must be a client-visible host:port, not a wildcard bind address")
	}
	certificate, err := certificateDetails(certFile)
	if err != nil {
		return PairingBundle{}, err
	}
	if serverName == "" {
		serverName = certificate.Subject.CommonName
		if len(certificate.DNSNames) != 0 {
			serverName = certificate.DNSNames[0]
		}
	}
	if serverName == "" {
		return PairingBundle{}, fmt.Errorf("server name is required")
	}
	if err := certificate.VerifyHostname(serverName); err != nil {
		return PairingBundle{}, fmt.Errorf("server certificate does not cover %s: %w", serverName, err)
	}
	if systemCA && caFile != "" {
		return PairingBundle{}, fmt.Errorf("system CA trust cannot be combined with an explicit CA")
	}
	record, secret, err := store.Create(scopes, expires)
	if err != nil {
		return PairingBundle{}, err
	}
	bundle := PairingBundle{Version: 1, Endpoint: endpoint, ServerName: serverName, TokenID: record.ID, Secret: secret, Scopes: record.Scopes}
	if (clientCertFile == "") != (clientKeyFile == "") {
		_ = store.Revoke(record.ID)
		return PairingBundle{}, fmt.Errorf("client certificate and key must be supplied together")
	}
	if clientCertFile != "" {
		clientCert, certErr := os.ReadFile(clientCertFile)
		clientKey, keyErr := os.ReadFile(clientKeyFile)
		if certErr != nil || keyErr != nil {
			_ = store.Revoke(record.ID)
			return PairingBundle{}, errors.Join(certErr, keyErr)
		}
		if _, pairErr := tls.X509KeyPair(clientCert, clientKey); pairErr != nil {
			_ = store.Revoke(record.ID)
			return PairingBundle{}, pairErr
		}
		bundle.ClientCert, bundle.ClientKey = string(clientCert), string(clientKey)
	}
	if systemCA {
		bundle.TrustMode = "system"
	} else if caFile != "" {
		caPEM, readErr := os.ReadFile(caFile)
		if readErr != nil {
			_ = store.Revoke(record.ID)
			return PairingBundle{}, readErr
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			_ = store.Revoke(record.ID)
			return PairingBundle{}, fmt.Errorf("CA file contains no certificates")
		}
		bundle.TrustMode, bundle.CAPEM = "ca", string(caPEM)
	} else {
		digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
		bundle.TrustMode, bundle.SPKIPin = "pin", base64.RawStdEncoding.EncodeToString(digest[:])
	}
	return bundle, nil
}

// CreateListenerPairing derives stable connection and trust metadata from one
// configured listener and adds only the client-specific authentication material.
func CreateListenerPairing(store *TokenStore, identityDir, listenerName string, listener config.ListenerConfig, clientCertFile, clientKeyFile string, scopes []string, expires *time.Time) (PairingBundle, error) {
	endpoint := listener.AdvertisedAddress
	if endpoint == "" {
		endpoint = listener.Address
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		return PairingBundle{}, fmt.Errorf("listener advertised_address must be a client-visible host:port")
	}
	certFile := listener.TLSCert
	caFile := listener.PairingCA
	if certFile == "" {
		managed, err := ensureManagedServerIdentity(identityDir, listenerName, endpoint)
		if err != nil {
			return PairingBundle{}, err
		}
		certFile, caFile = managed.cert, managed.ca
	}
	certificate, err := certificateDetails(certFile)
	if err != nil {
		return PairingBundle{}, err
	}
	if err := certificate.VerifyHostname(host); err != nil {
		return PairingBundle{}, fmt.Errorf("server certificate does not cover %s: %w", host, err)
	}
	pairingID, err := randomHex(16)
	if err != nil {
		return PairingBundle{}, err
	}
	created := time.Now().UTC()
	bundle := PairingBundle{Version: 1, PairingID: pairingID, Endpoint: endpoint, ServerName: host, Created: created, Expires: expires}
	if caFile == "" {
		bundle.TrustMode = "system"
	} else {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return PairingBundle{}, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return PairingBundle{}, fmt.Errorf("pairing_ca contains no certificates")
		}
		bundle.TrustMode, bundle.CAPEM = "ca", string(caPEM)
	}
	if listener.PairingPinCertificate {
		digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
		bundle.SPKIPin = base64.RawStdEncoding.EncodeToString(digest[:])
	}
	if strings.Contains(listener.AuthMode, "token") {
		if store == nil {
			return PairingBundle{}, fmt.Errorf("token store is required")
		}
		record, secret, err := store.Create(scopes, expires)
		if err != nil {
			return PairingBundle{}, err
		}
		bundle.TokenID, bundle.Secret, bundle.Scopes = record.ID, secret, record.Scopes
	}
	if listener.AuthMode == "mtls" || listener.AuthMode == "mtls+token" {
		if listener.ClientCA == "" {
			if clientCertFile != "" || clientKeyFile != "" {
				return PairingBundle{}, fmt.Errorf("client certificate files cannot be combined with managed client PKI")
			}
			bundle.ClientID = pairingID
			if err == nil {
				var cert, key []byte
				cert, key, err = issueManagedClientIdentity(identityDir, "boomerangz-"+bundle.ClientID)
				bundle.ClientCert, bundle.ClientKey = string(cert), string(key)
			}
		} else {
			if clientCertFile == "" || clientKeyFile == "" {
				return PairingBundle{}, fmt.Errorf("external mTLS requires client certificate and key")
			}
			cert, certErr := os.ReadFile(clientCertFile)
			key, keyErr := os.ReadFile(clientKeyFile)
			err = errors.Join(certErr, keyErr)
			bundle.ClientCert, bundle.ClientKey = string(cert), string(key)
		}
		if err != nil {
			if bundle.TokenID != "" {
				_ = store.Revoke(bundle.TokenID)
			}
			return PairingBundle{}, err
		}
		if _, err := tls.X509KeyPair([]byte(bundle.ClientCert), []byte(bundle.ClientKey)); err != nil {
			if bundle.TokenID != "" {
				_ = store.Revoke(bundle.TokenID)
			}
			return PairingBundle{}, err
		}
	}
	record := PairingRecord{ID: pairingID, Listener: listenerName, AuthMode: listener.AuthMode, TokenID: bundle.TokenID, ClientID: bundle.ClientID, Scopes: slices.Clone(bundle.Scopes), Created: created, Expires: expires}
	if err := recordPairing(identityDir, record); err != nil {
		if bundle.TokenID != "" {
			_ = store.Revoke(bundle.TokenID)
		}
		if bundle.ClientID != "" {
			_ = RevokeManagedClient(identityDir, bundle.ClientID)
		}
		return PairingBundle{}, err
	}
	return bundle, nil
}

// ImportPairingBundle validates and stores a client bundle with restrictive permissions.
func ImportPairingBundle(credentialsDir, name string, data []byte) (string, error) {
	if name == "" || strings.ContainsAny(name, "/\\\x00\r\n") {
		return "", fmt.Errorf("credential name is invalid")
	}
	var bundle PairingBundle
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return "", err
	}
	if _, err := clientTLSConfig(bundle); err != nil {
		return "", err
	}
	if bundle.Version != 1 {
		return "", fmt.Errorf("pairing bundle is incomplete")
	}
	hasToken := bundle.TokenID != "" || bundle.Secret != "" || len(bundle.Scopes) != 0
	hasClient := bundle.ClientCert != "" || bundle.ClientKey != ""
	if !hasToken && !hasClient {
		return "", fmt.Errorf("pairing bundle contains no client authentication")
	}
	if hasToken {
		identifier, identifierErr := hex.DecodeString(bundle.TokenID)
		secret, secretErr := hex.DecodeString(bundle.Secret)
		if identifierErr != nil || len(identifier) != 16 || secretErr != nil || len(secret) != 32 || len(bundle.Scopes) == 0 {
			return "", fmt.Errorf("pairing bundle token is incomplete")
		}
		if _, err := normalizeScopes(bundle.Scopes); err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(credentialsDir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(credentialsDir, name+".json")
	file, err := os.CreateTemp(credentialsDir, ".pairing-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if err := file.Chmod(0o600); err != nil {
		return "", err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(bundle)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return "", err
	}
	if err := os.Link(file.Name(), path); err != nil {
		return "", err
	}
	directory, err := os.Open(credentialsDir)
	if err != nil {
		return "", err
	}
	return path, errors.Join(directory.Sync(), directory.Close())
}

// LoadPairingBundle reads one imported bundle without following its final symlink.
func LoadPairingBundle(path string) (PairingBundle, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return PairingBundle{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return PairingBundle{}, fmt.Errorf("pairing bundle must be a regular file no larger than 1 MiB")
	}
	var bundle PairingBundle
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return PairingBundle{}, err
	}
	if bundle.Version != 1 {
		return PairingBundle{}, fmt.Errorf("unsupported pairing bundle version")
	}
	return bundle, nil
}

func clientTLSConfig(bundle PairingBundle) (*tls.Config, error) {
	if bundle.ServerName == "" {
		return nil, fmt.Errorf("pairing bundle server name is required")
	}
	var certificates []tls.Certificate
	if (bundle.ClientCert == "") != (bundle.ClientKey == "") {
		return nil, fmt.Errorf("client certificate and key must be supplied together")
	}
	if bundle.ClientCert != "" {
		certificate, err := tls.X509KeyPair([]byte(bundle.ClientCert), []byte(bundle.ClientKey))
		if err != nil {
			return nil, err
		}
		certificates = []tls.Certificate{certificate}
	}
	var result *tls.Config
	switch bundle.TrustMode {
	case "system":
		result = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: bundle.ServerName, Certificates: certificates}
	case "ca":
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(bundle.CAPEM)) {
			return nil, fmt.Errorf("pairing bundle has no CA certificates")
		}
		result = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: bundle.ServerName, Certificates: certificates}
	case "pin":
		expected, err := base64.RawStdEncoding.DecodeString(bundle.SPKIPin)
		if err != nil || len(expected) != sha256.Size {
			return nil, fmt.Errorf("pairing bundle has invalid public-key pin")
		}
		return &tls.Config{
			MinVersion:         tls.VersionTLS13,
			ServerName:         bundle.ServerName,
			Certificates:       certificates,
			InsecureSkipVerify: true, // Verification is performed explicitly below against the paired key.
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("server supplied no certificate")
				}
				leaf := state.PeerCertificates[0]
				now := time.Now()
				if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
					return fmt.Errorf("server certificate is not currently valid")
				}
				if err := leaf.VerifyHostname(bundle.ServerName); err != nil {
					return err
				}
				digest := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
				if subtle.ConstantTimeCompare(expected, digest[:]) != 1 {
					return fmt.Errorf("server public-key pin mismatch")
				}
				return nil
			},
		}, nil
	default:
		return nil, fmt.Errorf("pairing bundle trust mode must be system, ca, or pin")
	}
	if bundle.SPKIPin != "" {
		expected, err := base64.RawStdEncoding.DecodeString(bundle.SPKIPin)
		if err != nil || len(expected) != sha256.Size {
			return nil, fmt.Errorf("pairing bundle has invalid public-key pin")
		}
		result.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("server supplied no certificate")
			}
			digest := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)
			if subtle.ConstantTimeCompare(expected, digest[:]) != 1 {
				return fmt.Errorf("server public-key pin mismatch")
			}
			return nil
		}
	}
	return result, nil
}

type tokenCredentials struct{ id, secret string }

func (c tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.id + "." + c.secret}, nil
}
func (tokenCredentials) RequireTransportSecurity() bool { return true }

// Client wraps both generated services over one authenticated connection.
type Client struct {
	Connection *grpc.ClientConn
	Status     controlrpc.StatusServiceClient
	Control    controlrpc.ControlServiceClient
}

// DialLocal connects to the filesystem-authorized Unix control socket.
func DialLocal(ctx context.Context, socket string) (*Client, error) {
	connection, err := grpc.NewClient("passthrough:///"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
	if err != nil {
		return nil, err
	}
	client := &Client{Connection: connection, Status: controlrpc.NewStatusServiceClient(connection), Control: controlrpc.NewControlServiceClient(connection)}
	if _, err := client.Status.GetStatus(ctx, &controlrpc.GetStatusRequest{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return client, nil
}

// DialBundle connects with the exact TLS trust and token in a pairing bundle.
func DialBundle(ctx context.Context, bundle PairingBundle) (*Client, error) {
	connection, err := DialPairingConnection(bundle)
	if err != nil {
		return nil, err
	}
	client := &Client{Connection: connection, Status: controlrpc.NewStatusServiceClient(connection), Control: controlrpc.NewControlServiceClient(connection)}
	if _, err := client.Status.GetStatus(ctx, &controlrpc.GetStatusRequest{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return client, nil
}

// DialPairingConnection creates an authenticated connection without assuming
// which services or scopes the pairing grants.
func DialPairingConnection(bundle PairingBundle) (*grpc.ClientConn, error) {
	tlsConfig, err := clientTLSConfig(bundle)
	if err != nil {
		return nil, err
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	if bundle.TokenID != "" || bundle.Secret != "" {
		options = append(options, grpc.WithPerRPCCredentials(tokenCredentials{id: bundle.TokenID, secret: bundle.Secret}))
	}
	connection, err := grpc.NewClient(bundle.Endpoint, options...)
	if err != nil {
		return nil, err
	}
	return connection, nil
}
