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
	"strings"
	"syscall"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// PairingBundle is the one-time portable client credential document.
type PairingBundle struct {
	Version    int      `json:"version"`
	Endpoint   string   `json:"endpoint"`
	TrustMode  string   `json:"trust_mode"`
	CAPEM      string   `json:"ca_pem,omitempty"`
	ServerName string   `json:"server_name"`
	SPKIPin    string   `json:"spki_pin,omitempty"`
	TokenID    string   `json:"token_id"`
	Secret     string   `json:"token_secret"`
	Scopes     []string `json:"scopes"`
	ClientCert string   `json:"client_certificate_pem,omitempty"`
	ClientKey  string   `json:"client_private_key_pem,omitempty"`
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
	identifier, identifierErr := hex.DecodeString(bundle.TokenID)
	secret, secretErr := hex.DecodeString(bundle.Secret)
	if bundle.Version != 1 || identifierErr != nil || len(identifier) != 16 || secretErr != nil || len(secret) != 32 || len(bundle.Scopes) == 0 {
		return "", fmt.Errorf("pairing bundle is incomplete")
	}
	if _, err := normalizeScopes(bundle.Scopes); err != nil {
		return "", err
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
	switch bundle.TrustMode {
	case "system":
		return &tls.Config{MinVersion: tls.VersionTLS13, ServerName: bundle.ServerName, Certificates: certificates}, nil
	case "ca":
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(bundle.CAPEM)) {
			return nil, fmt.Errorf("pairing bundle has no CA certificates")
		}
		return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: bundle.ServerName, Certificates: certificates}, nil
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
	tlsConfig, err := clientTLSConfig(bundle)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(bundle.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithPerRPCCredentials(tokenCredentials{id: bundle.TokenID, secret: bundle.Secret}))
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
