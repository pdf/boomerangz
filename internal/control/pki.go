package control

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const managedLeafRenewBefore = 30 * 24 * time.Hour

type managedIdentity struct {
	cert string
	key  string
	ca   string
}

// ManagedClientRecord identifies a client certificate issued by boomerangz.
type ManagedClientRecord struct {
	ID          string    `json:"id"`
	Fingerprint string    `json:"fingerprint"`
	Created     time.Time `json:"created"`
	Revoked     bool      `json:"revoked"`
}

type managedClientFile struct {
	Version int                   `json:"version"`
	Clients []ManagedClientRecord `json:"clients"`
}

func managedClientsPath(identityDir string) string {
	return filepath.Join(identityDir, "pki", "clients", "identities.json")
}

func loadManagedClients(identityDir string) (managedClientFile, error) {
	path := managedClientsPath(identityDir)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return managedClientFile{Version: 1}, nil
	}
	if err != nil {
		return managedClientFile{}, err
	}
	defer func() { _ = file.Close() }()
	var stored managedClientFile
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil || stored.Version != 1 {
		return managedClientFile{}, fmt.Errorf("invalid managed client identity store")
	}
	return stored, nil
}

func saveManagedClients(identityDir string, stored managedClientFile) error {
	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicPrivateFile(managedClientsPath(identityDir), data, 0o600)
}

func clientFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(digest[:])
}

func authorizeManagedClient(identityDir string, certificate *x509.Certificate) error {
	lock, err := managedLock(identityDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	stored, err := loadManagedClients(identityDir)
	if err != nil {
		return err
	}
	fingerprint := clientFingerprint(certificate)
	for _, record := range stored.Clients {
		if record.Fingerprint == fingerprint && !record.Revoked {
			return nil
		}
	}
	return fmt.Errorf("managed client identity is unknown or revoked")
}

// ListManagedClients returns all managed client identities, including revoked identities.
func ListManagedClients(identityDir string) ([]ManagedClientRecord, error) {
	lock, err := managedLock(identityDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	stored, err := loadManagedClients(identityDir)
	return stored.Clients, err
}

// RevokeManagedClient prevents a managed client identity from authenticating again.
func RevokeManagedClient(identityDir, id string) error {
	lock, err := managedLock(identityDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	stored, err := loadManagedClients(identityDir)
	if err != nil {
		return err
	}
	for index := range stored.Clients {
		if stored.Clients[index].ID == id {
			stored.Clients[index].Revoked = true
			return saveManagedClients(identityDir, stored)
		}
	}
	return fmt.Errorf("managed client identity %s was not found", id)
}

func atomicPrivateFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".pki-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func parseCertificate(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%s contains no certificate", path)
	}
	return x509.ParseCertificate(block.Bytes)
}

func parsePrivateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s contains no private key", path)
	}
	value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := value.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an Ed25519 private key", path)
	}
	return key, nil
}

func encodePrivateKey(key ed25519.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func managedLock(identityDir string) (*os.File, error) {
	dir := filepath.Join(identityDir, "pki")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ".lock")
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if err := syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func ensureCA(dir, commonName string) (*x509.Certificate, ed25519.PrivateKey, string, error) {
	certPath, keyPath := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key")
	certInfo, certErr := os.Stat(certPath)
	keyInfo, keyErr := os.Stat(keyPath)
	if certErr == nil && keyErr == nil {
		if !certInfo.Mode().IsRegular() || !keyInfo.Mode().IsRegular() {
			return nil, nil, "", fmt.Errorf("managed CA paths must be regular files")
		}
		certificate, err := parseCertificate(certPath)
		if err != nil {
			return nil, nil, "", err
		}
		key, err := parsePrivateKey(keyPath)
		return certificate, key, certPath, err
	}
	if !errors.Is(certErr, os.ErrNotExist) || !errors.Is(keyErr, os.ErrNotExist) {
		return nil, nil, "", fmt.Errorf("managed CA is incomplete")
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, "", err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: commonName}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return nil, nil, "", err
	}
	keyPEM, err := encodePrivateKey(private)
	if err != nil {
		return nil, nil, "", err
	}
	if err := atomicPrivateFile(keyPath, keyPEM, 0o600); err != nil {
		return nil, nil, "", err
	}
	if err := atomicPrivateFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return nil, nil, "", err
	}
	certificate, err := x509.ParseCertificate(der)
	return certificate, private, certPath, err
}

func randomSerial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return serial
}

func issueLeaf(ca *x509.Certificate, caKey ed25519.PrivateKey, commonName string, client bool) ([]byte, []byte, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().UTC()
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	template := &x509.Certificate{SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: commonName}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.AddDate(0, 3, 0), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	if !client {
		if ip := net.ParseIP(commonName); ip != nil {
			template.IPAddresses = []net.IP{ip}
		} else {
			template.DNSNames = []string{commonName}
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := encodePrivateKey(private)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM, nil
}

func ensureManagedServerIdentity(identityDir, listener, advertised string) (managedIdentity, error) {
	host, _, err := net.SplitHostPort(advertised)
	if err != nil || host == "" {
		return managedIdentity{}, fmt.Errorf("managed TLS advertised address must be host:port")
	}
	lock, err := managedLock(identityDir)
	if err != nil {
		return managedIdentity{}, err
	}
	defer func() { _ = lock.Close() }()
	dir := filepath.Join(identityDir, "pki", "listeners", listener)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return managedIdentity{}, err
	}
	ca, caKey, caPath, err := ensureCA(dir, "Boomerangz server CA for "+listener)
	if err != nil {
		return managedIdentity{}, err
	}
	pairPath := filepath.Join(dir, "server.pem")
	renew := true
	if certificate, certErr := parseCertificate(pairPath); certErr == nil {
		if key, keyErr := parsePrivateKey(pairPath); keyErr == nil {
			public, ok := certificate.PublicKey.(ed25519.PublicKey)
			if ok && bytes.Equal(key.Public().(ed25519.PublicKey), public) && certificate.VerifyHostname(host) == nil && time.Until(certificate.NotAfter) > managedLeafRenewBefore {
				renew = false
			}
		}
	}
	if renew {
		certPEM, keyPEM, err := issueLeaf(ca, caKey, host, false)
		if err != nil {
			return managedIdentity{}, err
		}
		pairPEM := append(certPEM, keyPEM...)
		if err := atomicPrivateFile(pairPath, pairPEM, 0o600); err != nil {
			return managedIdentity{}, err
		}
	}
	return managedIdentity{cert: pairPath, key: pairPath, ca: caPath}, nil
}

func ensureManagedClientCA(identityDir string) (certPath string, err error) {
	lock, err := managedLock(identityDir)
	if err != nil {
		return "", err
	}
	defer func() { _ = lock.Close() }()
	dir := filepath.Join(identityDir, "pki", "clients")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	_, _, path, err := ensureCA(dir, "Boomerangz managed client CA")
	return path, err
}

func issueManagedClientIdentity(identityDir, name string) (certificate, key []byte, err error) {
	lock, err := managedLock(identityDir)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = lock.Close() }()
	dir := filepath.Join(identityDir, "pki", "clients")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	ca, caKey, _, err := ensureCA(dir, "Boomerangz managed client CA")
	if err != nil {
		return nil, nil, err
	}
	certificate, key, err = issueLeaf(ca, caKey, name, true)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(certificate)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	stored, err := loadManagedClients(identityDir)
	if err != nil {
		return nil, nil, err
	}
	id := name
	if len(name) > len("boomerangz-") && name[:len("boomerangz-")] == "boomerangz-" {
		id = name[len("boomerangz-"):]
	}
	stored.Clients = append(stored.Clients, ManagedClientRecord{ID: id, Fingerprint: clientFingerprint(leaf), Created: time.Now().UTC()})
	if err := saveManagedClients(identityDir, stored); err != nil {
		return nil, nil, err
	}
	return certificate, key, nil
}
