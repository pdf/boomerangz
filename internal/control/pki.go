package control

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
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
	if !(errors.Is(certErr, os.ErrNotExist) && errors.Is(keyErr, os.ErrNotExist)) {
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
	certPath, keyPath := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	renew := true
	if certificate, certErr := parseCertificate(certPath); certErr == nil {
		if key, keyErr := parsePrivateKey(keyPath); keyErr == nil {
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
		if err := atomicPrivateFile(keyPath, keyPEM, 0o600); err != nil {
			return managedIdentity{}, err
		}
		if err := atomicPrivateFile(certPath, certPEM, 0o644); err != nil {
			return managedIdentity{}, err
		}
	}
	return managedIdentity{cert: certPath, key: keyPath, ca: caPath}, nil
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
