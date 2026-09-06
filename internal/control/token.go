package control

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

const tokenFilename = "token-verifiers.json"
const tokenLockFilename = "token-verifiers.lock"

var validScopes = []string{"admin", "status", "trigger"}

// TokenRecord is safe to display: the one-time secret is never retained.
type TokenRecord struct {
	ID      string     `json:"id"`
	Scopes  []string   `json:"scopes"`
	Expires *time.Time `json:"expires,omitempty"`
	Hash    string     `json:"hash,omitempty"`
}

type tokenFile struct {
	Version int           `json:"version"`
	Tokens  []TokenRecord `json:"tokens"`
}

// TokenStore persists high-entropy token verifiers atomically.
type TokenStore struct {
	dir string
	mu  sync.Mutex
}

// NewTokenStore binds verifier persistence to an absolute identity directory.
func NewTokenStore(identityDir string) (*TokenStore, error) {
	if !filepath.IsAbs(identityDir) {
		return nil, fmt.Errorf("identity directory must be absolute")
	}
	return &TokenStore{dir: identityDir}, nil
}

func (s *TokenStore) lock(exclusive bool) (*os.File, error) {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(s.dir, tokenLockFilename)
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	operation := syscall.LOCK_SH
	if exclusive {
		operation = syscall.LOCK_EX
	}
	if err := syscall.Flock(fd, operation); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func randomHex(bytes int) (string, error) {
	data := make([]byte, bytes)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func normalizeScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 {
		scopes = []string{"status"}
	}
	scopes = slices.Clone(scopes)
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	for _, scope := range scopes {
		if !slices.Contains(validScopes, scope) {
			return nil, fmt.Errorf("unknown token scope %q", scope)
		}
	}
	return scopes, nil
}

func (s *TokenStore) load() (tokenFile, error) {
	path := filepath.Join(s.dir, tokenFilename)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if os.IsNotExist(err) {
		return tokenFile{Version: 1}, nil
	}
	if err != nil {
		return tokenFile{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return tokenFile{}, fmt.Errorf("invalid token verifier store")
	}
	var stored tokenFile
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil || stored.Version != 1 {
		return tokenFile{}, fmt.Errorf("invalid token verifier store")
	}
	return stored, nil
}

func (s *TokenStore) save(stored tokenFile) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.dir, ".token-verifiers-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	if err := temp.Chmod(0o600); err != nil {
		return err
	}
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(stored); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), filepath.Join(s.dir, tokenFilename)); err != nil {
		return err
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// Create persists a verifier and returns its one-time 256-bit secret.
func (s *TokenStore) Create(scopes []string, expires *time.Time) (TokenRecord, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := s.lock(true)
	if err != nil {
		return TokenRecord{}, "", err
	}
	defer func() { _ = lock.Close() }()
	scopes, err = normalizeScopes(scopes)
	if err != nil {
		return TokenRecord{}, "", err
	}
	id, err := randomHex(16)
	if err != nil {
		return TokenRecord{}, "", err
	}
	secret, err := randomHex(32)
	if err != nil {
		return TokenRecord{}, "", err
	}
	digest := sha256.Sum256([]byte(secret))
	record := TokenRecord{ID: id, Scopes: scopes, Expires: expires, Hash: hex.EncodeToString(digest[:])}
	stored, err := s.load()
	if err != nil {
		return TokenRecord{}, "", err
	}
	stored.Tokens = append(stored.Tokens, record)
	slices.SortFunc(stored.Tokens, func(a, b TokenRecord) int { return strings.Compare(a.ID, b.ID) })
	if err := s.save(stored); err != nil {
		return TokenRecord{}, "", err
	}
	display := record
	display.Hash = ""
	return display, secret, nil
}

// List returns token metadata without verifiers or secrets.
func (s *TokenStore) List() ([]TokenRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := s.lock(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	stored, err := s.load()
	if err != nil {
		return nil, err
	}
	result := slices.Clone(stored.Tokens)
	for index := range result {
		result[index].Hash = ""
		result[index].Scopes = slices.Clone(result[index].Scopes)
	}
	return result, nil
}

// Revoke removes one exact token identifier.
func (s *TokenStore) Revoke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := s.lock(true)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	stored, err := s.load()
	if err != nil {
		return err
	}
	before := len(stored.Tokens)
	stored.Tokens = slices.DeleteFunc(stored.Tokens, func(record TokenRecord) bool { return record.ID == id })
	if len(stored.Tokens) == before {
		return fmt.Errorf("token %s was not found", id)
	}
	return s.save(stored)
}

// Authorize verifies a live token and required scope in constant time.
func (s *TokenStore) Authorize(id, secret, scope string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := s.lock(false)
	if err != nil {
		return false
	}
	defer func() { _ = lock.Close() }()
	stored, err := s.load()
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(secret))
	for _, record := range stored.Tokens {
		if record.ID != id || (record.Expires != nil && !time.Now().Before(*record.Expires)) {
			continue
		}
		expected, err := hex.DecodeString(record.Hash)
		if err != nil || len(expected) != len(digest) || subtle.ConstantTimeCompare(expected, digest[:]) != 1 {
			return false
		}
		return slices.Contains(record.Scopes, scope) || slices.Contains(record.Scopes, "admin")
	}
	return false
}
