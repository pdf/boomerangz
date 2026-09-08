package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const pairingFilename = "pairings.json"

// PairingRecord is safe administrative metadata for one issued relationship.
type PairingRecord struct {
	ID       string     `json:"id"`
	Listener string     `json:"listener"`
	AuthMode string     `json:"auth_mode"`
	TokenID  string     `json:"token_id,omitempty"`
	ClientID string     `json:"client_id,omitempty"`
	Scopes   []string   `json:"scopes,omitempty"`
	Created  time.Time  `json:"created"`
	Expires  *time.Time `json:"expires,omitempty"`
	Revoked  bool       `json:"revoked"`
}

type pairingFile struct {
	Version  int             `json:"version"`
	Pairings []PairingRecord `json:"pairings"`
}

func loadPairings(identityDir string) (pairingFile, error) {
	path := filepath.Join(identityDir, pairingFilename)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return pairingFile{Version: 1}, nil
	}
	if err != nil {
		return pairingFile{}, err
	}
	defer func() { _ = file.Close() }()
	var stored pairingFile
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil || stored.Version != 1 {
		return pairingFile{}, fmt.Errorf("invalid pairing store")
	}
	return stored, nil
}

func savePairings(identityDir string, stored pairingFile) error {
	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicPrivateFile(filepath.Join(identityDir, pairingFilename), data, 0o600)
}

func recordPairing(identityDir string, record PairingRecord) error {
	lock, err := managedLock(identityDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	stored, err := loadPairings(identityDir)
	if err != nil {
		return err
	}
	stored.Pairings = append(stored.Pairings, record)
	slices.SortFunc(stored.Pairings, func(a, b PairingRecord) int { return strings.Compare(a.ID, b.ID) })
	return savePairings(identityDir, stored)
}

// ListPairings returns detached issued-pairing metadata.
func ListPairings(identityDir string) ([]PairingRecord, error) {
	lock, err := managedLock(identityDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	stored, err := loadPairings(identityDir)
	return slices.Clone(stored.Pairings), err
}

// RevokePairing disables every Boomerangz-managed credential in one pairing.
func RevokePairing(identityDir string, tokens *TokenStore, id string) error {
	lock, err := managedLock(identityDir)
	if err != nil {
		return err
	}
	stored, err := loadPairings(identityDir)
	if err != nil {
		_ = lock.Close()
		return err
	}
	index := slices.IndexFunc(stored.Pairings, func(record PairingRecord) bool { return record.ID == id })
	if index < 0 {
		_ = lock.Close()
		return fmt.Errorf("pairing %s was not found", id)
	}
	record := stored.Pairings[index]
	stored.Pairings[index].Revoked = true
	if err := savePairings(identityDir, stored); err != nil {
		_ = lock.Close()
		return err
	}
	_ = lock.Close()
	var revokeErr error
	if record.TokenID != "" {
		revokeErr = errors.Join(revokeErr, tokens.Revoke(record.TokenID))
	}
	if record.ClientID != "" {
		revokeErr = errors.Join(revokeErr, RevokeManagedClient(identityDir, record.ClientID))
	}
	return revokeErr
}
