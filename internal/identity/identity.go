// Package identity persists the non-secret installation identity independently
// of credentials. Callers must hold the installation lifecycle lock during use.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

// Filename is the fixed installation identity basename inside the configured directory.
const Filename = "installation-id"

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Valid accepts canonical UUIDs; new identities are always random version 4.
func Valid(value string) bool { return uuidPattern.MatchString(value) }

// New generates an RFC 9562 version 4 UUID using cryptographic randomness.
func New() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	data[6] = data[6]&0x0f | 0x40
	data[8] = data[8]&0x3f | 0x80
	value := hex.EncodeToString(data[:])
	return value[:8] + "-" + value[8:12] + "-" + value[12:16] + "-" + value[16:20] + "-" + value[20:], nil
}

// Read never creates or repairs identity state. A symlink, malformed file, or
// missing file is an error, not an invitation to replace an existing identity.
func Read(dir string) (string, error) {
	file, err := os.OpenFile(filepath.Join(dir, Filename), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("installation identity must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 38))
	if err != nil {
		return "", err
	}
	value := strings.TrimSuffix(string(data), "\n")
	if !Valid(value) {
		return "", fmt.Errorf("invalid installation identity in %s; explicit recovery required", filepath.Join(dir, Filename))
	}
	return value, nil
}

// LoadOrCreate publishes a fully written identity without replacing any existing
// directory entry. Concurrent creators converge on the winner's durable UUID.
// Recovery, not this function, must authorize replacing an existing identity.
func LoadOrCreate(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("identity directory is required")
	}
	value, err := Read(dir)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return value, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	value, err = New()
	if err != nil {
		return "", err
	}
	temp, err := os.CreateTemp(dir, ".installation-id-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	_, writeErr := io.WriteString(temp, value+"\n")
	err = errors.Join(writeErr, temp.Sync(), temp.Close())
	if err != nil {
		return "", err
	}
	if err := os.Link(temp.Name(), filepath.Join(dir, Filename)); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return "", err
	}
	return Read(dir)
}

// Recover atomically replaces an expected installation identity after the
// caller has completed the preview-first recovery checks while holding the
// installation lifecycle lock. It never repairs malformed or changed state.
func Recover(dir, expected, recovered string) error {
	if dir == "" || !Valid(expected) || !Valid(recovered) || expected == recovered {
		return fmt.Errorf("distinct valid expected and recovered identities are required")
	}
	current, err := Read(dir)
	if err != nil {
		return err
	}
	if current != expected {
		return fmt.Errorf("installation identity changed; retry recovery")
	}
	temp, err := os.CreateTemp(dir, ".installation-id-recovery-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	_, writeErr := io.WriteString(temp, recovered+"\n")
	err = errors.Join(writeErr, temp.Sync(), temp.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), filepath.Join(dir, Filename)); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
