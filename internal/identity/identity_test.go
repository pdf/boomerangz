package identity

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestConcurrentCreation(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "identity")
	var group sync.WaitGroup
	values := make(chan string, 16)
	for range 16 {
		group.Go(func() {
			value, err := LoadOrCreate(dir)
			if err != nil {
				t.Error(err)
				return
			}
			values <- value
		})
	}
	group.Wait()
	close(values)
	stored, err := Read(dir)
	if err != nil || !Valid(stored) {
		t.Fatalf("stored=%q err=%v", stored, err)
	}
	for value := range values {
		if value != stored {
			t.Fatalf("concurrent identities: %s != %s", value, stored)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != Filename {
		t.Fatalf("temporary files remain: %v %v", entries, err)
	}
}

func TestExistingIdentityNeverReplaced(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "broken\n", "11111111-1111-4111-8111-111111111111\ntrailing", "11111111-1111-4111-8111-111111111111\n"} {
		t.Run(value, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, Filename)
			if err := os.WriteFile(path, []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadOrCreate(dir)
			if (err == nil) != (value == "11111111-1111-4111-8111-111111111111\n") {
				t.Fatalf("unexpected validation: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != value {
				t.Fatalf("identity overwritten: %q %v", after, err)
			}
		})
	}
}

func TestSymlinkIdentityRefused(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "absent"), filepath.Join(dir, Filename)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("followed or replaced symlink")
	}
	if _, err := os.Lstat(filepath.Join(dir, "absent")); !os.IsNotExist(err) {
		t.Fatalf("created symlink target: %v", err)
	}
}

func TestExplicitRecoveryUsesExpectedIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	old := "11111111-1111-4111-8111-111111111111"
	recovered := "22222222-2222-4222-8222-222222222222"
	if err := os.WriteFile(filepath.Join(dir, Filename), []byte(old+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Recover(dir, recovered, old); err == nil {
		t.Fatal("recovered after expected identity mismatch")
	}
	if actual, _ := Read(dir); actual != old {
		t.Fatal("mismatched recovery changed identity")
	}
	if err := Recover(dir, old, recovered); err != nil {
		t.Fatal(err)
	}
	if actual, _ := Read(dir); actual != recovered {
		t.Fatalf("recovered identity=%s", actual)
	}
}
