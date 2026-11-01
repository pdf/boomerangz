package ownership_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pdf/boomerangz/internal/ownership"
	"github.com/pdf/boomerangz/internal/testutil/ownershiptest"
)

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func createPrivate(t *testing.T, dir string) *os.File {
	t.Helper()
	file, err := os.CreateTemp(dir, "file-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := file.Chmod(0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestUnprivilegedWritesAreUnchanged(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root")
	}
	t.Parallel()
	dir := t.TempDir()
	file := createPrivate(t, dir)
	if err := ownership.Shared(file); err != nil {
		t.Fatal(err)
	}
	if got := mode(t, file.Name()); got != 0o600 {
		t.Fatalf("unprivileged shared file mode=%o, want 600", got)
	}
	nested := filepath.Join(dir, "a", "b")
	if err := ownership.MkdirAllShared(nested); err != nil {
		t.Fatal(err)
	}
	if got := mode(t, nested); got != 0o700 {
		t.Fatalf("unprivileged shared directory mode=%o, want 700", got)
	}
}

func TestPrivilegedWritesInheritTheirDirectory(t *testing.T) {
	recorder := ownershiptest.Record(t)
	root := t.TempDir()
	identity := filepath.Join(root, "state", "identity")
	if err := ownership.MkdirAll(filepath.Join(identity, "pki"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Inherit(createPrivate(t, identity)); err != nil {
		t.Fatal(err)
	}
	recorder.Inherited(t, filepath.Join(root, "state"))
	if recorder.Changed(t, root) {
		t.Fatal("an existing directory was given a new owner")
	}
	if got := mode(t, identity); got != 0o700 {
		t.Fatalf("private directory mode=%o, want 700", got)
	}
}

func TestPrivilegedSharedWritesAreGroupReadable(t *testing.T) {
	recorder := ownershiptest.Record(t)
	root := t.TempDir()
	credentials := filepath.Join(root, "etc", "credentials.d")
	if err := ownership.MkdirAllShared(credentials); err != nil {
		t.Fatal(err)
	}
	file := createPrivate(t, credentials)
	if err := ownership.Shared(file); err != nil {
		t.Fatal(err)
	}
	recorder.Inherited(t, filepath.Join(root, "etc"))
	for _, dir := range []string{filepath.Join(root, "etc"), credentials} {
		if got := mode(t, dir); got != 0o750 {
			t.Errorf("%s mode=%o, want 750", dir, got)
		}
	}
	if got := mode(t, file.Name()); got != 0o640 {
		t.Fatalf("shared file mode=%o, want 640", got)
	}
}

func TestMkdirAllRefusesAFile(t *testing.T) {
	ownershiptest.Record(t)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ownership.MkdirAll(path, 0o700); err == nil {
		t.Fatal("MkdirAll accepted a regular file")
	}
	if err := ownership.MkdirAll(filepath.Join(path, "child"), 0o700); err == nil {
		t.Fatal("MkdirAll created a directory beneath a regular file")
	}
}
