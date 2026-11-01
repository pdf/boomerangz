// Package ownership keeps what an administrator writes into the service
// account's directories usable by that account. Run through sudo, the CLI
// would otherwise leave root-owned private files where the daemon must read
// and replace them, and the daemon then fails to start. Run as root, each file
// and directory created through this package takes the owner of the directory
// that holds it; run as anyone else, nothing changes.
package ownership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// observer, when set, stands in for root: every call behaves as if the
// process were privileged and reports each ownership change instead of
// making it.
var observer func(path string, uid, gid int)

// Observe makes later calls behave as if the process ran as root, passing
// each ownership change to record instead of applying it, until restore is
// called. It lets tests check that writes into service directories inherit
// ownership without running as root, so a test using it must not run in
// parallel with others.
func Observe(record func(path string, uid, gid int)) (restore func()) {
	previous := observer
	observer = record
	return func() { observer = previous }
}

func privileged() bool { return observer != nil || os.Geteuid() == 0 }

func ownerOf(dir string) (uid, gid int, err error) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("ownership of %s is unavailable", dir)
	}
	return int(stat.Uid), int(stat.Gid), nil
}

// Inherit gives file the owner and group of the directory that contains it.
// The change is made through the open descriptor, so a path replaced in the
// meantime is never followed.
func Inherit(file *os.File) error {
	if !privileged() {
		return nil
	}
	uid, gid, err := ownerOf(filepath.Dir(file.Name()))
	if err != nil {
		return err
	}
	if observer != nil {
		observer(file.Name(), uid, gid)
		return nil
	}
	return file.Chown(uid, gid)
}

// Shared is Inherit for a file the service account reads from a directory it
// does not own, such as an imported credential in credentials_dir: the file
// is also made readable by the directory's group, as that group is how the
// account reaches it.
func Shared(file *os.File) error {
	if !privileged() {
		return nil
	}
	if err := Inherit(file); err != nil {
		return err
	}
	return file.Chmod(0o640)
}

// MkdirAll is os.MkdirAll, except that each directory it creates takes the
// owner of its parent.
func MkdirAll(path string, perm os.FileMode) error {
	return mkdirAll(path, perm, false)
}

// MkdirAllShared is MkdirAll for a directory the service account reads but
// does not own: as with Shared, each directory it creates as root is also
// open to its group, mode 0750 rather than 0700.
func MkdirAllShared(path string) error {
	return mkdirAll(path, 0o700, true)
}

func mkdirAll(path string, perm os.FileMode, shared bool) error {
	if !privileged() {
		return os.MkdirAll(path, perm)
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if parent := filepath.Dir(path); parent != path {
		if err := mkdirAll(parent, perm, shared); err != nil {
			return err
		}
	}
	if err := os.Mkdir(path, perm); err != nil {
		// A concurrent creator owns what it made.
		if info, statErr := os.Lstat(path); statErr == nil && info.IsDir() {
			return nil
		}
		return err
	}
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	err = Inherit(dir)
	if err == nil && shared {
		err = dir.Chmod(0o750)
	}
	return errors.Join(err, dir.Close())
}
