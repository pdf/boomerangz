// Package ownershiptest checks that writes into service directories hand
// what they create to the directory's owner, as they do when the CLI runs as
// root, without the test running as root.
package ownershiptest

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/pdf/boomerangz/internal/ownership"
)

type owner struct{ uid, gid int }

type fileID struct{ dev, ino uint64 }

// Recorder collects the ownership changes made while it is active. Changes
// are keyed by inode, so an atomic write is matched to the name it was renamed
// to rather than the temporary name it was created under.
type Recorder struct {
	mu      sync.Mutex
	changes map[fileID]owner
	errs    []error
}

// Record makes the ownership package behave as root until tb ends, recording
// each change instead of applying it. A test using it must not call
// t.Parallel, since the ownership package is switched for the whole process.
func Record(tb testing.TB) *Recorder {
	tb.Helper()
	recorder := &Recorder{changes: map[fileID]owner{}}
	tb.Cleanup(ownership.Observe(recorder.record))
	return recorder
}

func (r *Recorder) record(path string, uid, gid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, err := os.Lstat(path)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("ownership change for missing %s: %w", path, err))
		return
	}
	r.changes[identify(info)] = owner{uid: uid, gid: gid}
}

func identify(info fs.FileInfo) fileID {
	stat := info.Sys().(*syscall.Stat_t)
	return fileID{dev: uint64(stat.Dev), ino: stat.Ino}
}

func ownerOf(info fs.FileInfo) owner {
	stat := info.Sys().(*syscall.Stat_t)
	return owner{uid: int(stat.Uid), gid: int(stat.Gid)}
}

// Inherited fails tb unless every entry under root, root included, was given
// the owner and group of the directory holding it.
func (r *Recorder) Inherited(tb testing.TB, root string) {
	tb.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, err := range r.errs {
		tb.Error(err)
	}
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		parent, err := os.Stat(filepath.Dir(path))
		if err != nil {
			return err
		}
		got, ok := r.changes[identify(info)]
		switch {
		case !ok:
			tb.Errorf("%s keeps the owner of the process that wrote it", path)
		case got != ownerOf(parent):
			tb.Errorf("%s was given %d:%d, want its directory's %d:%d", path, got.uid, got.gid, ownerOf(parent).uid, ownerOf(parent).gid)
		}
		return nil
	})
	if walkErr != nil {
		tb.Fatal(walkErr)
	}
}

// Changed reports whether path was given an owner while recording.
func (r *Recorder) Changed(tb testing.TB, path string) bool {
	tb.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		tb.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.changes[identify(info)]
	return ok
}
