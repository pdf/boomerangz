package daemon

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/zfs"
)

// racingZFS changes the source between a lifecycle operation's read and its
// check that nothing changed, once, the way a concurrent transfer writing its
// target binding does.
type racingZFS struct {
	*memoryZFS
	mu        sync.Mutex
	recursive int
	raced     bool
}

func (r *racingZFS) InspectState(ctx context.Context, dataset string, recursive bool) (zfs.State, error) {
	r.mu.Lock()
	race := false
	if recursive {
		r.recursive++
		race = r.recursive == 2 && !r.raced
		r.raced = r.raced || race
	}
	r.mu.Unlock()
	if race {
		if err := r.SetProperties(ctx, dataset, map[string]string{"org.boomerangz:state:target-binding:concurrent": "{}"}); err != nil {
			return zfs.State{}, err
		}
	}
	return r.memoryZFS.InspectState(ctx, dataset, recursive)
}

func TestProtectingANewSnapshotSurvivesAConcurrentSourceChange(t *testing.T) {
	t.Parallel()
	source, _ := newReportPool(t)
	racing := &racingZFS{memoryZFS: source}
	runtime, err := NewWithLocalStream(config.Defaults(), racing, reportInstallation, slog.New(slog.NewTextHandler(io.Discard, nil)), memoryStream{source: source, destination: source})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotAt(t, source, 4)

	if err := runtime.protectAndCoalesce(reportSource, snapshot, reportCanonical, false); err != nil {
		t.Fatalf("protecting %s failed on a concurrent change it should re-plan around: %v", snapshot, err)
	}
	if !racing.raced {
		t.Fatal("the concurrent change was never made, so the test proves nothing")
	}
	state, err := source.InspectState(t.Context(), reportSource, true)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Holds[snapshot], func(hold string) bool { return strings.Contains(hold, "boomerangz") }) {
		t.Fatalf("%s holds no reference after protection: %v", snapshot, state.Holds)
	}
	if pending, found := runtime.pending.Peek(reportSource, reportCanonical); !found || pending.Name != snapshot {
		t.Fatalf("pending = %+v found=%v, want %s", pending, found, snapshot)
	}
}
