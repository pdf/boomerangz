package daemon

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

// racingZFS changes the source between a lifecycle operation's read and its
// check that nothing changed, the way a concurrent transfer writing its target
// binding does. It counts the reads made with recursive, and changes the
// source before every second one: once, or, with always, on every attempt.
type racingZFS struct {
	*memoryZFS
	recursive bool
	always    bool
	mu        sync.Mutex
	reads     int
	races     int
}

func (r *racingZFS) InspectState(ctx context.Context, dataset string, recursive bool) (zfs.State, error) {
	r.mu.Lock()
	race := false
	if recursive == r.recursive {
		r.reads++
		race = r.reads%2 == 0 && (r.always || r.races == 0)
		if race {
			r.races++
		}
	}
	races := r.races
	r.mu.Unlock()
	if race {
		if err := r.SetProperties(ctx, dataset, map[string]string{"org.boomerangz:state:target-binding:concurrent": strconv.Itoa(races)}); err != nil {
			return zfs.State{}, err
		}
	}
	return r.memoryZFS.InspectState(ctx, dataset, recursive)
}

func (r *racingZFS) raced() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.races
}

func TestProtectingANewSnapshotSurvivesAConcurrentSourceChange(t *testing.T) {
	t.Parallel()
	source, _ := newReportPool(t)
	racing := &racingZFS{memoryZFS: source, recursive: true}
	runtime, err := NewWithLocalStream(config.Defaults(), racing, reportInstallation, slog.New(slog.NewTextHandler(io.Discard, nil)), memoryStream{source: source, destination: source})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotAt(t, source, 4)

	if err := runtime.protectAndCoalesce(reportSource, snapshot, reportCanonical, false); err != nil {
		t.Fatalf("protecting %s failed on a concurrent change it should re-plan around: %v", snapshot, err)
	}
	if racing.raced() == 0 {
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

// inactiveRuntime runs an inactive reconciliation of the report source
// against racing, with a started management pool and a subscription.
func inactiveRuntime(t *testing.T, racing *racingZFS) (*Runtime, *Subscription) {
	t.Helper()
	runtime, err := NewWithLocalStream(config.Defaults(), racing, reportInstallation, slog.New(slog.NewTextHandler(io.Discard, nil)), memoryStream{source: racing.memoryZFS, destination: racing.memoryZFS})
	if err != nil {
		t.Fatal(err)
	}
	runtime.delayContext, runtime.delayCancel = context.WithCancel(t.Context())
	ctx, cancel := context.WithCancel(t.Context())
	if err := runtime.management.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		runtime.delayCancel()
		runtime.management.Close()
		runtime.management.wait.Wait()
		runtime.delayWait.Wait()
	})
	return runtime, subscribe(t, runtime.status)
}

func TestInactiveReconciliationSurvivesAConcurrentSourceChange(t *testing.T) {
	t.Parallel()
	source, _ := newReportPool(t)
	racing := &racingZFS{memoryZFS: source}
	runtime, subscription := inactiveRuntime(t, racing)

	runtime.enqueueInactive(reportSource, false)
	run := runOf(t, subscription, "inactive:"+reportSource+":false")
	outcome := run[len(run)-1]
	if outcome.State != "succeeded" || outcome.Marker != "set" {
		t.Fatalf("inactive reconciliation ended %s (%s) with marker action %q, want succeeded placing the marker", outcome.State, outcome.Reason, outcome.Marker)
	}
	if racing.raced() == 0 {
		t.Fatal("the concurrent change was never made, so the test proves nothing")
	}
	state, err := source.InspectState(t.Context(), reportSource, false)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Properties, func(property zfs.Property) bool {
		return property.Dataset == reportSource && property.Name == lifecycle.InactiveProperty && property.Source == zfs.SourceLocal
	}) {
		t.Fatal("the inactive marker was not placed")
	}
}

func TestInactiveReconciliationThatKeepsConflictingIsRetried(t *testing.T) {
	t.Parallel()
	source, _ := newReportPool(t)
	racing := &racingZFS{memoryZFS: source, always: true}
	runtime, subscription := inactiveRuntime(t, racing)
	job := "inactive:" + reportSource + ":false"

	runtime.enqueueInactive(reportSource, false)
	run := runOf(t, subscription, job)
	outcome := run[len(run)-1]
	if outcome.State != "waiting-retry" || !strings.Contains(outcome.Reason, "retry with a fresh plan") {
		t.Fatalf("inactive reconciliation ended %s (%s), want waiting-retry for the conflict", outcome.State, outcome.Reason)
	}
	if got := racing.raced(); got != replanAttempts {
		t.Fatalf("the job made %d attempts before giving up, want %d", got, replanAttempts)
	}
	runtime.mu.Lock()
	_, scheduled := runtime.delayed[job]
	runtime.mu.Unlock()
	if !scheduled {
		t.Fatal("no retry was scheduled for the conflicting reconciliation")
	}
}

func TestInactiveRetryDoesNotUndoAnActivationChange(t *testing.T) {
	t.Parallel()
	source, _ := newReportPool(t)
	// The management pool is not started, so a retry that queues the job
	// leaves it visible in the queue.
	runtime, err := NewWithLocalStream(config.Defaults(), source, reportInstallation, slog.New(slog.NewTextHandler(io.Discard, nil)), memoryStream{source: source, destination: source})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.management.Close)
	setActive := func(active bool) {
		runtime.mu.Lock()
		runtime.active[reportSource] = active
		runtime.mu.Unlock()
	}

	// The dataset was re-enabled after the reconciliation deactivating it
	// failed, so retrying that reconciliation must not queue it.
	setActive(true)
	runtime.retryInactive(reportSource, false)
	if queued := runtime.management.queue.Snapshot(); queued.Pending != 0 {
		t.Fatalf("a retry against a changed activation queued %v", queued.IDs)
	}
	// Still deactivated, the retry queues it.
	setActive(false)
	runtime.retryInactive(reportSource, false)
	if queued := runtime.management.queue.Snapshot(); !slices.Equal(queued.IDs, []string{"inactive:" + reportSource + ":false"}) {
		t.Fatalf("a retry against an unchanged activation queued %v", queued.IDs)
	}
}
