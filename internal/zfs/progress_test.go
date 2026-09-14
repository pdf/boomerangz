package zfs

import (
	"sync/atomic"
	"testing"
	"time"
)

// manualProgress is a meter on a clock the test advances and ticks.
type manualProgress struct {
	meter   *ProgressMeter
	ticks   chan time.Time
	samples chan Progress
	clock   atomic.Int64
}

func newManualProgress(t *testing.T, estimate Estimate) *manualProgress {
	t.Helper()
	m := &manualProgress{ticks: make(chan time.Time), samples: make(chan Progress, 1)}
	start := time.Unix(1_000_000, 0)
	m.clock.Store(start.UnixNano())
	m.meter = startProgress(estimate, func(p Progress) { m.samples <- p }, m.ticks, func() time.Time { return time.Unix(0, m.clock.Load()) })
	if first := m.next(t); first.Bytes != 0 || first.BytesPerSecond != 0 {
		t.Fatalf("first sample = %+v", first)
	}
	return m
}

func (m *manualProgress) next(t *testing.T) Progress {
	t.Helper()
	select {
	case p := <-m.samples:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("no progress sample arrived")
	}
	return Progress{}
}

// tick advances the clock one interval and returns the sample the tick emits.
func (m *manualProgress) tick(t *testing.T) Progress {
	t.Helper()
	m.clock.Add(int64(ProgressInterval))
	select {
	case m.ticks <- time.Time{}:
	case <-time.After(5 * time.Second):
		t.Fatal("the meter stopped taking ticks")
	}
	return m.next(t)
}

func TestProgressReportsAStalledStreamAsStalled(t *testing.T) {
	t.Parallel()
	m := newManualProgress(t, Estimate{Known: true, Bytes: 1 << 30})
	// A steady stream of 1000 bytes per interval, for long enough that an
	// average since the start would take many samples to fall.
	var moving Progress
	for range 40 {
		m.meter.Add(1000)
		moving = m.tick(t)
	}
	if want := 1000 / ProgressInterval.Seconds(); moving.BytesPerSecond != want || moving.ETA == nil {
		t.Fatalf("moving sample = %+v, want a rate of %v and an ETA", moving, want)
	}
	// The stream stops. Samples keep arriving without a write, and the rate
	// reaches zero once the window has passed.
	within := int(progressWindow / ProgressInterval)
	var stalled Progress
	for sample := 1; ; sample++ {
		stalled = m.tick(t)
		if stalled.Bytes != 40*1000 {
			t.Fatalf("stalled sample bytes = %d", stalled.Bytes)
		}
		if stalled.BytesPerSecond == 0 {
			if sample > within {
				t.Fatalf("rate reached zero after %d samples, want within %d", sample, within)
			}
			break
		}
		if sample > within {
			t.Fatalf("rate is still %v after %d stalled samples", stalled.BytesPerSecond, sample)
		}
	}
	if stalled.ETA != nil {
		t.Fatalf("a stalled stream reported an ETA of %v", *stalled.ETA)
	}
	// It recovers as soon as bytes move again.
	m.meter.Add(500)
	if resumed := m.tick(t); resumed.BytesPerSecond <= 0 {
		t.Fatalf("resumed sample = %+v", resumed)
	}
	final := make(chan Progress)
	go func() { final <- m.meter.Finish(true) }()
	last := m.next(t)
	if !last.Completed || (<-final) != last {
		t.Fatalf("last sample = %+v", last)
	}
}

func TestProgressReportsNothingAfterFinish(t *testing.T) {
	t.Parallel()
	var finished atomic.Bool
	var late atomic.Int64
	samples := make(chan struct{}, 64)
	meter := StartProgress(Estimate{}, func(Progress) {
		if finished.Load() {
			late.Add(1)
		}
		select {
		case samples <- struct{}{}:
		default:
		}
	})
	if len(samples) != 1 {
		t.Fatalf("StartProgress returned after %d samples, want the first", len(samples))
	}
	// An active meter reports on its clock with no bytes moving.
	for range 3 {
		select {
		case <-samples:
		case <-time.After(5 * time.Second):
			t.Fatal("an active meter stopped reporting")
		}
	}
	last := meter.Finish(false)
	finished.Store(true)
	// The clock goroutine is the only reporter besides Finish's caller, and
	// Finish returns only once it has exited, so nothing can report later.
	// That is checked directly rather than by waiting for a sample that
	// should not come.
	select {
	case <-meter.done:
	default:
		t.Fatal("Finish returned while the meter's clock was still running")
	}
	if late.Load() != 0 || last.Completed {
		t.Fatalf("%d samples after Finish, last=%+v", late.Load(), last)
	}
}
