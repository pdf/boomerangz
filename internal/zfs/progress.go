package zfs

import (
	"sync/atomic"
	"time"
)

const (
	// ProgressInterval is how often an active stream reports progress. It
	// reports on this clock whether or not bytes moved, so a stall is visible
	// as a stall.
	ProgressInterval = 250 * time.Millisecond
	// progressWindow is the span the reported rate is measured over. A stream
	// that stops moving reports a rate of zero once this much time has passed,
	// rather than an average since the start that decays toward it.
	progressWindow = time.Second
)

// ProgressMeter counts the bytes of one stream and reports progress on a
// clock while the stream is active. Add may be called from any goroutine. The
// report callback is called serially: first by StartProgress, then on each
// tick, and last by Finish, after which it is never called again.
type ProgressMeter struct {
	bytes    atomic.Uint64
	estimate Estimate
	report   func(Progress)
	now      func() time.Time
	points   []progressPoint // oldest first; the first is at or before the window's start
	stop     chan struct{}
	done     chan struct{}
}

type progressPoint struct {
	at    time.Time
	bytes uint64
}

// StartProgress reports a first sample and then one per ProgressInterval until
// Finish.
func StartProgress(estimate Estimate, report func(Progress)) *ProgressMeter {
	ticker := time.NewTicker(ProgressInterval)
	m := startProgress(estimate, report, ticker.C, time.Now)
	go func() {
		<-m.done
		ticker.Stop()
	}()
	return m
}

func startProgress(estimate Estimate, report func(Progress), ticks <-chan time.Time, now func() time.Time) *ProgressMeter {
	m := &ProgressMeter{estimate: estimate, report: report, now: now, stop: make(chan struct{}), done: make(chan struct{})}
	m.emit(false)
	go func() {
		defer close(m.done)
		for {
			select {
			case <-ticks:
				m.emit(false)
			case <-m.stop:
				return
			}
		}
	}()
	return m
}

// Add counts bytes the stream has moved.
func (m *ProgressMeter) Add(n uint64) { m.bytes.Add(n) }

// Bytes reports the bytes counted so far.
func (m *ProgressMeter) Bytes() uint64 { return m.bytes.Load() }

// Finish stops the clock and reports the last sample, which it returns.
func (m *ProgressMeter) Finish(completed bool) Progress {
	close(m.stop)
	<-m.done
	return m.emit(completed)
}

// emit is called by one goroutine at a time: StartProgress's caller, then the
// clock goroutine, then Finish's caller once the clock goroutine has exited.
func (m *ProgressMeter) emit(completed bool) Progress {
	now := m.now()
	bytes := m.bytes.Load()
	m.points = append(m.points, progressPoint{at: now, bytes: bytes})
	// Keep one point at or before the window's start as the rate's base.
	start := now.Add(-progressWindow)
	for len(m.points) > 1 && !m.points[1].at.After(start) {
		m.points = m.points[1:]
	}
	p := Progress{Bytes: bytes, Estimate: m.estimate, Completed: completed}
	if base := m.points[0]; now.After(base.at) {
		p.BytesPerSecond = float64(bytes-base.bytes) / now.Sub(base.at).Seconds()
	}
	if m.estimate.Known && m.estimate.Bytes >= bytes && p.BytesPerSecond > 0 {
		seconds := float64(m.estimate.Bytes-bytes) / p.BytesPerSecond
		if seconds < float64(time.Duration(1<<63-1))/float64(time.Second) {
			eta := time.Duration(seconds * float64(time.Second))
			p.ETA = &eta
		}
	}
	if m.report != nil {
		m.report(p)
	}
	return p
}
