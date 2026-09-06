package transfer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestPendingSetCoalescesNewest(t *testing.T) {
	t.Parallel()
	var pending PendingSet
	target := "ssh://backup.example.net:22/tank/backups"
	older := PendingSnapshot{Name: "tank/data@older", CreateTXG: 10}
	newer := PendingSnapshot{Name: "tank/data@newer", CreateTXG: 20}
	if changed, err := pending.Offer("tank/data", target, older); err != nil || !changed {
		t.Fatalf("offer older changed=%t err=%v", changed, err)
	}
	if changed, err := pending.Offer("tank/data", target, older); err != nil || changed {
		t.Fatalf("duplicate changed=%t err=%v", changed, err)
	}
	if changed, err := pending.Offer("tank/data", target, newer); err != nil || !changed {
		t.Fatalf("offer newer changed=%t err=%v", changed, err)
	}
	if got, exists := pending.Peek("tank/data", target); !exists || got != newer {
		t.Fatalf("pending=%+v exists=%t", got, exists)
	}
	if pending.Complete("tank/data", target, older) || !pending.Complete("tank/data", target, newer) {
		t.Fatal("completion did not compare exact coalesced work")
	}
}

func TestRetryPolicyIsBoundedAndJittered(t *testing.T) {
	t.Parallel()
	policy := DefaultRetryPolicy()
	for _, tc := range []struct {
		failures int
		random   float64
		want     time.Duration
	}{{1, 0, 4 * time.Second}, {1, 0.5, 5 * time.Second}, {2, 0.5, 10 * time.Second}, {20, 0.5, 5 * time.Minute}} {
		got, err := policy.Delay(tc.failures, tc.random)
		if err != nil || got != tc.want {
			t.Fatalf("delay(%d,%v)=%s err=%v want %s", tc.failures, tc.random, got, err, tc.want)
		}
	}
}

type scriptedApply struct {
	results []Result
	errors  []error
	calls   []Request
}

func (s *scriptedApply) Apply(_ context.Context, request Request, _ func(zfs.Progress)) (Result, error) {
	s.calls = append(s.calls, request)
	index := len(s.calls) - 1
	return s.results[index], s.errors[index]
}

type temporaryTestError struct{}

func (temporaryTestError) Error() string   { return "offline" }
func (temporaryTestError) Temporary() bool { return true }

func remoteRecoveryRequest() Request {
	return Request{Source: "tank/data", DestinationRoot: "backup/data", Transport: "ssh", RemoteName: "home", CanonicalTarget: "ssh://backup.example.net:22/backup/data"}
}

func TestRoadwarriorResumesBeforeCoalescedSnapshot(t *testing.T) {
	t.Parallel()
	pending := &PendingSet{}
	request := remoteRecoveryRequest()
	newest := PendingSnapshot{Name: "tank/data@newest", CreateTXG: 20}
	_, _ = pending.Offer(request.Source, request.CanonicalTarget, newest)
	engine := &scriptedApply{
		results: []Result{{Plan: Plan{Mode: "resume", Snapshot: "tank/data@held"}, Verified: true}, {Plan: Plan{Mode: "incremental-latest", Snapshot: newest.Name}, Verified: true}},
		errors:  []error{nil, nil},
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	recovery, err := NewRoadwarrior(engine, request, pending, DefaultRetryPolicy(), func() time.Time { return now }, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := recovery.Reconcile(t.Context(), nil)
	if err != nil || outcome.Status != "succeeded" || len(outcome.Results) != 2 || len(engine.calls) != 2 {
		t.Fatalf("outcome=%+v err=%v calls=%v", outcome, err, engine.calls)
	}
	if engine.calls[0].Snapshot != newest.Name || engine.calls[1].Snapshot != newest.Name {
		t.Fatalf("coalesced selection was not retained across resume: %v", engine.calls)
	}
	if _, exists := pending.Peek(request.Source, request.CanonicalTarget); exists {
		t.Fatal("verified coalesced work remained pending")
	}
}

func TestRoadwarriorRetriesOnlyTemporaryFailures(t *testing.T) {
	t.Parallel()
	request := remoteRecoveryRequest()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	engine := &scriptedApply{results: []Result{{}}, errors: []error{temporaryTestError{}}}
	recovery, err := NewRoadwarrior(engine, request, nil, DefaultRetryPolicy(), func() time.Time { return now }, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := recovery.Reconcile(t.Context(), nil)
	if err == nil || outcome.Status != "waiting-retry" || !outcome.NotBefore.Equal(now.Add(5*time.Second)) {
		t.Fatalf("outcome=%+v err=%v", outcome, err)
	}
	outcome, err = recovery.Reconcile(t.Context(), nil)
	if err != nil || outcome.Status != "waiting-retry" || len(engine.calls) != 1 {
		t.Fatalf("early retry outcome=%+v err=%v calls=%d", outcome, err, len(engine.calls))
	}

	blocked := &scriptedApply{results: []Result{{}}, errors: []error{errors.New("identity mismatch")}}
	recovery, _ = NewRoadwarrior(blocked, request, nil, DefaultRetryPolicy(), func() time.Time { return now }, func() float64 { return 0.5 })
	outcome, err = recovery.Reconcile(t.Context(), nil)
	if err == nil || outcome.Status != "blocked" || !outcome.NotBefore.IsZero() {
		t.Fatalf("blocked outcome=%+v err=%v", outcome, err)
	}
}
