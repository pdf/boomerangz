package daemon

import (
	"slices"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestStatusListsAReceiveUntilItEnds(t *testing.T) {
	t.Parallel()
	owner, _ := ownedStatus(t)
	start := daemonstate.Receive{Dataset: "backup/host-a/tank", State: daemonstate.ReceiveReceiving, Sender: "host-a", Transport: "native", TotalBytes: 1000, TotalKnown: true}
	owner.handle(statusMessage{receive: &receiveChange{id: 1, begin: &start}})
	if !owner.applyReceiveProgress(receiveSample{id: 1, bytes: 400, bytesPerSecond: 100}) {
		t.Fatal("a sample for a listed receive was discarded")
	}
	receives := owner.state().Receives
	want := start
	want.Bytes, want.BytesPerSecond, want.ETA = 400, 100, 6*time.Second
	if len(receives) != 1 || receives[0] != want {
		t.Fatalf("receives = %+v, want %+v", receives, want)
	}
	owner.handle(statusMessage{receive: &receiveChange{id: 1}})
	if receives := owner.state().Receives; len(receives) != 0 {
		t.Fatalf("an ended receive is still listed: %+v", receives)
	}
	if owner.applyReceiveProgress(receiveSample{id: 1, bytes: 900}) {
		t.Fatal("a sample read after its receive ended brought it back")
	}
	if receives := owner.state().Receives; len(receives) != 0 {
		t.Fatalf("a late sample listed an ended receive: %+v", receives)
	}
}

func TestStatusReceiveWithoutAnEstimateHasNoETA(t *testing.T) {
	t.Parallel()
	owner, _ := ownedStatus(t)
	start := daemonstate.Receive{Dataset: "backup/host-a/tank", State: daemonstate.ReceiveReceiving}
	owner.handle(statusMessage{receive: &receiveChange{id: 1, begin: &start}})
	owner.applyReceiveProgress(receiveSample{id: 1, bytes: 400, bytesPerSecond: 100})
	if receive := owner.state().Receives[0]; receive.ETA != 0 || receive.TotalKnown || receive.Bytes != 400 || receive.BytesPerSecond != 100 {
		t.Fatalf("receive without an estimate = %+v", receive)
	}
}

// TestStatusShowsATracedReceiveOnlyOutsideReportedOnes pins that the resume
// token a reported receive leaves at or beneath its root shows as that
// receive, not as a second one.
func TestStatusShowsATracedReceiveOnlyOutsideReportedOnes(t *testing.T) {
	t.Parallel()
	owner, _ := ownedStatus(t)
	interrupted := []daemonstate.Receive{{Dataset: "backup/a", State: daemonstate.ReceiveInterrupted}, {Dataset: "backup/a/child", State: daemonstate.ReceiveReceiving}, {Dataset: "backup/ab", State: daemonstate.ReceiveInterrupted}}
	owner.handle(statusMessage{traced: &interrupted})
	active := daemonstate.Receive{Dataset: "backup/a", State: daemonstate.ReceiveReceiving, Transport: "ssh-shell"}
	owner.handle(statusMessage{receive: &receiveChange{id: 7, begin: &active}})
	receives := owner.state().Receives
	if len(receives) != 2 || receives[0] != active || receives[1] != interrupted[2] {
		t.Fatalf("receives = %+v", receives)
	}
	owner.handle(statusMessage{receive: &receiveChange{id: 7}})
	if receives := owner.state().Receives; len(receives) != 3 || receives[0] != interrupted[0] {
		t.Fatalf("after the receive ended, receives = %+v", receives)
	}
}

// TestStatusBeginReceiveReportsThroughTheOwner drives a receive through the
// reporter BeginReceive returns: it is listed as receiving from when it
// began, its samples reach the state, and its end, however often it is
// called, takes it away for good.
func TestStatusBeginReceiveReportsThroughTheOwner(t *testing.T) {
	t.Parallel()
	began := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	status := NewStatus(func() time.Time { return began })
	subscription := subscribe(t, status)
	reporter := status.BeginReceive(daemonstate.Receive{Dataset: "backup/host-a/tank", State: daemonstate.ReceiveInterrupted, Bytes: 99, Sender: "host-a"})
	listed := status.Snapshot().Receives
	if len(listed) != 1 || listed[0].State != daemonstate.ReceiveReceiving || !listed[0].Started.Equal(began) || listed[0].Bytes != 0 {
		t.Fatalf("a begun receive is listed as %+v", listed)
	}
	reporter.Progress(512, 64)
	for {
		update := receive(t, subscription)
		if receives := update.State.Receives; len(receives) == 1 && receives[0].Bytes == 512 {
			break
		}
	}
	reporter.End()
	reporter.End()
	reporter.Progress(1024, 64)
	if receives := status.Snapshot().Receives; len(receives) != 0 {
		t.Fatalf("an ended receive is still listed: %+v", receives)
	}
}

// TestReceiveTracesTellARunningReceiveFromAnInterruptedOne pins the reading
// the guest suite showed: a running receive's token changes as transaction
// groups sync and an interrupted one's does not. Readings closer together
// than a transaction group, as a short reconcile interval takes them, must
// not make a running receive look interrupted.
func TestReceiveTracesTellARunningReceiveFromAnInterruptedOne(t *testing.T) {
	t.Parallel()
	var traces receiveTraces
	start := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	at := func(offset time.Duration) time.Time { return start.Add(offset) }
	receives, recheck := traces.observe(map[string]string{"backup/a": "1-a1", "backup/b": "1-b1"}, at(0))
	if len(receives) != 0 || !recheck.Equal(at(receiveTraceWindow)) {
		t.Fatalf("first reading = %+v recheck=%v, want nothing yet and a recheck when the window ends", receives, recheck)
	}
	receives, _ = traces.observe(map[string]string{"backup/a": "1-a2", "backup/b": "1-b1"}, at(5*time.Second))
	if want := []daemonstate.Receive{{Dataset: "backup/a", State: daemonstate.ReceiveReceiving}}; !slices.Equal(receives, want) {
		t.Fatalf("after a token changed = %+v, want %+v", receives, want)
	}
	receives, _ = traces.observe(map[string]string{"backup/a": "1-a2", "backup/b": "1-b1"}, at(5*time.Second+200*time.Millisecond))
	if want := []daemonstate.Receive{{Dataset: "backup/a", State: daemonstate.ReceiveReceiving}}; !slices.Equal(receives, want) {
		t.Fatalf("a reading within a transaction group = %+v, want %+v", receives, want)
	}
	receives, recheck = traces.observe(map[string]string{"backup/a": "1-a2", "backup/b": "1-b1", "backup/c": "1-c1"}, at(receiveTraceWindow))
	want := []daemonstate.Receive{{Dataset: "backup/a", State: daemonstate.ReceiveReceiving}, {Dataset: "backup/b", State: daemonstate.ReceiveInterrupted}}
	if !slices.Equal(receives, want) || !recheck.Equal(at(5*time.Second+receiveTraceWindow)) {
		t.Fatalf("at the window's end = %+v recheck=%v, want %+v and a recheck for backup/a", receives, recheck, want)
	}
	receives, _ = traces.observe(map[string]string{"backup/a": "1-a2", "backup/b": "1-b1", "backup/c": "1-c1"}, at(5*time.Second+receiveTraceWindow))
	if want := []daemonstate.Receive{{Dataset: "backup/a", State: daemonstate.ReceiveInterrupted}, {Dataset: "backup/b", State: daemonstate.ReceiveInterrupted}}; !slices.Equal(receives, want) {
		t.Fatalf("once backup/a stopped = %+v, want %+v", receives, want)
	}
	if receives, recheck := traces.observe(map[string]string{}, at(time.Minute)); len(receives) != 0 || !recheck.IsZero() {
		t.Fatalf("receives outlived their tokens: %+v recheck=%v", receives, recheck)
	}
}

func TestTraceRootsReadsOnlyTheTopmostDestinationRoots(t *testing.T) {
	t.Parallel()
	entry := func(name string, replica bool) discovery.Entry {
		e := discovery.Entry{Dataset: zfs.Dataset{Name: name}}
		if replica {
			e.Replica = discovery.Replica{Destination: "backup"}
		}
		return e
	}
	entries := []discovery.Entry{entry("backup", false), entry("backup/host-a", true), entry("backup/host-a/tank", true), entry("archive/x", true), entry("tank", false), entry("incoming", false)}
	got := traceRoots(entries, []string{"incoming", "absent/root", "backup"})
	if want := []string{"archive/x", "backup", "incoming"}; !slices.Equal(got, want) {
		t.Fatalf("trace roots = %q, want %q", got, want)
	}
}
