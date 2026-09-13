package daemon

import (
	"context"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

// watchRuntime serves a real status owner to the control plane, as Runtime
// does, so a watch test exercises the owner, its forwarders, and the handler
// together.
type watchRuntime struct{ status *Status }

func (r watchRuntime) ControlStatus() ControlSnapshot { return r.status.Snapshot() }
func (r watchRuntime) SubscribeStatus(ctx context.Context) (daemonstate.Subscription, error) {
	subscription, err := r.status.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}
func (watchRuntime) Trigger([]string) ([]string, error) { return nil, nil }
func (watchRuntime) Reconcile()                         {}
func (watchRuntime) Clean(context.Context, []string, bool, bool, bool, bool) ([]lifecycle.CleanPlan, error) {
	return nil, nil
}

// watchStatus serves status over a control socket and opens a watch on it,
// returning the watch once its first message, the state at registration, has
// arrived. The client's flow-control windows are fixed, so a client that stops
// reading holds the server's sends at a known size rather than one that grows.
func watchStatus(t *testing.T, status *Status) (controlrpc.StatusServiceClient, controlrpc.StatusService_WatchStatusClient) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	server, err := control.StartServer(cfg, watchRuntime{status: status}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	connection, err := grpc.NewClient("passthrough:///"+cfg.Paths.SocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Paths.SocketPath)
		}),
		grpc.WithInitialWindowSize(1<<16),
		grpc.WithInitialConnWindowSize(1<<16),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := controlrpc.NewStatusServiceClient(connection)
	stream, err := client.WatchStatus(t.Context(), &controlrpc.WatchStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetTransitions()) != 0 {
		t.Fatalf("first watch message carried transitions: %v", first.GetTransitions())
	}
	return client, stream
}

func jobRows(snapshot *controlrpc.StatusSnapshot, job string) []*controlrpc.JobStatus {
	var rows []*controlrpc.JobStatus
	for _, row := range snapshot.GetJobs() {
		if row.GetJob() == job {
			rows = append(rows, row)
		}
	}
	return rows
}

// TestWatchStatusCarriesTransitionsTheSnapshotCollapses pins the defect the
// transitions field exists for: a job that goes waiting-retry, probing,
// waiting-retry holds one row in any snapshot, and a watcher still receives all
// three changes, each message pairing its transitions with the state they
// produced.
func TestWatchStatusCarriesTransitionsTheSnapshotCollapses(t *testing.T) {
	t.Parallel()
	status := NewStatus(nil)
	client, stream := watchStatus(t, status)
	const job = "remote:tank/data:offsite"
	sent := []Event{transition(job, "waiting-retry", "connection refused"), transition(job, "probing", ""), transition(job, "waiting-retry", "connection reset")}
	for _, event := range sent {
		status.record(event)
	}
	var got []string
	for len(got) < len(sent) {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		transitions := response.GetTransitions()
		if len(transitions) == 0 {
			continue
		}
		for _, transition := range transitions {
			got = append(got, transition.GetState()+"/"+transition.GetReason())
		}
		last := transitions[len(transitions)-1]
		if rows := jobRows(response.GetStatus(), job); len(rows) != 1 || rows[0].GetState() != last.GetState() || rows[0].GetReason() != last.GetReason() {
			t.Fatalf("message state %v does not follow its last transition %v", rows, last)
		}
	}
	want := []string{"waiting-retry/connection refused", "probing/", "waiting-retry/connection reset"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("watch transitions=%q, want %q", got, want)
	}
	current, err := client.GetStatus(t.Context(), &controlrpc.GetStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rows := jobRows(current.GetStatus(), job); len(rows) != 1 || rows[0].GetState() != "waiting-retry" || rows[0].GetReason() != "connection reset" {
		t.Fatalf("snapshot rows=%v, want the one latest state", rows)
	}
}

// TestWatchStatusThatFallsBehindEndsAborted holds a watcher's reads until its
// subscription has overflowed, then reads everything the server sent. What
// arrives is an unbroken prefix of the recorded sequence followed by
// codes.Aborted, and nothing after it: the stream ends rather than resuming
// past a gap.
func TestWatchStatusThatFallsBehindEndsAborted(t *testing.T) {
	t.Parallel()
	status := NewStatus(nil)
	_, stream := watchStatus(t, status)
	// Far more than the forwarder's bound, its channel, and what the fixed
	// flow-control windows let the server send while the client reads nothing.
	total := 8 * transitionBound
	padding := strings.Repeat("x", 512)
	for index := range total {
		status.record(transition("remote:tank/data:offsite", "waiting-retry", strconv.Itoa(index)+" "+padding))
	}
	// A snapshot request is answered after every transition before it has been
	// offered to the subscriber, so the overflow has happened by now.
	status.Snapshot()
	next := 0
	var err error
	for {
		var response *controlrpc.WatchStatusResponse
		response, err = stream.Recv()
		if err != nil {
			break
		}
		for _, transition := range response.GetTransitions() {
			index, _, _ := strings.Cut(transition.GetReason(), " ")
			if index != strconv.Itoa(next) {
				t.Fatalf("received transition %s where %d was next: the stream skipped a gap", index, next)
			}
			next++
		}
	}
	if grpcstatus.Code(err) != codes.Aborted {
		t.Fatalf("watch that fell behind ended with %v, want Aborted", err)
	}
	if next >= total {
		t.Fatalf("received all %d transitions, so the watch never fell behind", total)
	}
	if response, again := stream.Recv(); again == nil {
		t.Fatalf("a message arrived after the watch ended: %v", response)
	}
}

// nextWatchMessage receives until a message satisfies match. Every message it
// reads, the matching one included, must carry no transitions: the change it
// waits for is one no job made.
func nextWatchMessage(t *testing.T, stream controlrpc.StatusService_WatchStatusClient, match func(*controlrpc.StatusSnapshot) bool) *controlrpc.StatusSnapshot {
	t.Helper()
	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if transitions := response.GetTransitions(); len(transitions) != 0 {
			t.Fatalf("a view change arrived with transitions: %v", transitions)
		}
		if match(response.GetStatus()) {
			return response.GetStatus()
		}
	}
}

// queuedJobs returns the job IDs a snapshot lists for one queue.
func queuedJobs(snapshot *controlrpc.StatusSnapshot, name string) []string {
	for _, queue := range snapshot.GetQueues() {
		if queue.GetName() == name {
			return queue.GetJobIds()
		}
	}
	return nil
}

// managementPool returns a status pool that is never started, holding one
// queued job per dataset, so its jobs leave the queue only when the test
// removes or pops them.
func managementPool(t *testing.T, status *Status, datasets ...string) *Pool {
	t.Helper()
	pool, err := newStatusPool("management", "management", 1, defaultQueueCapacity, status)
	if err != nil {
		t.Fatal(err)
	}
	for _, dataset := range datasets {
		job := Job{ID: "reconcile:" + dataset, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "reconciling", Run: func(context.Context) Outcome { return Outcome{} }}
		if added, err := pool.Submit(job); err != nil || !added {
			t.Fatalf("submit %s: added=%t err=%v", dataset, added, err)
		}
	}
	return pool
}

// TestWatchStatusReflectsViewChangesWithoutATransition holds a watch while a
// job is popped from its queue and the runtime activates a dataset, neither of
// which is a job transition. The watcher's snapshot reflects each as it
// happens rather than when some unrelated job next moves.
func TestWatchStatusReflectsViewChangesWithoutATransition(t *testing.T) {
	t.Parallel()
	status := NewStatus(nil)
	pool := managementPool(t, status, "tank/a", "tank/b")
	_, stream := watchStatus(t, status)
	// A worker reports the job it popped once it starts it; the pop itself is
	// only a queue view.
	if job, ok := pool.queue.Pop(t.Context()); !ok || job.ID != "reconcile:tank/a" {
		t.Fatalf("popped %q ok=%t", job.ID, ok)
	}
	nextWatchMessage(t, stream, func(snapshot *controlrpc.StatusSnapshot) bool {
		return strings.Join(queuedJobs(snapshot, "management"), ",") == "reconcile:tank/b"
	})
	runtime := &Runtime{status: status, known: map[string]bool{"tank/new": true}, active: map[string]bool{"tank/new": true}, recursive: map[string]bool{}}
	runtime.mu.Lock()
	runtime.publishRuntimeLocked()
	runtime.mu.Unlock()
	nextWatchMessage(t, stream, func(snapshot *controlrpc.StatusSnapshot) bool {
		datasets := snapshot.GetDatasets()
		return len(datasets) == 1 && datasets[0].GetName() == "tank/new" && datasets[0].GetActive()
	})
}

// TestWatchStatusDeliversRemovedJobsWithTheQueueView removes queued work and
// requires the message whose queue view no longer lists a job to carry that
// job's cancelled transition, so no watcher holds a pending row for a job that
// has left the queue.
func TestWatchStatusDeliversRemovedJobsWithTheQueueView(t *testing.T) {
	t.Parallel()
	status := NewStatus(nil)
	pool := managementPool(t, status, "tank/a", "tank/b", "tank/c")
	_, stream := watchStatus(t, status)
	removal := func(remove func() int, removed []string, reason, remaining string) {
		t.Helper()
		if count := remove(); count != len(removed) {
			t.Fatalf("removed %d jobs, want %d", count, len(removed))
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(queuedJobs(response.GetStatus(), "management"), ","); got != remaining {
			t.Fatalf("queue lists %q, want %q", got, remaining)
		}
		var cancelled []string
		for _, transition := range response.GetTransitions() {
			if transition.GetState() != "cancelled" || transition.GetReason() != reason {
				t.Fatalf("removal transition = %v", transition)
			}
			cancelled = append(cancelled, transition.GetJob())
		}
		slices.Sort(cancelled)
		if !slices.Equal(cancelled, removed) {
			t.Fatalf("cancelled %v, want %v", cancelled, removed)
		}
		for _, job := range removed {
			if rows := jobRows(response.GetStatus(), job); len(rows) != 1 || rows[0].GetState() != "cancelled" || rows[0].GetQueuePosition() != 0 {
				t.Fatalf("row for removed job %s = %v", job, rows)
			}
		}
	}
	removal(func() int { return pool.RemoveScope("tank/a", "dataset deactivated") }, []string{"reconcile:tank/a"}, "dataset deactivated", "reconcile:tank/b,reconcile:tank/c")
	removal(func() int { return pool.DiscardPending("daemon shutting down") }, []string{"reconcile:tank/b", "reconcile:tank/c"}, "daemon shutting down", "")
}
