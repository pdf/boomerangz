package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	reportLineage      = "11111111-1111-4111-8111-111111111111"
	reportInstallation = "22222222-2222-4222-8222-222222222222"
	reportSource       = "tank/data"
	reportDestination  = "backup/data"
	reportRemote       = "home"
	reportCanonical    = "ssh://replicator@backup.example.net:22/backup/data"
)

// memoryZFS is an in-memory pool holding whole datasets. It implements the
// operations a transfer performs and panics on anything else.
type memoryZFS struct {
	zfs.Executor
	mu         sync.Mutex
	inventory  []zfs.Dataset
	identities map[string]zfs.DatasetIdentity
	states     map[string]zfs.State
}

func (m *memoryZFS) state(object string) zfs.State {
	dataset, _, _ := strings.Cut(object, "@")
	dataset, _, _ = strings.Cut(dataset, "#")
	return m.states[dataset]
}

func (m *memoryZFS) update(object string, change func(*zfs.State)) {
	dataset, _, _ := strings.Cut(object, "@")
	dataset, _, _ = strings.Cut(dataset, "#")
	state := m.states[dataset]
	change(&state)
	m.states[dataset] = state
}

func (m *memoryZFS) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.inventory), nil
}

func (m *memoryZFS) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	identity, found := m.identities[dataset]
	if !found {
		return zfs.DatasetIdentity{}, fmt.Errorf("identity absent: %s", dataset)
	}
	return identity, nil
}

func (*memoryZFS) CheckPermissions(context.Context, string, []string) error { return nil }

func (*memoryZFS) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, nil
}

func (m *memoryZFS) InspectState(_ context.Context, dataset string, _ bool) (zfs.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, found := m.states[dataset]
	if !found {
		return zfs.State{}, fmt.Errorf("dataset absent: %s", dataset)
	}
	var cloned zfs.State
	data, err := json.Marshal(state)
	if err == nil {
		err = json.Unmarshal(data, &cloned)
	}
	return cloned, err
}

func (m *memoryZFS) SetProperties(_ context.Context, object string, values map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(object, func(state *zfs.State) {
		for key, value := range values {
			state.Properties = slices.DeleteFunc(state.Properties, func(row zfs.Property) bool {
				return row.Dataset == object && row.Name == key && row.Source == zfs.SourceLocal
			})
			state.Properties = append(state.Properties, zfs.Property{Dataset: object, Name: key, Value: value, Source: zfs.SourceLocal})
		}
	})
	return nil
}

func (m *memoryZFS) InheritProperty(_ context.Context, object, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(object, func(state *zfs.State) {
		state.Properties = slices.DeleteFunc(state.Properties, func(row zfs.Property) bool { return row.Dataset == object && row.Name == key })
	})
	return nil
}

func (m *memoryZFS) Hold(_ context.Context, tag, snapshot string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(snapshot, func(state *zfs.State) {
		if state.Holds == nil {
			state.Holds = make(map[string][]string)
		}
		state.Holds[snapshot] = append(state.Holds[snapshot], tag)
	})
	return nil
}

func (m *memoryZFS) Release(_ context.Context, tag, snapshot string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(snapshot, func(state *zfs.State) {
		state.Holds[snapshot] = slices.DeleteFunc(state.Holds[snapshot], func(held string) bool { return held == tag })
	})
	return nil
}

func (m *memoryZFS) Bookmark(_ context.Context, snapshot, bookmark string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, object := range m.state(snapshot).Objects {
		if object.Name == snapshot {
			m.update(snapshot, func(state *zfs.State) {
				state.Objects = append(state.Objects, zfs.Object{Name: bookmark, Type: "bookmark", GUID: object.GUID, CreateTXG: object.CreateTXG})
			})
			return nil
		}
	}
	return fmt.Errorf("snapshot absent: %s", snapshot)
}

func (m *memoryZFS) DestroyBookmark(_ context.Context, bookmark string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(bookmark, func(state *zfs.State) {
		state.Objects = slices.DeleteFunc(state.Objects, func(object zfs.Object) bool { return object.Name == bookmark })
	})
	return nil
}

func (*memoryZFS) EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error) {
	return zfs.Estimate{Bytes: 123, Known: true}, nil
}

// addSnapshot creates an owned snapshot of the report source at txg.
func (m *memoryZFS) addSnapshot(t *testing.T, txg uint64) string {
	t.Helper()
	metadata, err := lifecycle.NewMetadata(reportLineage, time.Date(2026, 9, 6, 12, int(txg), 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	name := reportSource + "@" + metadata.Name()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.update(reportSource, func(state *zfs.State) {
		state.Objects = append(state.Objects, zfs.Object{Name: name, Type: "snapshot", GUID: 100 + txg, CreateTXG: txg})
		for key, value := range metadata.Properties() {
			state.Properties = append(state.Properties, zfs.Property{Dataset: name, Name: key, Value: value, Source: zfs.SourceLocal})
		}
	})
	return name
}

// newReportPool returns a pool holding an owned, enabled source with two owned
// snapshots and an empty backup pool.
func newReportPool(t *testing.T) (*memoryZFS, []zfs.Property) {
	t.Helper()
	properties := []zfs.Property{
		{Dataset: reportSource, Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: reportSource, Name: policy.Namespace + "incremental", Value: "latest", Source: zfs.SourceLocal},
		{Dataset: reportSource, Name: policy.Namespace + "local", Value: reportDestination, Source: zfs.SourceLocal},
		{Dataset: reportSource, Name: lifecycle.LineageProperty, Value: reportLineage, Source: zfs.SourceLocal},
		{Dataset: reportSource, Name: lifecycle.OwnerProperty, Value: reportInstallation, Source: zfs.SourceLocal},
	}
	pool := &memoryZFS{
		inventory: []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, {Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, {Name: "backup", Type: zfs.Filesystem}},
		identities: map[string]zfs.DatasetIdentity{
			"backup": {Name: "backup", Type: zfs.Filesystem, GUID: 10, Pool: "backup", PoolGUID: 11},
		},
		states: map[string]zfs.State{
			reportSource: {Objects: []zfs.Object{{Name: reportSource, Type: "filesystem", GUID: 1, CreateTXG: 1}}, Properties: slices.Clone(properties)},
		},
	}
	pool.addSnapshot(t, 2)
	pool.addSnapshot(t, 4)
	return pool, properties
}

// receive creates the destination dataset if needed and receives snapshot.
func (m *memoryZFS) receive(root string, snapshot zfs.Object) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.states[root]; !found {
		m.inventory = append(m.inventory, zfs.Dataset{Name: root, Type: zfs.Filesystem, EncryptionRoot: "-"})
		m.identities[root] = zfs.DatasetIdentity{Name: root, Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 11}
		m.states[root] = zfs.State{Objects: []zfs.Object{{Name: root, Type: "filesystem", GUID: 20, CreateTXG: 20}}}
	}
	m.update(root, func(state *zfs.State) {
		_, component, _ := strings.Cut(snapshot.Name, "@")
		state.Objects = append(state.Objects, zfs.Object{Name: root + "@" + component, Type: "snapshot", GUID: snapshot.GUID, CreateTXG: 20 + snapshot.CreateTXG})
		delete(state.ResumeTokens, root)
	})
}

// memoryStream copies the endpoint snapshot between two memory pools and emits
// one sample before it copies anything, as both real streams do.
type memoryStream struct {
	source      *memoryZFS
	destination *memoryZFS
	fail        error
}

func (s memoryStream) Run(_ context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	if report != nil {
		report(zfs.Progress{Estimate: estimate})
	}
	if s.fail != nil {
		return zfs.Progress{}, s.fail
	}
	s.source.mu.Lock()
	state := s.source.state(reportSource)
	var endpoint zfs.Object
	for _, object := range state.Objects {
		selected := object.Name == send.Snapshot
		// A resume token names the held snapshot the interrupted receive was for.
		if send.ResumeToken != "" && object.Type == "snapshot" && len(state.Holds[object.Name]) > 0 {
			selected = true
		}
		if selected && object.CreateTXG > endpoint.CreateTXG {
			endpoint = object
		}
	}
	s.source.mu.Unlock()
	if endpoint.Name == "" {
		return zfs.Progress{}, fmt.Errorf("no endpoint for %+v", send)
	}
	s.destination.receive(receive.Root, endpoint)
	return zfs.Progress{Bytes: estimate.Bytes, Estimate: estimate}, nil
}

type memoryRemote struct {
	executor *memoryZFS
	stream   memoryStream
}

func (*memoryRemote) CanonicalTarget() string { return reportCanonical }
func (*memoryRemote) Transport() string       { return "ssh" }
func (r *memoryRemote) Open(context.Context) (openedRemote, error) {
	return openedRemote{mode: "memory", executor: r.executor, stream: r.stream, close: func() error { return nil }}, nil
}

// transitionLog records the job states the daemon logs, in order, and wakes
// a waiter on each one. A state logged with a reason is recorded as
// "state: reason".
type transitionLog struct {
	mu      sync.Mutex
	states  map[string][]string
	changed chan struct{}
}

func (l *transitionLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *transitionLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *transitionLog) WithGroup(string) slog.Handler            { return l }
func (l *transitionLog) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "worker state" {
		return nil
	}
	var job, state, reason string
	record.Attrs(func(attr slog.Attr) bool {
		switch attr.Key {
		case "job":
			job = attr.Value.String()
		case "state":
			state = attr.Value.String()
		case "reason":
			reason = attr.Value.String()
		}
		return true
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.states == nil {
		l.states = make(map[string][]string)
	}
	// Pool.Submit emits pending after the queue offer, so a worker can log the
	// start state first (design/event-driven-waits.md 3.1). Until that is
	// ordered, pending says nothing about the sequence a transfer reports.
	if strings.HasPrefix(state, "pending-") {
		return nil
	}
	if reason != "" {
		state += ": " + reason
	}
	l.states[job] = append(l.states[job], state)
	if l.changed != nil {
		close(l.changed)
	}
	l.changed = make(chan struct{})
	return nil
}

// waitRun waits for job's states since index from to end in a terminal state
// and returns them.
func (l *transitionLog) waitRun(t *testing.T, job string, from int) []string {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		l.mu.Lock()
		states := slices.Clone(l.states[job][from:])
		if l.changed == nil {
			l.changed = make(chan struct{})
		}
		changed := l.changed
		l.mu.Unlock()
		if index := slices.IndexFunc(states, func(state string) bool {
			state, _, _ = strings.Cut(state, ":")
			return slices.Contains([]string{"succeeded", "failed", "blocked", "waiting-retry", "cancelled"}, state)
		}); index >= 0 {
			return states[:index+1]
		}
		select {
		case <-changed:
		case <-timeout:
			t.Fatalf("job %s did not finish; states %v", job, states)
		}
	}
}

func (l *transitionLog) count(job string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.states[job])
}

func newReportRuntime(t *testing.T, source *memoryZFS, stream transfer.Stream) (*Runtime, *transitionLog) {
	t.Helper()
	log := &transitionLog{}
	cfg := config.Defaults()
	runtime, err := NewWithLocalStream(cfg, source, reportInstallation, slog.New(log), stream)
	if err != nil {
		t.Fatal(err)
	}
	runtime.delayContext, runtime.delayCancel = context.WithCancel(t.Context())
	if err := runtime.gate.SetEnabled(reportSource, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	for _, pool := range []*Pool{runtime.local, runtime.remote} {
		if err := pool.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cancel()
		runtime.delayCancel()
		runtime.local.Close()
		runtime.remote.Close()
		runtime.local.wait.Wait()
		runtime.remote.wait.Wait()
		runtime.delayWait.Wait()
	})
	return runtime, log
}

func remoteReportRuntime(t *testing.T) (*Runtime, *transitionLog, *memoryZFS, *memoryZFS, policy.Effective) {
	t.Helper()
	source, properties := newReportPool(t)
	properties = append(properties, zfs.Property{Dataset: reportSource, Name: policy.Namespace + "remote", Value: reportRemote, Source: zfs.SourceLocal})
	source.states[reportSource] = zfs.State{Objects: source.states[reportSource].Objects, Properties: append(source.states[reportSource].Properties, properties[len(properties)-1])}
	destination := &memoryZFS{
		inventory:  []zfs.Dataset{{Name: "backup", Type: zfs.Filesystem}},
		identities: map[string]zfs.DatasetIdentity{"backup": source.identities["backup"]},
		states:     map[string]zfs.State{},
	}
	runtime, log := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	runtime.config.Remotes = map[string]config.RemoteConfig{reportRemote: {Transport: "ssh", Root: reportDestination}}
	runtime.remotes[reportRemote] = &memoryRemote{executor: destination, stream: memoryStream{source: source, destination: destination}}
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, map[string]struct{}{reportRemote: {}})
	return runtime, log, source, destination, effective
}

func TestRemoteJobReportsTransferPhases(t *testing.T) {
	t.Parallel()
	runtime, log, _, _, effective := remoteReportRuntime(t)
	job := "remote:" + reportSource + ":" + reportRemote

	if !runtime.enqueueRemote(reportSource, reportRemote, effective, "") {
		t.Fatal("remote job was not accepted")
	}
	states := log.waitRun(t, job, 0)
	if want := []string{"probing", "sending", "verifying", "succeeded"}; !slices.Equal(states, want) {
		t.Fatalf("remote job states = %v, want %v", states, want)
	}

	// The target now holds the newest snapshot. The job probes and succeeds
	// without sending, and says so by never reporting sending.
	from := log.count(job)
	if !runtime.enqueueRemote(reportSource, reportRemote, effective, "") {
		t.Fatal("second remote job was not accepted")
	}
	states = log.waitRun(t, job, from)
	if want := []string{"probing", "succeeded"}; !slices.Equal(states, want) {
		t.Fatalf("up-to-date remote job states = %v, want %v", states, want)
	}
}

func TestRemoteResumeReportsEachSend(t *testing.T) {
	t.Parallel()
	runtime, log, source, destination, effective := remoteReportRuntime(t)
	job := "remote:" + reportSource + ":" + reportRemote
	road, err := runtime.road(reportSource, reportRemote, effective)
	if err != nil {
		t.Fatal(err)
	}

	// Interrupt a first send to leave the source holding its endpoint and the
	// destination holding a resume token for it.
	interrupted, err := transfer.NewRemote(source, destination, memoryStream{source: source, destination: destination, fail: errors.New("interrupted")}, reportInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.Apply(t.Context(), road.request, nil); err == nil {
		t.Fatal("interrupted send succeeded")
	}
	destination.mu.Lock()
	destination.inventory = append(destination.inventory, zfs.Dataset{Name: reportDestination, Type: zfs.Filesystem, EncryptionRoot: "-"})
	destination.identities[reportDestination] = zfs.DatasetIdentity{Name: reportDestination, Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 11}
	destination.states[reportDestination] = zfs.State{
		Objects:      []zfs.Object{{Name: reportDestination, Type: "filesystem", GUID: 20, CreateTXG: 20}},
		ResumeTokens: map[string]string{reportDestination: "1-resume-token"},
	}
	destination.mu.Unlock()
	// A newer snapshot appears while the receive is interrupted, so the pass
	// after the resume has something of its own to send.
	source.addSnapshot(t, 10)

	if !runtime.enqueueRemote(reportSource, reportRemote, effective, "") {
		t.Fatal("remote job was not accepted")
	}
	states := log.waitRun(t, job, 0)
	if want := []string{"probing", "sending", "verifying", "sending", "verifying", "succeeded"}; !slices.Equal(states, want) {
		t.Fatalf("resumed remote job states = %v, want %v", states, want)
	}
}

func TestLocalJobReportsPlanningBeforeSending(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	runtime, log := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)
	job := "local:" + reportSource + ":" + reportDestination

	if !runtime.enqueueLocal(reportSource, reportDestination, effective, "") {
		t.Fatal("local job was not accepted")
	}
	states := log.waitRun(t, job, 0)
	if want := []string{"planning", "sending", "verifying", "succeeded"}; !slices.Equal(states, want) {
		t.Fatalf("local job states = %v, want %v", states, want)
	}
}

func TestTransferReporterRecordsProgressUnderSending(t *testing.T) {
	t.Parallel()
	status := &StatusStore{}
	var events []Event
	pool, err := NewPool("transfer", 1, 1, func(event Event) {
		events = append(events, event)
		status.Record(event)
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{status: status, now: time.Now}
	job := Job{ID: "local:tank/data:backup/data", Scope: reportSource, LockKey: "local:backup/data"}
	report := runtime.transferReporter(pool, job)
	report(transfer.Report{Phase: transfer.PhaseSending})
	report(transfer.Report{Progress: &zfs.Progress{Bytes: 7, Estimate: zfs.Estimate{Bytes: 9, Known: true}}})
	if len(events) != 1 || events[0].State != "sending" || events[0].Job != job.ID || events[0].Target != job.LockKey {
		t.Fatalf("phase transition = %+v", events)
	}
	snapshot := status.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("status = %+v", snapshot)
	}
	got := snapshot[0]
	if got.State != "sending" || got.Pool != "transfer" || got.Scope != reportSource || got.Target != job.LockKey || got.Bytes != 7 || got.TotalBytes != 9 || !got.TotalKnown {
		t.Fatalf("progress = %+v", got)
	}
}
