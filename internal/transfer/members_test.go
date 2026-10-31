package transfer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// treePool is one host's pools held as individual datasets, so a root and its
// members, and their replicas, are distinct datasets as they are in ZFS. It
// implements what a transfer and its lifecycle writes need and panics on
// anything else.
type treePool struct {
	zfs.Executor
	mu       sync.Mutex
	datasets []zfs.Dataset
	objects  []zfs.Object
	props    []zfs.Property
	holds    map[string][]string
	tokens   map[string]string
	// resumes maps a resume token to the source snapshot its receive was for.
	resumes map[string]string
	// failRename fails the next rename of each dataset it names.
	failRename map[string]bool
	txg        uint64
	guid       uint64
}

func newTreePool(names ...string) *treePool {
	p := &treePool{holds: map[string][]string{}, tokens: map[string]string{}, resumes: map[string]string{}, failRename: map[string]bool{}, txg: 1, guid: 1}
	for _, name := range names {
		p.create(name)
	}
	return p
}

func (p *treePool) create(name string) {
	p.datasets = append(p.datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	p.guid++
	p.objects = append(p.objects, zfs.Object{Name: name, Type: "filesystem", GUID: p.guid, CreateTXG: p.txg})
	slices.SortFunc(p.datasets, func(a, b zfs.Dataset) int { return strings.Compare(a.Name, b.Name) })
}

func (p *treePool) exists(name string) bool {
	return slices.ContainsFunc(p.datasets, func(dataset zfs.Dataset) bool { return dataset.Name == name })
}

func (p *treePool) object(name string) (zfs.Object, bool) {
	index := slices.IndexFunc(p.objects, func(object zfs.Object) bool { return object.Name == name })
	if index < 0 {
		return zfs.Object{}, false
	}
	return p.objects[index], true
}

func (p *treePool) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.datasets), nil
}

func (p *treePool) InspectState(_ context.Context, dataset string, recursive bool) (zfs.State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.exists(dataset) {
		return zfs.State{}, fmt.Errorf("cannot open '%s': dataset does not exist", dataset)
	}
	in := func(name string) bool {
		base := datasetOf(name)
		return base == dataset || (recursive && strings.HasPrefix(base, dataset+"/"))
	}
	state := zfs.State{Holds: map[string][]string{}, ResumeTokens: map[string]string{}}
	for _, object := range p.objects {
		if in(object.Name) {
			state.Objects = append(state.Objects, object)
			if holds := p.holds[object.Name]; len(holds) > 0 {
				state.Holds[object.Name] = slices.Clone(holds)
			}
		}
	}
	for _, property := range p.props {
		if in(property.Dataset) {
			state.Properties = append(state.Properties, property)
		}
	}
	for name, token := range p.tokens {
		if in(name) {
			state.ResumeTokens[name] = token
		}
	}
	if len(state.Holds) == 0 {
		state.Holds = nil
	}
	if len(state.ResumeTokens) == 0 {
		state.ResumeTokens = nil
	}
	return state, nil
}

func (p *treePool) InspectDatasetIdentity(_ context.Context, name string) (zfs.DatasetIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	object, found := p.object(name)
	if !found || !p.exists(name) {
		return zfs.DatasetIdentity{}, fmt.Errorf("dataset does not exist: %s", name)
	}
	pool, _, _ := strings.Cut(name, "/")
	placeholder := slices.ContainsFunc(p.props, func(row zfs.Property) bool {
		return row.Dataset == name && row.Name == zfs.PlaceholderProperty && row.Source == zfs.SourceLocal && row.Value == "on"
	})
	return zfs.DatasetIdentity{Name: name, Type: zfs.Filesystem, GUID: object.GUID, Pool: pool, PoolGUID: uint64(len(pool)) + 1000, Placeholder: placeholder}, nil
}

func (p *treePool) GetStoredProperties(_ context.Context, names []string) ([]zfs.Property, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var rows []zfs.Property
	for _, property := range p.props {
		if slices.Contains(names, property.Dataset) {
			rows = append(rows, property)
		}
	}
	return rows, nil
}

func (p *treePool) CheckPermissions(context.Context, string, []string) error { return nil }

func (p *treePool) EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error) {
	return zfs.Estimate{Bytes: 1, Known: true}, nil
}

func (p *treePool) SetProperties(_ context.Context, object string, values map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, value := range values {
		p.props = slices.DeleteFunc(p.props, func(row zfs.Property) bool {
			return row.Dataset == object && row.Name == key && row.Source == zfs.SourceLocal
		})
		p.props = append(p.props, zfs.Property{Dataset: object, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	return nil
}

func (p *treePool) InheritProperty(_ context.Context, object, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.props = slices.DeleteFunc(p.props, func(row zfs.Property) bool {
		return row.Dataset == object && row.Name == key && row.Source == zfs.SourceLocal
	})
	return nil
}

func (p *treePool) Snapshot(_ context.Context, datasets []string, name string, properties map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.txg++
	for _, dataset := range datasets {
		full := dataset + "@" + name
		p.guid++
		p.objects = append(p.objects, zfs.Object{Name: full, Type: "snapshot", GUID: p.guid, CreateTXG: p.txg})
		for key, value := range properties {
			p.props = append(p.props, zfs.Property{Dataset: full, Name: key, Value: value, Source: zfs.SourceLocal})
		}
	}
	return nil
}

func (p *treePool) Hold(_ context.Context, tag, snapshot string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.holds[snapshot] = append(p.holds[snapshot], tag)
	return nil
}

func (p *treePool) Release(_ context.Context, tag, snapshot string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	index := slices.Index(p.holds[snapshot], tag)
	if index < 0 {
		return fmt.Errorf("no hold %s on %s", tag, snapshot)
	}
	p.holds[snapshot] = slices.Delete(p.holds[snapshot], index, index+1)
	return nil
}

func (p *treePool) Bookmark(_ context.Context, snapshot, bookmark string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	object, found := p.object(snapshot)
	if !found {
		return fmt.Errorf("snapshot absent: %s", snapshot)
	}
	p.objects = append(p.objects, zfs.Object{Name: bookmark, Type: "bookmark", GUID: object.GUID, CreateTXG: object.CreateTXG})
	return nil
}

func (p *treePool) DestroyBookmark(_ context.Context, bookmark string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects = slices.DeleteFunc(p.objects, func(object zfs.Object) bool { return object.Name == bookmark })
	return nil
}

func (p *treePool) CreateReceiveParent(_ context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exists(name) {
		return fmt.Errorf("dataset already exists: %s", name)
	}
	if parent := name[:strings.LastIndexByte(name, '/')]; !p.exists(parent) {
		return fmt.Errorf("parent does not exist: %s", parent)
	}
	p.create(name)
	p.props = append(p.props, zfs.Property{Dataset: name, Name: zfs.PlaceholderProperty, Value: "on", Source: zfs.SourceLocal})
	return nil
}

// RenameDataset moves a dataset with everything beneath it, as zfs rename
// does, and fails when its injected failure names the dataset.
func (p *treePool) RenameDataset(_ context.Context, from, to string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failRename[from] {
		delete(p.failRename, from)
		return fmt.Errorf("injected rename failure on %s", from)
	}
	if !p.exists(from) || p.exists(to) {
		return fmt.Errorf("cannot rename %s to %s", from, to)
	}
	if parent := to[:strings.LastIndexByte(to, '/')]; !p.exists(parent) {
		return fmt.Errorf("parent does not exist: %s", parent)
	}
	move := func(name string) string {
		base := datasetOf(name)
		if base == from || strings.HasPrefix(base, from+"/") {
			return to + strings.TrimPrefix(name, from)
		}
		return name
	}
	for i := range p.datasets {
		p.datasets[i].Name = move(p.datasets[i].Name)
	}
	for i := range p.objects {
		p.objects[i].Name = move(p.objects[i].Name)
	}
	for i := range p.props {
		p.props[i].Dataset = move(p.props[i].Dataset)
	}
	holds, tokens := map[string][]string{}, map[string]string{}
	for name, value := range p.holds {
		holds[move(name)] = value
	}
	for name, value := range p.tokens {
		tokens[move(name)] = value
	}
	p.holds, p.tokens = holds, tokens
	slices.SortFunc(p.datasets, func(a, b zfs.Dataset) int { return strings.Compare(a.Name, b.Name) })
	return nil
}

// DestroyDataset removes a dataset; without recursive, only one that has no
// children and no snapshots, as zfs destroy refuses anything else.
func (p *treePool) DestroyDataset(_ context.Context, name string, recursive bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if recursive {
		gone := func(object string) bool {
			base := datasetOf(object)
			return base == name || strings.HasPrefix(base, name+"/")
		}
		p.datasets = slices.DeleteFunc(p.datasets, func(dataset zfs.Dataset) bool { return gone(dataset.Name) })
		p.objects = slices.DeleteFunc(p.objects, func(object zfs.Object) bool { return gone(object.Name) })
		p.props = slices.DeleteFunc(p.props, func(property zfs.Property) bool { return gone(property.Dataset) })
		maps.DeleteFunc(p.tokens, func(dataset, _ string) bool { return gone(dataset) })
		return nil
	}
	for _, object := range p.objects {
		if object.Name != name && datasetOf(object.Name) == name || strings.HasPrefix(object.Name, name+"/") {
			return fmt.Errorf("cannot destroy %s: it has children or snapshots", name)
		}
	}
	p.datasets = slices.DeleteFunc(p.datasets, func(dataset zfs.Dataset) bool { return dataset.Name == name })
	p.objects = slices.DeleteFunc(p.objects, func(object zfs.Object) bool { return object.Name == name })
	p.props = slices.DeleteFunc(p.props, func(property zfs.Property) bool { return property.Dataset == name })
	return nil
}

// AbortReceive discards dataset's partial receive as zfs receive -A does: an
// interrupted incremental keeps its dataset, while an interrupted full receive
// takes with it the dataset it was creating, which holds no snapshot yet.
func (p *treePool) AbortReceive(_ context.Context, dataset string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.tokens, dataset)
	if slices.ContainsFunc(p.objects, func(object zfs.Object) bool { return datasetOf(object.Name) == dataset && object.Name != dataset }) {
		return nil
	}
	p.datasets = slices.DeleteFunc(p.datasets, func(d zfs.Dataset) bool { return d.Name == dataset })
	p.objects = slices.DeleteFunc(p.objects, func(object zfs.Object) bool { return object.Name == dataset })
	p.props = slices.DeleteFunc(p.props, func(property zfs.Property) bool { return property.Dataset == dataset })
	return nil
}

// treeStream receives into the dataset zfs receive would derive from the
// stream's own source name and the receive options, so a stream that maps to
// the wrong place lands in the wrong place. Streams from a failing source
// leave a resume token on their target.
type treeStream struct {
	pool    *treePool
	failing map[string]bool
	sent    *[]string
}

func (s treeStream) Run(_ context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	if report != nil {
		report(zfs.Progress{Estimate: estimate})
	}
	p := s.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := send.Snapshot
	target := receive.Root
	if send.ResumeToken != "" {
		snapshot = p.resumes[send.ResumeToken]
		delete(p.tokens, target)
	} else {
		var err error
		if target, err = zfs.MapReceiveDataset(send.Source, receive.Root, receive.Discard); err != nil {
			return zfs.Progress{}, err
		}
	}
	if s.sent != nil {
		*s.sent = append(*s.sent, datasetOf(snapshot))
	}
	source, found := p.object(snapshot)
	if !found {
		return zfs.Progress{}, fmt.Errorf("source snapshot absent: %s", snapshot)
	}
	if !p.exists(target) {
		if parent := target[:strings.LastIndexByte(target, '/')]; !p.exists(parent) {
			return zfs.Progress{}, fmt.Errorf("cannot receive %s: parent does not exist", target)
		}
		p.create(target)
	}
	if s.failing[datasetOf(snapshot)] && send.ResumeToken == "" {
		token := "token-" + target
		p.tokens[target] = token
		p.resumes[token] = snapshot
		return zfs.Progress{}, errors.New("injected receive failure")
	}
	_, component, _ := strings.Cut(snapshot, "@")
	p.txg++
	p.objects = append(p.objects, zfs.Object{Name: target + "@" + component, Type: "snapshot", GUID: source.GUID, CreateTXG: p.txg})
	return zfs.Progress{Bytes: estimate.Bytes, Completed: true}, nil
}

const treeInstallation = "33333333-3333-4333-8333-333333333333"

// treeRoot builds tank/home as an actor root sending to the local pool backup
// under discard=all, with the given datasets beneath it. A name suffixed with
// "!" is excluded with enabled=off, as a hole, and one suffixed with "+" sets
// enabled=on itself, as a member beneath a hole must; the rest inherit.
func treeRoot(t *testing.T, beneath ...string) (*treePool, Request, *lifecycle.Service) {
	t.Helper()
	names := []string{"tank", "tank/home", "backup"}
	pool := newTreePool()
	properties := []zfs.Property{
		{Dataset: "tank/home", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/home", Name: policy.Namespace + "local", Value: "backup", Source: zfs.SourceLocal},
		{Dataset: "tank/home", Name: policy.Namespace + "discard", Value: "all", Source: zfs.SourceLocal},
		{Dataset: "tank/home", Name: policy.Namespace + "incremental", Value: "latest", Source: zfs.SourceLocal},
	}
	for _, name := range beneath {
		value := ""
		if hole, excluded := strings.CutSuffix(name, "!"); excluded {
			name, value = hole, "off"
		} else if member, enabled := strings.CutSuffix(name, "+"); enabled {
			name, value = member, "on"
		}
		names = append(names, "tank/home/"+name)
		if value != "" {
			properties = append(properties, zfs.Property{Dataset: "tank/home/" + name, Name: policy.Namespace + "enabled", Value: value, Source: zfs.SourceLocal})
		}
	}
	for _, name := range names {
		pool.create(name)
	}
	pool.props = append(pool.props, properties...)
	rootRows := slices.DeleteFunc(slices.Clone(properties), func(row zfs.Property) bool { return row.Dataset != "tank/home" })
	request := Request{Source: "tank/home", DestinationRoot: "backup"}
	request.Policy = policy.Resolve(zfs.Dataset{Name: "tank/home", Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rootRows, nil)
	resolved := map[string]policy.Effective{"tank/home": request.Policy}
	request.Members = []Member{{Dataset: "tank/home", Policy: request.Policy}}
	for _, name := range names[3:] {
		var rows []zfs.Property
		for _, property := range properties {
			if property.Dataset == name {
				rows = append(rows, property)
			}
		}
		parent := name[:strings.LastIndexByte(name, '/')]
		var inherited *policy.Effective
		if effective, found := resolved[parent]; found {
			inherited = &effective
		}
		resolved[name] = policy.Resolve(zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"}, inherited, rows, nil)
		if resolved[name].Enabled {
			request.Members = append(request.Members, Member{Dataset: name, Policy: resolved[name]})
		}
	}
	service, err := lifecycle.NewService(pool, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	return pool, request, service
}

func snapshotAt(t *testing.T, service *lifecycle.Service, request Request, at time.Time, members ...string) string {
	t.Helper()
	metadata, err := service.CreateSnapshot(t.Context(), request.Source, members, at, request.Policy)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.Name()
}

func referencesOn(t *testing.T, pool *treePool) []lifecycle.Reference {
	t.Helper()
	state, err := pool.InspectState(t.Context(), "tank/home", false)
	if err != nil {
		t.Fatal(err)
	}
	lineage, err := lifecycle.RootAuthority(state, "tank/home", treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := lifecycle.References(state, "tank/home", lineage)
	if err != nil {
		t.Fatal(err)
	}
	return refs
}

func TestMemberStreamsNestUnderTheRootsReplica(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "hole!", "hole/deep+")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	both := snapshotAt(t, service, request, start, "tank/home", "tank/home/hole/deep")
	deep := snapshotAt(t, service, request, start.Add(time.Hour), "tank/home/hole/deep")
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || len(result.Streams) != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	// discard=all maps the root to backup/home. Mapping the member by its own
	// name would put it at backup/deep, outside the root's replica entirely.
	for _, want := range []string{"backup/home@" + both, "backup/home/hole", "backup/home/hole/deep@" + deep} {
		if _, found := pool.object(want); !found {
			t.Errorf("missing %s", want)
		}
	}
	if pool.exists("backup/deep") {
		t.Error("member was received by its own name rather than beneath the root's replica")
	}
	// The hole is a placeholder: nothing was sent for it.
	if snapshots := lifecycle.Snapshots(must(pool.InspectState(t.Context(), "backup/home/hole", false)), "backup/home/hole"); len(snapshots) != 0 {
		t.Errorf("hole received snapshots: %v", snapshots)
	}
	refs := referencesOn(t, pool)
	streams := map[string]string{}
	for _, ref := range refs {
		streams[ref.Dataset] = ref.Metadata.Name()
	}
	if len(refs) != 2 || streams["tank/home"] != both || streams["tank/home/hole/deep"] != deep {
		t.Fatalf("each member's proof should be at its own newest instant: %+v", refs)
	}
	state := must(pool.InspectState(t.Context(), "tank/home", false))
	binding, err := storedTargetBinding(state, "tank/home", "local:backup")
	if err != nil || binding == nil || binding.Anchor != "backup/home" || binding.MappedDataset != "backup/home" {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
}

func TestFailedStreamSkipsOnlyItsDescendants(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "a", "a/child", "b")
	snapshotAt(t, service, request, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home", "tank/home/a", "tank/home/a/child", "tank/home/b")
	var sent []string
	engine, err := NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home/a": true}, sent: &sent}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	var failed *StreamsError
	if !errors.As(err, &failed) || len(failed.Failed) != 1 || failed.Failed[0].Dataset != "tank/home/a" || failed.Temporary() || result.Verified {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	// a/child would be received into a placeholder at backup/home/a, which
	// would then block a's own full receive for good.
	if want := []string{"tank/home", "tank/home/a", "tank/home/b"}; !slices.Equal(sent, want) {
		t.Fatalf("streams sent = %q, want %q", sent, want)
	}
	if !pool.exists("backup/home/b") || pool.exists("backup/home/a/child") {
		t.Fatal("an unrelated sibling was held back, or a descendant of the failure ran")
	}
}

func TestStreamReleasesOnlyItsOwnOlderProofs(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "fast")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	first := snapshotAt(t, service, request, start, "tank/home", "tank/home/fast")
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Apply(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	later := snapshotAt(t, service, request, start.Add(5*time.Minute), "tank/home/fast")
	if _, err := engine.Apply(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	streams := map[string]lifecycle.Reference{}
	for _, ref := range referencesOn(t, pool) {
		streams[ref.Dataset] = ref
	}
	// fast advanced and retired its own first proof; the root's proof at the
	// first instant is the root's only incremental base and must survive,
	// checkpoint included.
	if len(streams) != 2 || streams["tank/home"].Metadata.Name() != first || streams["tank/home/fast"].Metadata.Name() != later {
		t.Fatalf("proofs = %+v", streams)
	}
	if _, found := pool.object(streams["tank/home"].BookmarkName()); !found {
		t.Fatal("the root's checkpoint was destroyed by a member's advance")
	}
}

func TestMemberResumeIsVerifiedAgainstThatMemberAlone(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "slow")
	instant := snapshotAt(t, service, request, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home", "tank/home/slow")
	failing, err := NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home/slow": true}}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("injected member failure succeeded")
	}
	if pool.tokens["backup/home/slow"] == "" {
		t.Fatal("fixture: the failed member left no resume token")
	}
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || len(result.Streams) != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	slow := result.Streams[1]
	if slow.Plan.Source != "tank/home/slow" || !slow.Resumed || slow.ResumedSnapshot != "tank/home/slow@"+instant {
		t.Fatalf("slow stream = %+v", slow)
	}
	if result.Streams[0].Resumed || result.Streams[0].Plan.Mode != "up-to-date" {
		t.Fatalf("the root's stream was drawn into the member's resume: %+v", result.Streams[0])
	}
}

// A stream that verified its pinned endpoint completes its own pending work even
// when a sibling stream in the same job fails; the failed stream's work stays
// pending, pinned to the same endpoint, for the retry.
func TestPendingCompletesEachStreamThatVerified(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "ok", "broken")
	instant := snapshotAt(t, service, request, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home", "tank/home/ok", "tank/home/broken")
	const target = "local:backup"
	pending := &PendingSet{}
	for _, stream := range []string{"tank/home", "tank/home/ok", "tank/home/broken"} {
		object, _ := pool.object(stream + "@" + instant)
		if _, err := pending.Offer(stream, target, PendingSnapshot{Name: object.Name, CreateTXG: object.CreateTXG}); err != nil {
			t.Fatal(err)
		}
	}
	engine, err := NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home/broken": true}}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	pinned, reserved, err := pending.BeginStreams(request, target)
	if err != nil || len(reserved) != 3 {
		t.Fatalf("reserved=%v err=%v", reserved, err)
	}
	for _, member := range pinned.Members {
		if member.Snapshot != member.Dataset+"@"+instant {
			t.Fatalf("%s was not pinned to its pending endpoint: %q", member.Dataset, member.Snapshot)
		}
	}
	result, applyErr := engine.Apply(t.Context(), pinned, nil)
	pending.FinishStreams(target, reserved, result)
	if applyErr == nil {
		t.Fatal("injected member failure succeeded")
	}
	for stream, want := range map[string]bool{"tank/home": false, "tank/home/ok": false, "tank/home/broken": true} {
		if _, remains := pending.Peek(stream, target); remains != want {
			t.Errorf("%s pending = %t, want %t", stream, remains, want)
		}
	}
	if snapshot, found := pending.PeekScope("tank/home", target); !found || snapshot.Name != "tank/home/broken@"+instant {
		t.Errorf("the root's job no longer names the work it still carries: %+v", snapshot)
	}
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// setProperty replaces a dataset's local boomerangz property, or removes it
// when value is empty, as zfs set and zfs inherit would.
func (p *treePool) setProperty(dataset, key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.props = slices.DeleteFunc(p.props, func(row zfs.Property) bool {
		return row.Dataset == dataset && row.Name == policy.Namespace+key && row.Source == zfs.SourceLocal
	})
	if value != "" {
		p.props = append(p.props, zfs.Property{Dataset: dataset, Name: policy.Namespace + key, Value: value, Source: zfs.SourceLocal})
	}
}

// resolve builds the request for one local target from the pool's current
// properties, as the daemon does from a discovery generation: each dataset
// inherits from its parent, and members are the named datasets whose policy
// sends to the target. The root's own stream joins only if it sends there.
func (p *treePool) resolve(t *testing.T, target string, members ...string) Request {
	t.Helper()
	p.mu.Lock()
	rows := slices.Clone(p.props)
	p.mu.Unlock()
	effective := map[string]policy.Effective{}
	var walk func(name string) policy.Effective
	walk = func(name string) policy.Effective {
		if resolved, found := effective[name]; found {
			return resolved
		}
		var own []zfs.Property
		for _, row := range rows {
			if row.Dataset == name && strings.HasPrefix(row.Name, policy.Namespace) {
				own = append(own, row)
			}
		}
		var parent *policy.Effective
		if name != "tank/home" {
			inherited := walk(name[:strings.LastIndexByte(name, '/')])
			parent = &inherited
		}
		effective[name] = policy.Resolve(zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"}, parent, own, nil)
		return effective[name]
	}
	request := Request{Source: "tank/home", DestinationRoot: target, Policy: walk("tank/home")}
	for _, name := range append([]string{"tank/home"}, members...) {
		if resolved := walk(name); resolved.Enabled && slices.Contains(resolved.Local, target) {
			request.Members = append(request.Members, Member{Dataset: name, Policy: resolved})
		}
	}
	return request
}

func (p *treePool) isPlaceholder(t *testing.T, dataset string) bool {
	t.Helper()
	state, err := p.InspectState(t.Context(), dataset, false)
	if err != nil {
		t.Fatal(err)
	}
	return isPlaceholder(state, dataset)
}

func bindingAnchor(t *testing.T, pool *treePool, target string) string {
	t.Helper()
	state := must(pool.InspectState(t.Context(), "tank/home", false))
	binding, err := storedTargetBinding(state, "tank/home", "local:"+target)
	if err != nil || binding == nil {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
	return binding.Anchor
}

func TestHoleBecomingAMemberReplacesItsPlaceholder(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "hole!", "hole/deep+")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	first := snapshotAt(t, service, request, start, "tank/home", "tank/home/hole/deep")
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Apply(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	if !pool.isPlaceholder(t, "backup/home/hole") {
		t.Fatal("fixture: the hole's mapped name is not a marked placeholder")
	}
	deepGUID := must(pool.InspectDatasetIdentity(t.Context(), "backup/home/hole/deep")).GUID

	// Including the hole makes it a member that has never been sent: its full
	// receive has to replace the placeholder its own child sits beneath.
	pool.setProperty("tank/home/hole", "enabled", "")
	members := pool.resolve(t, "backup", "tank/home/hole", "tank/home/hole/deep")
	hole := snapshotAt(t, service, members, start.Add(time.Hour), "tank/home/hole")
	result, err := engine.Apply(t.Context(), members, nil)
	if err != nil || !result.Verified || len(result.Streams) != 3 || result.Streams[1].Plan.Mode != "full" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if pool.isPlaceholder(t, "backup/home/hole") {
		t.Fatal("the placeholder was not replaced")
	}
	if _, found := pool.object("backup/home/hole@" + hole); !found {
		t.Fatal("the hole's replica was not received")
	}
	// The member beneath moved intact: same dataset, same history.
	if moved := must(pool.InspectDatasetIdentity(t.Context(), "backup/home/hole/deep")).GUID; moved != deepGUID {
		t.Fatalf("the member beneath was recreated rather than moved: %d, want %d", moved, deepGUID)
	}
	if _, found := pool.object("backup/home/hole/deep@" + first); !found {
		t.Fatal("the member beneath lost its history")
	}
	if pool.exists(placeholderAside("backup/home/hole")) {
		t.Fatal("the set-aside placeholder was left behind")
	}
	// And it carries on incrementally from that history.
	next := snapshotAt(t, service, members, start.Add(2*time.Hour), "tank/home/hole/deep")
	result, err = engine.Apply(t.Context(), members, nil)
	if err != nil || result.Streams[2].Plan.Mode != "incremental-latest" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, found := pool.object("backup/home/hole/deep@" + next); !found {
		t.Fatal("the moved member did not advance")
	}
}

func TestInterruptedPlaceholderReplacementCompletesNextAttempt(t *testing.T) {
	t.Parallel()
	for name, interrupt := range map[string]func(*treePool) treeStream{
		// The placeholder is set aside and the replica's receive fails,
		// leaving it resumable.
		"receive": func(pool *treePool) treeStream {
			return treeStream{pool: pool, failing: map[string]bool{"tank/home/hole": true}}
		},
		// The replica verifies, then moving the placeholder's children
		// beneath it fails.
		"adoption": func(pool *treePool) treeStream {
			pool.failRename[placeholderAside("backup/home/hole")+"/deep"] = true
			return treeStream{pool: pool}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, request, service := treeRoot(t, "hole!", "hole/deep+")
			start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			first := snapshotAt(t, service, request, start, "tank/home", "tank/home/hole/deep")
			engine := must(NewLocal(pool, treeStream{pool: pool}, treeInstallation))
			if _, err := engine.Apply(t.Context(), request, nil); err != nil {
				t.Fatal(err)
			}
			pool.setProperty("tank/home/hole", "enabled", "")
			members := pool.resolve(t, "backup", "tank/home/hole", "tank/home/hole/deep")
			snapshotAt(t, service, members, start.Add(time.Hour), "tank/home/hole")
			interrupted := must(NewLocal(pool, interrupt(pool), treeInstallation))
			if _, err := interrupted.Apply(t.Context(), members, nil); err == nil {
				t.Fatal("the injected interruption succeeded")
			}
			if !pool.exists(placeholderAside("backup/home/hole")) {
				t.Fatal("fixture: the interruption left nothing set aside")
			}
			result, err := engine.Apply(t.Context(), members, nil)
			if err != nil || !result.Verified {
				t.Fatalf("the next attempt did not finish the replacement: result=%+v err=%v", result, err)
			}
			if pool.exists(placeholderAside("backup/home/hole")) || pool.isPlaceholder(t, "backup/home/hole") {
				t.Fatal("the replacement was left half done")
			}
			if _, found := pool.object("backup/home/hole/deep@" + first); !found {
				t.Fatal("the member beneath lost its history")
			}
		})
	}
}

// A target only a member sends to needs the root's replica as a placeholder.
// The binding must never anchor on it, or the root joining that target later
// would find its replica's name taken by a dataset the binding vouches for.
func TestMemberOnlyTargetNeverAnchorsOnAPlaceholder(t *testing.T) {
	t.Parallel()
	pool, _, service := treeRoot(t, "docs")
	pool.setProperty("tank/home", "local", "")
	pool.setProperty("tank/home/docs", "local", "backup")
	request := pool.resolve(t, "backup", "tank/home/docs")
	if len(request.Members) != 1 || request.Members[0].Dataset != "tank/home/docs" {
		t.Fatalf("fixture: members = %+v", request.Members)
	}
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	docs := snapshotAt(t, service, request, start, "tank/home", "tank/home/docs")
	engine := must(NewLocal(pool, treeStream{pool: pool}, treeInstallation))
	if _, err := engine.Apply(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	if !pool.isPlaceholder(t, "backup/home") {
		t.Fatal("fixture: the root's replica is not a placeholder")
	}
	if anchor := bindingAnchor(t, pool, "backup"); anchor != "backup" {
		t.Fatalf("the binding anchored on %s, want the real ancestor backup", anchor)
	}
	// A later pass must not read the placeholder as the root's replica.
	if result, err := engine.Apply(t.Context(), request, nil); err != nil || !result.Verified {
		t.Fatalf("result=%+v err=%v", result, err)
	}

	// The root joins the target: its replica replaces the placeholder and
	// the binding is promoted to it as an ordinary bootstrap.
	pool.setProperty("tank/home", "local", "backup")
	joined := pool.resolve(t, "backup", "tank/home/docs")
	root := snapshotAt(t, service, joined, start.Add(time.Hour), "tank/home")
	result, err := engine.Apply(t.Context(), joined, nil)
	if err != nil || !result.Verified || result.Streams[0].Plan.Mode != "full" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, found := pool.object("backup/home@" + root); !found || pool.isPlaceholder(t, "backup/home") {
		t.Fatal("the root's replica did not replace its placeholder")
	}
	if _, found := pool.object("backup/home/docs@" + docs); !found {
		t.Fatal("the member's replica did not move beneath the root's")
	}
	if anchor := bindingAnchor(t, pool, "backup"); anchor != "backup/home" {
		t.Fatalf("the binding stayed on %s, want the root's replica", anchor)
	}
}

// A placeholder with a resumable receive beneath it waits for that receive:
// its stream is deferred, the stream beneath runs and finishes, and the next
// attempt replaces the placeholder.
func TestPlaceholderWaitsForTheReceiveBeneathIt(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "hole!", "hole/deep+")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	snapshotAt(t, service, request, start, "tank/home", "tank/home/hole/deep")
	failing := must(NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home/hole/deep": true}}, treeInstallation))
	if _, err := failing.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("injected failure succeeded")
	}
	if pool.tokens["backup/home/hole/deep"] == "" {
		t.Fatal("fixture: no resumable receive beneath the placeholder")
	}
	pool.setProperty("tank/home/hole", "enabled", "")
	members := pool.resolve(t, "backup", "tank/home/hole", "tank/home/hole/deep")
	snapshotAt(t, service, members, start.Add(time.Hour), "tank/home/hole")
	engine := must(NewLocal(pool, treeStream{pool: pool}, treeInstallation))
	result, err := engine.Apply(t.Context(), members, nil)
	var failed *StreamsError
	if !errors.As(err, &failed) || len(failed.Failed) != 1 || failed.Failed[0].Dataset != "tank/home/hole" || !failed.Temporary() {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if deep := result.Streams[len(result.Streams)-1]; deep.Plan.Source != "tank/home/hole/deep" || !deep.Verified || !deep.Resumed {
		t.Fatalf("the receive beneath was held back: %+v", deep)
	}
	if result, err := engine.Apply(t.Context(), members, nil); err != nil || !result.Verified || pool.isPlaceholder(t, "backup/home/hole") {
		t.Fatalf("the placeholder was not replaced once free: result=%+v err=%v", result, err)
	}
}

// A dataset without the marker is not boomerangz's to move, whatever it
// holds: the stream fails closed as it always has.
func TestUnmarkedDestinationIsNeverReplaced(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "other")
	pool.create("backup/home")
	pool.create("backup/home/other")
	snapshotAt(t, service, request, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home", "tank/home/other")
	engine := must(NewLocal(pool, treeStream{pool: pool}, treeInstallation))
	if _, err := engine.Apply(t.Context(), request, nil); err == nil || !strings.Contains(err.Error(), "no common base") {
		t.Fatalf("an unmarked destination was not refused: %v", err)
	}
	if pool.exists(placeholderAside("backup/home")) || pool.exists(placeholderAside("backup/home/other")) {
		t.Fatal("an unmarked dataset was moved")
	}
}

// A reseed while the root's own replica is replacing its placeholder must
// destroy the set-aside placeholder too: it sits beside the root's replica,
// outside what the reseed destroys, holding member replicas whose proofs the
// reseed releases. Left behind, they would be moved back beneath the fresh
// replica as if they were current.
func TestReseedDestroysASetAsideRootPlaceholder(t *testing.T) {
	t.Parallel()
	pool, _, service := treeRoot(t, "docs")
	pool.setProperty("tank/home", "local", "")
	pool.setProperty("tank/home/docs", "local", "backup")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	memberOnly := pool.resolve(t, "backup", "tank/home/docs")
	snapshotAt(t, service, memberOnly, start, "tank/home", "tank/home/docs")
	engine := must(NewLocal(pool, treeStream{pool: pool}, treeInstallation))
	if _, err := engine.Apply(t.Context(), memberOnly, nil); err != nil {
		t.Fatal(err)
	}
	pool.setProperty("tank/home", "local", "backup")
	joined := pool.resolve(t, "backup", "tank/home/docs")
	snapshotAt(t, service, joined, start.Add(time.Hour), "tank/home")
	failing := must(NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home": true}}, treeInstallation))
	if _, err := failing.Apply(t.Context(), joined, nil); err == nil {
		t.Fatal("injected root failure succeeded")
	}
	aside := placeholderAside("backup/home")
	if !pool.exists(aside + "/docs") {
		t.Fatal("fixture: the member replica is not waiting beneath the set-aside placeholder")
	}
	reseed := must(NewReseedService(pool, pool, treeInstallation))
	plan, err := reseed.Apply(t.Context(), joined)
	if err != nil || !plan.Applied || plan.SetAside != aside || !slices.Contains(plan.DestinationObjects, aside+"/docs") {
		t.Fatalf("reseed=%+v err=%v", plan, err)
	}
	if pool.exists(aside) || pool.exists("backup/home") {
		t.Fatal("reseed left the root's replica or its set-aside placeholder behind")
	}
	result, err := engine.Apply(t.Context(), joined, nil)
	if err != nil || !result.Verified {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, stream := range result.Streams {
		if stream.Plan.Mode != "full" {
			t.Errorf("%s resumed stale history as %q after a reseed", stream.Plan.Source, stream.Plan.Mode)
		}
	}
}

func TestRecursiveStreamRefusesAPerMemberReplica(t *testing.T) {
	t.Parallel()
	pool, request, service := treeRoot(t, "member", "hole!", "hole/deep+")
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	snapshotAt(t, service, request, start, "tank/home", "tank/home/member", "tank/home/hole/deep")
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if result, applyErr := engine.Apply(t.Context(), request, nil); applyErr != nil || !result.Verified {
		t.Fatalf("result=%+v err=%v", result, applyErr)
	}
	// The hole was never sent, so a send -R stream has no common base for it;
	// the switch must say a reseed is needed rather than fail mid-protection.
	recursive := Request{Source: request.Source, DestinationRoot: request.DestinationRoot, Policy: request.Policy.Clone()}
	recursive.Policy.Send.Replicate = true
	later := snapshotAt(t, service, recursive, start.Add(time.Hour), "tank/home", "tank/home/member", "tank/home/hole", "tank/home/hole/deep")
	before := referencesOn(t, pool)
	_, err = engine.Apply(t.Context(), recursive, nil)
	if err == nil || !strings.Contains(err.Error(), "reseed") {
		t.Fatalf("err=%v, want a reseed instruction", err)
	}
	if _, found := pool.object("backup/home@" + later); found {
		t.Fatal("the refused stream was received")
	}
	if after := referencesOn(t, pool); !reflect.DeepEqual(after, before) {
		t.Fatalf("the refusal changed the proofs: %+v, was %+v", after, before)
	}
}

func TestRemoteMemberNeedsTheConfiguredRootUnderDiscardOff(t *testing.T) {
	t.Parallel()
	// Only the member sends to the remote. Under discard=off the root's
	// replica is the remote's configured root, which the remote never creates
	// as a receive parent, so the member's bootstrap must say to create it
	// rather than fail on the remote's scope refusal.
	pool, request, service := treeRoot(t, "member")
	rootPolicy := request.Policy.Clone()
	rootPolicy.Discard, rootPolicy.Local = policy.DiscardOff, nil
	memberPolicy := rootPolicy.Clone()
	memberPolicy.Remote = []string{"offsite"}
	remote := Request{
		Source: "tank/home", DestinationRoot: "backup/offsite", Policy: rootPolicy,
		Members:   []Member{{Dataset: "tank/home/member", Policy: memberPolicy}},
		Transport: "ssh", RemoteName: "offsite", CanonicalTarget: "ssh://replicator@offsite.example.net:22/backup/offsite",
		// The fake serves both ends; the remote is taken as validated.
		HostLocal: true,
	}
	snapshotAt(t, service, remote, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home/member")
	engine, err := NewRemote(pool, pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Apply(t.Context(), remote, nil)
	if err == nil || !strings.Contains(err.Error(), "create it on the remote") {
		t.Fatalf("err=%v, want an instruction to create the configured root", err)
	}
	if pool.exists("backup/offsite") {
		t.Fatal("the remote's configured root was created")
	}
	pool.create("backup/offsite")
	result, err := engine.Apply(t.Context(), remote, nil)
	if err != nil || !result.Verified || !pool.exists("backup/offsite/member") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

// interruptedMember leaves tank/home/slow's receive interrupted on backup.
func interruptedMember(t *testing.T) (*treePool, Request) {
	t.Helper()
	pool, request, service := treeRoot(t, "slow")
	snapshotAt(t, service, request, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "tank/home", "tank/home/slow")
	failing, err := NewLocal(pool, treeStream{pool: pool, failing: map[string]bool{"tank/home/slow": true}}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("injected member failure succeeded")
	}
	if pool.tokens["backup/home/slow"] == "" {
		t.Fatal("fixture: the failed member left no resume token")
	}
	return pool, request
}

func TestAbortDiscardsOnlyTheInterruptedReceives(t *testing.T) {
	t.Parallel()
	pool, request := interruptedMember(t)
	before := referencesOn(t, pool)
	service, err := NewReseedService(pool, pool, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := service.PlanAbort(t.Context(), request)
	if err != nil || preview.Applied || !slices.Equal(preview.Receives, []string{"backup/home/slow"}) {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	if pool.tokens["backup/home/slow"] == "" {
		t.Fatal("the preview discarded the receive")
	}
	applied, err := service.ApplyAbort(t.Context(), request)
	if err != nil || !applied.Applied {
		t.Fatalf("abort=%+v err=%v", applied, err)
	}
	if pool.tokens["backup/home/slow"] != "" {
		t.Fatal("the interrupted receive survived")
	}
	// The replica, the binding and every proof stay.
	if !pool.exists("backup/home") || bindingAnchor(t, pool, "backup") != "backup/home" {
		t.Fatal("the abort touched more than the interrupted receive")
	}
	if after := referencesOn(t, pool); !reflect.DeepEqual(after, before) {
		t.Fatalf("the abort changed the proofs: %+v, was %+v", after, before)
	}
	// The next transfer sends the member afresh rather than resuming.
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || len(result.Streams) != 2 || result.Streams[1].Resumed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestUnresumableReceiveNamesTheAbortCommand(t *testing.T) {
	t.Parallel()
	pool, request := interruptedMember(t)
	// Without the hold its proof relies on, the resume cannot be validated,
	// and nothing a transfer does on its own would clear the token.
	for _, ref := range referencesOn(t, pool) {
		if ref.Dataset != "tank/home/slow" {
			continue
		}
		for _, source := range ref.Sources {
			if err := pool.Release(t.Context(), ref.HoldName(), source.Dataset+"@"+source.Metadata.Name()); err != nil {
				t.Fatal(err)
			}
		}
	}
	engine, err := NewLocal(pool, treeStream{pool: pool}, treeInstallation)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Apply(t.Context(), request, nil)
	if err == nil || !strings.Contains(err.Error(), "boomerangz dataset abort-receive tank/home backup") {
		t.Fatalf("err=%v, want the abort command", err)
	}
}
