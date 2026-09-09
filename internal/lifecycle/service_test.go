package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type memoryBackend struct {
	state      zfs.State
	reads      int
	beforeRead func(*memoryBackend)
	writes     []string
}

func (b *memoryBackend) InspectState(ctx context.Context, _ string, _ bool) (zfs.State, error) {
	if err := ctx.Err(); err != nil {
		return zfs.State{}, err
	}
	b.reads++
	if b.beforeRead != nil {
		b.beforeRead(b)
	}
	var cloned zfs.State
	data, err := json.Marshal(b.state)
	if err != nil {
		return cloned, err
	}
	err = json.Unmarshal(data, &cloned)
	return cloned, err
}

func (b *memoryBackend) SetProperties(_ context.Context, object string, values map[string]string) error {
	b.writes = append(b.writes, "set "+object)
	for key, value := range values {
		b.state.Properties = slices.DeleteFunc(b.state.Properties, func(p zfs.Property) bool { return p.Dataset == object && p.Name == key && p.Source == zfs.SourceLocal })
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: object, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	return nil
}

func (b *memoryBackend) Snapshot(_ context.Context, dataset, name string, recursive bool, values map[string]string) error {
	b.writes = append(b.writes, "snapshot "+dataset)
	objects := append([]zfs.Object(nil), b.state.Objects...)
	for _, object := range objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		if object.Name != dataset && (!recursive || !strings.HasPrefix(object.Name, dataset+"/")) {
			continue
		}
		full := object.Name + "@" + name
		b.state.Objects = append(b.state.Objects, zfs.Object{Name: full, Type: "snapshot", GUID: uint64(100 + len(b.state.Objects))})
		for key, value := range values {
			b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: full, Name: key, Value: value, Source: zfs.SourceLocal})
		}
	}
	return nil
}

func (b *memoryBackend) DestroySnapshot(_ context.Context, name string) error {
	b.writes = append(b.writes, "destroy "+name)
	for i, object := range b.state.Objects {
		if object.Name == name {
			b.state.Objects = append(b.state.Objects[:i], b.state.Objects[i+1:]...)
			b.state.Properties = slices.DeleteFunc(b.state.Properties, func(p zfs.Property) bool { return p.Dataset == name })
			delete(b.state.Holds, name)
			delete(b.state.Clones, name)
			delete(b.state.Received, name)
			return nil
		}
	}
	return fmt.Errorf("snapshot absent")
}

func (b *memoryBackend) GetActivationProperties(_ context.Context) ([]zfs.Property, error) {
	var rows []zfs.Property
	for _, p := range b.state.Properties {
		if p.Name == policy.Namespace+"enabled" {
			rows = append(rows, p)
		}
	}
	return rows, nil
}

func (b *memoryBackend) Hold(_ context.Context, tag, snapshot string) error {
	if b.state.Holds == nil {
		b.state.Holds = map[string][]string{}
	}
	b.state.Holds[snapshot] = append(b.state.Holds[snapshot], tag)
	b.writes = append(b.writes, "hold "+snapshot)
	return nil
}
func (b *memoryBackend) Release(_ context.Context, tag, snapshot string) error {
	b.state.Holds[snapshot] = slices.DeleteFunc(b.state.Holds[snapshot], func(value string) bool { return value == tag })
	b.writes = append(b.writes, "release "+snapshot)
	return nil
}
func (b *memoryBackend) Bookmark(_ context.Context, snapshot, bookmark string) error {
	for _, o := range b.state.Objects {
		if o.Name == snapshot {
			b.state.Objects = append(b.state.Objects, zfs.Object{Name: bookmark, Type: "bookmark", GUID: o.GUID})
			b.writes = append(b.writes, "bookmark "+bookmark)
			return nil
		}
	}
	return fmt.Errorf("snapshot absent")
}
func (b *memoryBackend) DestroyBookmark(ctx context.Context, name string) error {
	return b.DestroySnapshot(ctx, name)
}
func (b *memoryBackend) InheritProperty(_ context.Context, object, key string) error {
	b.state.Properties = slices.DeleteFunc(b.state.Properties, func(p zfs.Property) bool { return p.Dataset == object && p.Name == key })
	b.writes = append(b.writes, "inherit "+object+" "+key)
	return nil
}

func backendWithSnapshots(t *testing.T) *memoryBackend {
	t.Helper()
	b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{{Name: "tank/data", Type: "filesystem", GUID: 1}}}}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		snapshot := owned(t, now.Add(-time.Duration(i)*time.Hour))
		b.state.Objects = append(b.state.Objects, zfs.Object{Name: snapshot.Name, Type: "snapshot", GUID: uint64(i + 2)})
		b.state.Properties = append(b.state.Properties, snapshot.Properties...)
	}
	return b
}

func activeTestPolicy(dataset string) policy.Effective {
	row := zfs.Property{Dataset: dataset, Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal}
	return policy.Resolve(zfs.Dataset{Name: dataset, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, []zfs.Property{row}, nil)
}

func addTestAuthority(b *memoryBackend) {
	b.state.Properties = append(b.state.Properties,
		zfs.Property{Dataset: "tank/data", Name: LineageProperty, Value: testLineage, Source: zfs.SourceLocal},
		zfs.Property{Dataset: "tank/data", Name: OwnerProperty, Value: testInstallation, Source: zfs.SourceLocal},
	)
}

func TestServiceCreatesAndVerifiesSnapshots(t *testing.T) {
	t.Parallel()
	for _, recursive := range []bool{false, true} {
		b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{{Name: "tank/data", Type: "filesystem", GUID: 1}}}}
		if recursive {
			b.state.Objects = append(b.state.Objects, zfs.Object{Name: "tank/data/child", Type: "volume", GUID: 2})
		}
		s, err := NewService(b, testInstallation)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := s.CreateSnapshot(t.Context(), "tank/data", recursive, time.Now(), activeTestPolicy("tank/data"))
		if err != nil {
			t.Fatal(err)
		}
		if !ValidID(metadata.Lineage) || len(b.writes) != 2 {
			t.Fatalf("metadata=%v writes=%v", metadata, b.writes)
		}
		if _, err := Ownership(snapshotsIn(b.state, "tank/data")[0], metadata.Lineage); err != nil {
			t.Fatal(err)
		}
		if recursive && len(snapshotsIn(b.state, "tank/data/child")) != 1 {
			t.Fatal("missing recursive child")
		}
	}
}

func TestServiceAdoptionAndPruning(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	s, _ := NewService(b, testInstallation)
	if _, err := s.CreateSnapshot(t.Context(), "tank/data", false, time.Now(), activeTestPolicy("tank/data")); err == nil || len(b.writes) != 0 {
		t.Fatal("silently replaced lost lineage")
	}
	lineage, err := s.AdoptDataset(t.Context(), "tank/data", activeTestPolicy("tank/data"))
	if err != nil || lineage != testLineage {
		t.Fatalf("adopt: %s %v", lineage, err)
	}
	if _, err := s.AdoptDataset(t.Context(), "tank/data", activeTestPolicy("tank/data")); err == nil {
		t.Fatal("overwrote lineage")
	}
	grid, _ := policy.ParseGrid("1x5m")
	effective := activeTestPolicy("tank/data")
	effective.Grid = grid
	preview, err := s.Prune(t.Context(), "tank/data", effective, false)
	if err != nil || len(preview) != 3 || len(b.writes) != 1 {
		t.Fatalf("preview: %v %v", preview, err)
	}
	if _, err := s.Prune(t.Context(), "tank/data", effective, true); err != nil {
		t.Fatal(err)
	}
	if len(b.writes) != 3 {
		t.Fatalf("writes=%v", b.writes)
	}
}

func TestServiceRefusesChangedOrResumableState(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"guid", "hold", "resume", "lineage"} {
		t.Run(change, func(t *testing.T) {
			b := backendWithSnapshots(t)
			addTestAuthority(b)
			b.beforeRead = func(b *memoryBackend) {
				if b.reads != 2 {
					return
				}
				switch change {
				case "guid":
					for i := range b.state.Objects {
						if b.state.Objects[i].Type == "snapshot" {
							b.state.Objects[i].GUID += 100
						}
					}
				case "hold":
					b.state.Holds = map[string][]string{}
					for _, o := range b.state.Objects {
						b.state.Holds[o.Name] = []string{"foreign"}
					}
				case "resume":
					b.state.ResumeTokens = map[string]string{"tank/data": "token"}
				case "lineage":
					b.state.Properties[len(b.state.Properties)-1].Value = "invalid"
				}
			}
			s, _ := NewService(b, testInstallation)
			grid, _ := policy.ParseGrid("1x5m")
			effective := activeTestPolicy("tank/data")
			effective.Grid = grid
			if _, err := s.Prune(t.Context(), "tank/data", effective, true); err == nil || len(b.writes) != 0 {
				t.Fatalf("mutation after %s: %v %v", change, b.writes, err)
			}
		})
	}
}

func TestServiceRejectsHiddenLineageAndCancelledContext(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	b.state.Received = map[string]map[string]string{"tank/data": {LineageProperty: testLineage}}
	s, _ := NewService(b, testInstallation)
	if _, err := s.AdoptDataset(t.Context(), "tank/data", activeTestPolicy("tank/data")); err == nil {
		t.Fatal("adopted over hidden lineage")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.CreateSnapshot(ctx, "tank/data", false, time.Now(), activeTestPolicy("tank/data")); err == nil || len(b.writes) != 0 {
		t.Fatal("mutated cancelled scope")
	}
}

func TestHierarchyLocksSerializeOverlappingRoots(t *testing.T) {
	t.Parallel()
	locks := newHierarchyLocks()
	releaseRoot, err := locks.acquire(t.Context(), "tank/data")
	if err != nil {
		t.Fatal(err)
	}
	childAcquired := make(chan func(), 1)
	go func() {
		release, acquireErr := locks.acquire(t.Context(), "tank/data/home")
		if acquireErr == nil {
			childAcquired <- release
		}
	}()
	select {
	case release := <-childAcquired:
		release()
		t.Fatal("overlapping descendant lock was not blocked")
	case <-time.After(20 * time.Millisecond):
	}
	releaseDisjoint, err := locks.acquire(t.Context(), "backup/data")
	if err != nil {
		t.Fatal(err)
	}
	releaseDisjoint()
	releaseRoot()
	select {
	case release := <-childAcquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("descendant lock was not released")
	}
}
