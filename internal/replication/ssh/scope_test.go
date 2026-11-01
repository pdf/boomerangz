package ssh

import (
	"context"
	"slices"
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

type renameBackend struct {
	zfs.Executor
	renamed [][2]string
}

func (b *renameBackend) RenameDataset(_ context.Context, from, to string) error {
	b.renamed = append(b.renamed, [2]string{from, to})
	return nil
}

func (*renameBackend) DestroyDataset(context.Context, string, bool) error { return nil }

func TestScopedRenameStaysInsideTheConfiguredRoot(t *testing.T) {
	t.Parallel()
	backend := &renameBackend{}
	scoped := &scopedExecutor{backend: backend, root: "tank/a"}
	if err := scoped.RenameDataset(t.Context(), "tank/a/hole", "tank/a/hole.boomerangz-placeholder"); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"tank/a", "tank/a/x"}, {"tank/a/x", "tank/a"}, {"tank/a/x", "tank/b/x"}, {"tank/b/x", "tank/a/x"}} {
		if err := scoped.RenameDataset(t.Context(), pair[0], pair[1]); err == nil {
			t.Errorf("renamed %s to %s", pair[0], pair[1])
		}
	}
	if len(backend.renamed) != 1 {
		t.Fatalf("backend saw %v", backend.renamed)
	}
}

type snapshotBackend struct {
	zfs.Destination
	destroyed []string
}

func (b *snapshotBackend) DestroySnapshot(_ context.Context, snapshot string) error {
	b.destroyed = append(b.destroyed, snapshot)
	return nil
}

func TestScopedDestroySnapshotStaysInsideTheConfiguredRoot(t *testing.T) {
	t.Parallel()
	backend := &snapshotBackend{}
	scoped := &scopedExecutor{backend: backend, root: "tank/a"}
	for _, snapshot := range []string{"tank/a@s", "tank/a/x@s"} {
		if err := scoped.DestroySnapshot(t.Context(), snapshot); err != nil {
			t.Fatalf("destroy %s: %v", snapshot, err)
		}
	}
	for _, snapshot := range []string{"tank@s", "tank/b@s", "tank/ab@s", "tank/a", "tank/a/x", "tank/a#b", "tank/a@s,t", "tank/a@s%t"} {
		if err := scoped.DestroySnapshot(t.Context(), snapshot); err == nil {
			t.Errorf("destroyed %s", snapshot)
		}
	}
	if !slices.Equal(backend.destroyed, []string{"tank/a@s", "tank/a/x@s"}) {
		t.Fatalf("backend saw %v", backend.destroyed)
	}
}
