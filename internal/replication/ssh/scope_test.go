package ssh

import (
	"context"
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
