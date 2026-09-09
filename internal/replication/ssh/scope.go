package ssh

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/pdf/boomerangz/internal/replication/scope"
	"github.com/pdf/boomerangz/internal/zfs"
)

// scopedExecutor prevents the direct SSH backend from issuing application
// operations outside its configured destination subtree and required ancestors.
type scopedExecutor struct {
	backend zfs.Executor
	root    string
}

func (e *scopedExecutor) related(dataset string) bool {
	return scope.Related(e.root, dataset)
}
func (e *scopedExecutor) inside(dataset string) bool {
	return scope.Inside(e.root, dataset)
}
func (e *scopedExecutor) ListDatasets(ctx context.Context) ([]zfs.Dataset, error) {
	all, err := e.backend.ListDatasets(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(dataset zfs.Dataset) bool { return !e.related(dataset.Name) }), nil
}
func (e *scopedExecutor) InspectDatasetIdentity(ctx context.Context, dataset string) (zfs.DatasetIdentity, error) {
	if !e.related(dataset) {
		return zfs.DatasetIdentity{}, fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	return e.backend.InspectDatasetIdentity(ctx, dataset)
}
func (e *scopedExecutor) InspectState(ctx context.Context, dataset string, recursive bool) (zfs.State, error) {
	if !e.inside(dataset) {
		return zfs.State{}, fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	return e.backend.InspectState(ctx, dataset, recursive)
}
func (e *scopedExecutor) CheckPermissions(ctx context.Context, dataset string, permissions []string) error {
	if !e.related(dataset) {
		return fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	return e.backend.CheckPermissions(ctx, dataset, slices.Clone(permissions))
}
func (e *scopedExecutor) CreateReceiveParent(ctx context.Context, dataset string) error {
	if !e.inside(dataset) || dataset == e.root {
		return fmt.Errorf("receive ancestor is outside configured SSH destination scope")
	}
	return e.backend.CreateReceiveParent(ctx, dataset)
}
func (e *scopedExecutor) AbortReceive(ctx context.Context, dataset string) error {
	if !e.inside(dataset) {
		return fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	backend, ok := e.backend.(zfs.ReseedExecutor)
	if !ok {
		return fmt.Errorf("destination reseed operations are unavailable")
	}
	return backend.AbortReceive(ctx, dataset)
}
func (e *scopedExecutor) DestroyDataset(ctx context.Context, dataset string, recursive bool) error {
	if !e.inside(dataset) {
		return fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	backend, ok := e.backend.(zfs.ReseedExecutor)
	if !ok {
		return fmt.Errorf("destination reseed operations are unavailable")
	}
	return backend.DestroyDataset(ctx, dataset, recursive)
}
func (e *scopedExecutor) SetProperties(ctx context.Context, dataset string, properties map[string]string) error {
	if !e.inside(dataset) {
		return fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	return e.backend.SetProperties(ctx, dataset, maps.Clone(properties))
}
func (e *scopedExecutor) InheritProperty(ctx context.Context, dataset, property string) error {
	if !e.inside(dataset) {
		return fmt.Errorf("dataset is outside configured SSH destination scope")
	}
	return e.backend.InheritProperty(ctx, dataset, property)
}
func (*scopedExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) Snapshot(context.Context, string, string, bool, map[string]string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) DestroySnapshot(context.Context, string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) Bookmark(context.Context, string, string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) DestroyBookmark(context.Context, string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) Hold(context.Context, string, string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
func (*scopedExecutor) Release(context.Context, string, string) error {
	return fmt.Errorf("source operations are unavailable through a destination endpoint")
}
