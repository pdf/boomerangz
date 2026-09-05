// Package zfs provides typed, shell-free access to OpenZFS operations.
package zfs

import "context"

// DatasetType identifies a manageable ZFS filesystem or volume.
type DatasetType string

// Dataset types included in sparse discovery.
const (
	Filesystem DatasetType = "filesystem"
	Volume     DatasetType = "volume"
)

// Dataset is a sparse inventory row.
type Dataset struct {
	Name           string
	Type           DatasetType
	EncryptionRoot string
}

// PropertySource records whether a property is locally set or received.
type PropertySource string

// Supported stored-property sources.
const (
	SourceLocal    PropertySource = "local"
	SourceReceived PropertySource = "received"
)

// Property is an explicitly stored boomerangz user property.
type Property struct {
	Dataset string
	Name    string
	Value   string
	Source  PropertySource
}

// Executor is the typed boundary for every ZFS mutation and query. A future
// privileged backend must implement these operations without accepting raw
// commands or flags from its client.
type Executor interface {
	ListDatasets(context.Context) ([]Dataset, error)
	GetActivationProperties(context.Context) ([]Property, error)
	GetStoredProperties(context.Context, []string) ([]Property, error)
	Snapshot(context.Context, string, string, bool, map[string]string) error
	DestroySnapshot(context.Context, string) error
	Bookmark(context.Context, string, string) error
	DestroyBookmark(context.Context, string) error
	Hold(context.Context, string, string) error
	Release(context.Context, string, string) error
}
