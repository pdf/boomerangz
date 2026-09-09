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

// DatasetIdentity is the stable local identity of an exact dataset and its
// containing pool. Both GUIDs are required because names survive neither pool
// replacement nor dataset replacement safely.
type DatasetIdentity struct {
	Name     string
	Type     DatasetType
	GUID     uint64
	Pool     string
	PoolGUID uint64
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
	InspectDatasetIdentity(context.Context, string) (DatasetIdentity, error)
	CheckPermissions(context.Context, string, []string) error
	GetActivationProperties(context.Context) ([]Property, error)
	GetStoredProperties(context.Context, []string) ([]Property, error)
	InspectState(context.Context, string, bool) (State, error)
	CreateReceiveParent(context.Context, string) error
	SetProperties(context.Context, string, map[string]string) error
	InheritProperty(context.Context, string, string) error
	Snapshot(context.Context, string, string, bool, map[string]string) error
	DestroySnapshot(context.Context, string) error
	Bookmark(context.Context, string, string) error
	DestroyBookmark(context.Context, string) error
	Hold(context.Context, string, string) error
	Release(context.Context, string, string) error
}

// ReseedExecutor exposes the destructive destination operations used only by
// the explicit, preview-first reseed workflow.
type ReseedExecutor interface {
	Executor
	AbortReceive(context.Context, string) error
	DestroyDataset(context.Context, string, bool) error
}

// Object is a filesystem, volume, snapshot, or bookmark in a lifecycle query.
type Object struct {
	Name      string
	Type      string
	GUID      uint64
	Creation  int64
	CreateTXG uint64
}

// State includes effective operational values and explicit namespace properties.
// Received records known received values, including masked values for known keys.
// ZFS CLI enumeration cannot discover every hidden dynamic property name.
type State struct {
	Objects      []Object
	Properties   []Property
	Received     map[string]map[string]string
	ResumeTokens map[string]string
	Clones       map[string][]string
	Holds        map[string][]string
}
