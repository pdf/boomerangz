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
	// Placeholder reports PlaceholderProperty set locally on the dataset: a
	// receive parent boomerangz created and has not yet replaced. It is read
	// in the same query, so ruling a placeholder out costs nothing extra.
	Placeholder bool
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

// Destination is the typed boundary a replication target exposes: the queries
// and mutations that receiving, verifying, reconciling, pruning, and retiring
// replicas need. A remote endpoint implements exactly this, so code that holds
// a Destination cannot reach an operation a remote does not carry.
type Destination interface {
	ListDatasets(context.Context) ([]Dataset, error)
	InspectDatasetIdentity(context.Context, string) (DatasetIdentity, error)
	CheckPermissions(context.Context, string, []string) error
	InspectState(context.Context, string, bool) (State, error)
	CreateReceiveParent(context.Context, string) error
	SetProperties(context.Context, string, map[string]string) error
	InheritProperty(context.Context, string, string) error
	DestroySnapshot(context.Context, string) error
}

// Executor is the typed boundary for every ZFS mutation and query. A future
// privileged backend must implement these operations without accepting raw
// commands or flags from its client.
type Executor interface {
	Destination
	GetActivationProperties(context.Context) ([]Property, error)
	GetStoredProperties(context.Context, []string) ([]Property, error)
	// Snapshot creates one snapshot name across an explicit set of datasets in
	// a single atomic transaction group. The set may be sparse, which `-r` cannot
	// express, so there is no recursive variant.
	Snapshot(context.Context, []string, string, map[string]string) error
	Bookmark(context.Context, string, string) error
	DestroyBookmark(context.Context, string) error
	Hold(context.Context, string, string) error
	Release(context.Context, string, string) error
}

// ReseedExecutor exposes the destructive destination operations used only by
// the explicit, preview-first reseed workflow.
type ReseedExecutor interface {
	Destination
	AbortReceive(context.Context, string) error
	DestroyDataset(context.Context, string, bool) error
}

// PlaceholderProperty marks a dataset boomerangz created only so that a deeper
// replica could be received beneath it. Every receive parent carries it from
// the moment it is created, so the marker is proof the dataset was never a
// replica of anything.
const PlaceholderProperty = propertyNamespace + "state:placeholder"

// ReplicaExecutor exposes the destination operations that replace a
// placeholder with the replica it stood in for: moving the placeholder aside,
// moving its children beneath the received replica, and removing it.
type ReplicaExecutor interface {
	Destination
	RenameDataset(context.Context, string, string) error
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
