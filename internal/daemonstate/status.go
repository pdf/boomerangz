// Package daemonstate defines detached operational values shared by the daemon
// and its control API without coupling either implementation to the other.
package daemonstate

import "time"

// EventKind says how an Event is delivered. The zero value is invalid, so an
// Event built without a kind is rejected rather than guessed at.
type EventKind uint8

const (
	// EventTransition is a change of job state: queued, lossless, in order.
	EventTransition EventKind = iota + 1
	// EventProgress is a transfer progress sample, conflated to the newest
	// sample per job. It never changes a job's state.
	EventProgress
)

// Event is a job-state transition or a transfer progress sample; Kind says
// which.
type Event struct {
	Kind EventKind `json:"-"`
	// Send identifies the sending transition a progress sample was taken
	// under. It is set on a sending transition and on each of its samples.
	Send           uint64        `json:"-"`
	Pool           string        `json:"pool"`
	Job            string        `json:"job"`
	Scope          string        `json:"scope"`
	Target         string        `json:"target,omitempty"`
	State          string        `json:"state"`
	Reason         string        `json:"reason,omitempty"`
	At             time.Time     `json:"at"`
	Pending        int           `json:"pending"`
	Position       int           `json:"queue_position,omitempty"`
	Bytes          uint64        `json:"bytes,omitempty"`
	TotalBytes     uint64        `json:"total_bytes,omitempty"`
	BytesPerSecond float64       `json:"bytes_per_second,omitempty"`
	ETA            time.Duration `json:"eta,omitempty"`
	TotalKnown     bool          `json:"total_known,omitempty"`
}

// QueueSnapshot is an observable bounded-queue view.
type QueueSnapshot struct {
	Capacity int      `json:"capacity"`
	Pending  int      `json:"pending"`
	IDs      []string `json:"ids,omitempty"`
}

// DatasetStatus is the detached daemon view used by the control API.
type DatasetStatus struct {
	Name         string
	Active       bool
	Recursive    bool
	NextSnapshot time.Time
}

// ControlSnapshot is one coherent-enough operational view.
type ControlSnapshot struct {
	Revision         uint64
	Observed         time.Time
	Generation       uint64
	ConfigGeneration uint64
	Datasets         []DatasetStatus
	Queues           map[string]QueueSnapshot
	Jobs             []Event
}

// Update is one delivery to a status subscriber: every transition since the
// previous Update, in order, and the state as of the last message merged into
// it. The first Update carries the state at registration and no transitions.
type Update struct {
	Transitions []Event
	State       ControlSnapshot
}

// Subscription is a live status subscription. Updates is closed when the
// subscription ends, and Err then reports why.
type Subscription interface {
	Updates() <-chan Update
	Err() error
}

// ReloadResult describes one successfully validated configuration generation.
// RestartRequired contains settings retained from the previous generation.
type ReloadResult struct {
	Generation      uint64
	Applied         []string
	RestartRequired []string
}
