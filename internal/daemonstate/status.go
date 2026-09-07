// Package daemonstate defines detached operational values shared by the daemon
// and its control API without coupling either implementation to the other.
package daemonstate

import "time"

// Event is a structured worker-state transition used by logs and live status.
type Event struct {
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
	Revision   uint64
	Observed   time.Time
	Generation uint64
	Datasets   []DatasetStatus
	Queues     map[string]QueueSnapshot
	Jobs       []Event
}
