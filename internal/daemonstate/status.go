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
	Send uint64 `json:"-"`
	// RunID is shared by every transition of one run of a job: its pending
	// state, start state, phases, and outcome. A configuration reload, which
	// is never queued, is a run of its own. It is unique within one daemon
	// process and restarts with it.
	RunID          uint64        `json:"run_id,omitempty"`
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
	// RetryAt is when the daemon next tries a transfer again, on a
	// waiting-retry transition; Attempts is how many attempts in a row have
	// failed, where the transfer counts them.
	RetryAt  time.Time `json:"retry_at,omitzero"`
	Attempts int       `json:"attempts,omitempty"`
	Identity
}

// DestroyedLimit is the most names an Identity carries in Destroyed.
// DestroyedCount always carries the total, so a reader knows when the list is
// incomplete.
const DestroyedLimit = 64

// Identity names what one run of a job acted on. Each field is set only on
// the transitions it describes, and is empty on every other.
type Identity struct {
	// Snapshot is, on a snapshot job's succeeded, the snapshot it created,
	// and on its scheduled, the owned snapshot whose age set the deadline. On
	// a transfer's sending it is the source snapshot sent, on its succeeded
	// the snapshot now on the destination, and on the waiting-retry,
	// blocked, or cancelled that ends a run the pending snapshot the run was
	// carrying, or, for a run that ended before taking one, the one pending
	// when it ended. A transfer cancelled while still queued names none.
	Snapshot string `json:"snapshot,omitempty"`
	// Base is, on a transfer's sending, the snapshot or bookmark an
	// incremental stream is based on.
	Base string `json:"base,omitempty"`
	// Mode is, on a transfer's sending, the plan's mode: full,
	// incremental-latest, incremental-all, or resume.
	Mode string `json:"mode,omitempty"`
	// Destination is, on a transfer's succeeded, the destination dataset.
	Destination string `json:"destination,omitempty"`
	// Marker is, on an inactive reconciliation's succeeded, the action it
	// applied to the inactive marker: set, cleared, or none.
	Marker string `json:"marker,omitempty"`
	// Destroyed names, on a prune's or a retirement's succeeded, the
	// snapshots it destroyed, at most DestroyedLimit of them.
	Destroyed []string `json:"destroyed,omitempty"`
	// DestroyedCount is how many snapshots Destroyed describes, including any
	// past DestroyedLimit.
	DestroyedCount int `json:"destroyed_count,omitempty"`
	// ConfigGeneration is, on a configuration reload's succeeded, the
	// generation it published.
	ConfigGeneration uint64 `json:"config_generation,omitempty"`
}

// DestroyedIdentity describes destroyed snapshots, keeping at most
// DestroyedLimit names and the full count.
func DestroyedIdentity(names []string) Identity {
	kept := names
	if len(kept) > DestroyedLimit {
		kept = kept[:DestroyedLimit]
	}
	return Identity{Destroyed: append([]string(nil), kept...), DestroyedCount: len(names)}
}

// QueueSnapshot is an observable bounded-queue view.
type QueueSnapshot struct {
	Capacity int      `json:"capacity"`
	Pending  int      `json:"pending"`
	IDs      []string `json:"ids,omitempty"`
}

// DatasetStatus is the detached daemon view used by the control API. Status
// is the dataset's management status as discovery classifies it; Active is
// set on a root the daemon schedules.
type DatasetStatus struct {
	Name         string
	Status       string
	Active       bool
	Recursive    bool
	NextSnapshot time.Time
	// CoveredBy is the actor root a member or excluded dataset lies under.
	CoveredBy string
	// Blocked lists the targets an actor root may not send to.
	Blocked []BlockedTarget
}

// BlockedTarget is a local destination root or, with Remote, a remote's name
// that an actor root may not send to, and why.
type BlockedTarget struct {
	Target string
	Remote bool
	Reason string
}

// DestinationPermissions is the latest check of one destination's delegated
// ZFS permissions against everything boomerangz may need there. Destination is
// a local destination root or, with Remote, a remote's name. Missing names the
// permissions Account lacks on Dataset, the destination root or its nearest
// existing ancestor; Error says why the check itself could not run. Either is
// a warning, never a failure: transfers go on, and each operation's own
// preflight still stops the one that needs a missing permission.
type DestinationPermissions struct {
	Destination string    `json:"destination"`
	Remote      bool      `json:"remote,omitempty"`
	Dataset     string    `json:"dataset,omitempty"`
	Account     string    `json:"account,omitempty"`
	Missing     []string  `json:"missing,omitempty"`
	Error       string    `json:"error,omitempty"`
	Checked     time.Time `json:"checked"`
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
	// Destinations holds the latest permission check of each destination
	// checked so far, sorted local first and then by name.
	Destinations []DestinationPermissions
	// Receives lists the receives into this host, sorted by dataset.
	Receives []Receive
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

// Receive states.
const (
	// ReceiveReceiving is a receive in progress.
	ReceiveReceiving = "receiving"
	// ReceiveInterrupted is a receive that stopped and left a resume token on
	// its destination, for its sender to resume.
	ReceiveInterrupted = "interrupted"
)

// Receive is one receive into this host. Dataset is the receive's destination
// root: the dataset itself for an exact receive, its parent for one that
// derives the name from the stream. Sender is the sending host as it names
// itself, and is display text only. Transport says which endpoint serves the
// receive: native, ssh-shell, or empty for one found only by its traces in
// ZFS, which has no figures. TotalBytes is the sender's estimate when
// TotalKnown is set.
type Receive struct {
	Dataset        string        `json:"dataset"`
	State          string        `json:"state"`
	Sender         string        `json:"sender_host,omitempty"`
	Transport      string        `json:"transport,omitempty"`
	Started        time.Time     `json:"started,omitzero"`
	Bytes          uint64        `json:"bytes,omitempty"`
	TotalBytes     uint64        `json:"total_bytes,omitempty"`
	TotalKnown     bool          `json:"total_known,omitempty"`
	BytesPerSecond float64       `json:"bytes_per_second,omitempty"`
	ETA            time.Duration `json:"eta,omitempty"`
}

// ReceiveTracker is told about each receive an endpoint serves, so that the
// receiving host can show it. BeginReceive is called as a receive starts,
// with its counters zero; the returned reporter takes its progress, and End
// once it has ended, however it ended. Neither may block for long: they run
// beside the stream.
type ReceiveTracker interface {
	BeginReceive(Receive) ReceiveReporter
}

// ReceiveReporter takes one receive's progress and its end. Progress is
// called serially and never after End.
type ReceiveReporter interface {
	Progress(bytes uint64, bytesPerSecond float64)
	End()
}
