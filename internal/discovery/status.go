package discovery

import "slices"

// The management statuses a dataset can have. Every command that lists
// datasets classifies them with Entry.Status and filters them with
// StatusFilter, so a dataset reads the same wherever it appears.
const (
	StatusActive   = "active"
	StatusMember   = "member"
	StatusExcluded = "excluded"
	StatusTarget   = "target"
	StatusInactive = "inactive"
	StatusInvalid  = "invalid"
)

// Statuses lists every status in the order a listing explains them.
var Statuses = []string{StatusActive, StatusMember, StatusExcluded, StatusTarget, StatusInactive, StatusInvalid}

// Status classifies the entry: invalid when its policy has errors; member or
// excluded when an actor root covers it; active when it is an enabled root;
// target when it is a replica boomerangz receives into; inactive otherwise.
func (e Entry) Status() string {
	if len(e.Policy.Errors) > 0 {
		return StatusInvalid
	}
	if e.CoveredBy != "" && e.Member {
		return StatusMember
	}
	if e.CoveredBy != "" {
		return StatusExcluded
	}
	if e.Inspected && e.Policy.Enabled {
		return StatusActive
	}
	if e.Replica != (Replica{}) {
		return StatusTarget
	}
	return StatusInactive
}

// StatusFilter returns whether a dataset with a given status is listed when
// the operator asked for statuses, "all" listing every one. With none asked
// for it lists every status but inactive, and defaulted is true so the
// listing can say how many it hid and how to see them.
func StatusFilter(statuses []string) (keep func(status string) bool, defaulted bool) {
	switch {
	case slices.Contains(statuses, "all"):
		return func(string) bool { return true }, false
	case len(statuses) == 0:
		return func(status string) bool { return status != StatusInactive }, true
	default:
		return func(status string) bool { return slices.Contains(statuses, status) }, false
	}
}
