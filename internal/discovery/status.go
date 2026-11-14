package discovery

import (
	"slices"
	"strings"
)

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

// Listed is what orders a dataset in a listing.
type Listed struct {
	Name      string
	Status    string
	CoveredBy string
}

// Listed returns what orders the entry in a listing.
func (e Entry) Listed() Listed {
	return Listed{Name: e.Dataset.Name, Status: e.Status(), CoveredBy: e.CoveredBy}
}

// listedSections orders a listing by what a dataset is to this host: what it
// sends first, then what should send and cannot, then what it receives, then
// what it ignores.
var listedSections = map[string]int{
	StatusActive:   0,
	StatusMember:   0,
	StatusExcluded: 0,
	StatusInvalid:  1,
	StatusTarget:   2,
	StatusInactive: 3,
}

// CompareListed orders datasets in a listing: each actor root with the
// members and excluded datasets it covers straight beneath it, roots by
// name, then datasets with invalid policy, targets and inactive datasets,
// each by name. Every terminal listing of datasets uses it, so they read in
// one order.
func CompareListed(a, b Listed) int {
	group := func(dataset Listed) (section int, root string) {
		section, known := listedSections[dataset.Status]
		if !known {
			section = len(listedSections)
		}
		if section == 0 && dataset.CoveredBy != "" {
			return section, dataset.CoveredBy
		}
		return section, dataset.Name
	}
	sectionA, rootA := group(a)
	sectionB, rootB := group(b)
	if sectionA != sectionB {
		return sectionA - sectionB
	}
	if rootA != rootB {
		return strings.Compare(rootA, rootB)
	}
	// The root itself leads its group.
	if leadA, leadB := a.Name == rootA, b.Name == rootB; leadA != leadB {
		if leadA {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Name, b.Name)
}
