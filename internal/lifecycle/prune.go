package lifecycle

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Decision is a deterministic pruning preview, never an authorization to delete
// without rechecking ownership, GUID, holds, clones, and resume dependencies.
type Decision struct {
	Snapshot string `json:"snapshot"`
	GUID     uint64 `json:"guid"`
	Destroy  bool   `json:"destroy"`
	Reason   string `json:"reason"`
}

// PlanPrune applies a grid to one dataset's proven owned snapshots. It does not
// expand large grids into individual windows or mutate input data.
func PlanPrune(dataset, lineage string, grid policy.Grid, snapshots []Snapshot) ([]Decision, error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return nil, err
	}
	if !ValidID(lineage) || grid.Cadence() <= 0 {
		return nil, fmt.Errorf("valid lineage and grid are required")
	}
	type candidate struct {
		snapshot Snapshot
		created  time.Time
		owned    bool
	}
	items := make([]candidate, 0, len(snapshots))
	seen := make(map[string]bool)
	var anchor time.Time
	for _, snapshot := range snapshots {
		if !strings.HasPrefix(snapshot.Name, dataset+"@") || seen[snapshot.Name] {
			return nil, fmt.Errorf("duplicate or out-of-scope snapshot %q", snapshot.Name)
		}
		seen[snapshot.Name] = true
		metadata, err := Ownership(snapshot, lineage)
		item := candidate{snapshot: snapshot, created: metadata.Created, owned: err == nil}
		if item.owned && item.created.After(anchor) {
			anchor = item.created
		}
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b candidate) int {
		if order := a.created.Compare(b.created); order != 0 {
			return order
		}
		return strings.Compare(a.snapshot.Name, b.snapshot.Name)
	})
	type window struct {
		group int
		index int64
	}
	winners := make(map[window]bool)
	buckets := grid.Buckets()
	var decisions []Decision
	for _, item := range items {
		d := Decision{Snapshot: item.snapshot.Name, GUID: item.snapshot.GUID, Reason: "foreign or unproven ownership"}
		if item.owned {
			age := anchor.Sub(item.created)
			offset := time.Duration(0)
			d.Destroy, d.Reason = true, "older than retention grid"
			for group, bucket := range buckets {
				end := offset + time.Duration(bucket.Count)*bucket.Span
				if age >= offset && age < end {
					key := window{group: group, index: int64((age - offset) / bucket.Span)}
					if !winners[key] {
						winners[key] = true
						d.Destroy, d.Reason = false, "oldest snapshot in retention window"
					} else {
						d.Reason = "newer duplicate in retention window"
					}
					break
				}
				offset = end
			}
			if len(item.snapshot.Holds) > 0 {
				d.Destroy, d.Reason = false, "snapshot has holds"
			}
			if len(item.snapshot.Clones) > 0 {
				d.Destroy, d.Reason = false, "snapshot has clones"
			}
			if item.snapshot.ResumeRequired {
				d.Destroy, d.Reason = false, "snapshot is required by resumable receive"
			}
		}
		decisions = append(decisions, d)
	}
	slices.SortFunc(decisions, func(a, b Decision) int { return strings.Compare(a.Snapshot, b.Snapshot) })
	return decisions, nil
}

// SnapshotDue uses the newest owned snapshot and never backfills missed ticks.
// A snapshot exactly one cadence old is not yet older than the cadence.
func SnapshotDue(now time.Time, dataset, lineage string, grid policy.Grid, snapshots []Snapshot) (bool, error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return false, err
	}
	if !ValidID(lineage) || grid.Cadence() <= 0 || now.IsZero() {
		return false, fmt.Errorf("valid time, lineage, and grid are required")
	}
	var newest time.Time
	for _, snapshot := range snapshots {
		if !strings.HasPrefix(snapshot.Name, dataset+"@") {
			return false, fmt.Errorf("out-of-scope snapshot %q", snapshot.Name)
		}
		metadata, err := Ownership(snapshot, lineage)
		if err == nil && metadata.Created.After(newest) {
			newest = metadata.Created
		}
	}
	return newest.IsZero() || now.Sub(newest) > grid.Cadence(), nil
}
