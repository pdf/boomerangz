// Package policy parses snapshot grids and effective dataset configuration.
package policy

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultGrid defines the default cadence and retention windows.
const DefaultGrid = "12x5m,24x1h,14x1d"

// Bucket describes Count adjacent retention windows of equal duration.
type Bucket struct {
	Count int           `json:"count"`
	Span  time.Duration `json:"span"`
}

// Grid is an immutable, validated retention grid.
type Grid struct {
	buckets []Bucket
	text    string
	horizon time.Duration
}

var bucketPattern = regexp.MustCompile(`^([0-9]+)x([0-9]+)(mo|[mhdwy])$`)

// Grid units are fixed elapsed durations, never calendar arithmetic. The same
// ordered table defines parsing and canonical formatting.
var gridUnits = []struct {
	name string
	span time.Duration
}{
	{"y", 365 * 24 * time.Hour},
	{"mo", 30 * 24 * time.Hour},
	{"w", 7 * 24 * time.Hour},
	{"d", 24 * time.Hour},
	{"h", time.Hour},
	{"m", time.Minute},
}

// ParseGrid validates and sorts tiers by duration, merging equal durations,
// without expanding individual windows. Written order does not affect retention.
func ParseGrid(value string) (Grid, error) {
	var grid Grid
	var canonical []string
	for _, part := range strings.Split(value, ",") {
		match := bucketPattern.FindStringSubmatch(strings.TrimSpace(part))
		if match == nil {
			return Grid{}, fmt.Errorf("invalid grid bucket %q", part)
		}
		count, err := strconv.ParseInt(match[1], 10, 32)
		if err != nil || count <= 0 {
			return Grid{}, fmt.Errorf("invalid grid count %q", match[1])
		}
		amount, err := strconv.ParseInt(match[2], 10, 64)
		var unit time.Duration
		for _, candidate := range gridUnits {
			if candidate.name == match[3] {
				unit = candidate.span
				break
			}
		}
		if err != nil || amount <= 0 || amount > math.MaxInt64/int64(unit) {
			return Grid{}, fmt.Errorf("invalid grid duration %q", match[2]+match[3])
		}
		span := time.Duration(amount) * unit
		if count > int64((math.MaxInt64-grid.horizon)/span) {
			return Grid{}, fmt.Errorf("grid horizon overflows time.Duration")
		}
		grid.horizon += time.Duration(count) * span
		grid.buckets = append(grid.buckets, Bucket{Count: int(count), Span: span})
	}
	sort.Slice(grid.buckets, func(i, j int) bool { return grid.buckets[i].Span < grid.buckets[j].Span })
	merged := grid.buckets[:0]
	for _, bucket := range grid.buckets {
		if len(merged) > 0 && merged[len(merged)-1].Span == bucket.Span {
			merged[len(merged)-1].Count += bucket.Count
		} else {
			merged = append(merged, bucket)
		}
	}
	grid.buckets = merged
	for _, bucket := range grid.buckets {
		for _, unit := range gridUnits {
			if bucket.Span%unit.span == 0 {
				canonical = append(canonical, fmt.Sprintf("%dx%d%s", bucket.Count, bucket.Span/unit.span, unit.name))
				break
			}
		}
	}
	grid.text = strings.Join(canonical, ",")
	return grid, nil
}

// Buckets returns a copy of the grid's bucket groups.
func (g Grid) Buckets() []Bucket { return append([]Bucket(nil), g.buckets...) }

// Cadence returns the smallest window, or zero for an uninitialized grid.
func (g Grid) Cadence() time.Duration {
	if len(g.buckets) == 0 {
		return 0
	}
	return g.buckets[0].Span
}

// Horizon returns the complete retention interval.
func (g Grid) Horizon() time.Duration { return g.horizon }

func (g Grid) String() string { return g.text }

// MarshalText exposes the canonical policy in inspection output.
func (g Grid) MarshalText() ([]byte, error) { return []byte(g.text), nil }
