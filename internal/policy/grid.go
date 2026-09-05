// Package policy parses snapshot grids and effective dataset configuration.
package policy

import (
	"fmt"
	"math"
	"regexp"
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

var bucketPattern = regexp.MustCompile(`^([0-9]+)x([0-9]+)([mhdw])$`)

// ParseGrid validates the grammar, ordering, and total duration without expanding windows.
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
		unit := map[string]time.Duration{"m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}[match[3]]
		if err != nil || amount <= 0 || amount > math.MaxInt64/int64(unit) {
			return Grid{}, fmt.Errorf("invalid grid duration %q", match[2]+match[3])
		}
		span := time.Duration(amount) * unit
		if len(grid.buckets) > 0 && span <= grid.buckets[len(grid.buckets)-1].Span {
			return Grid{}, fmt.Errorf("grid durations must be strictly increasing")
		}
		if count > int64((math.MaxInt64-grid.horizon)/span) {
			return Grid{}, fmt.Errorf("grid horizon overflows time.Duration")
		}
		grid.horizon += time.Duration(count) * span
		grid.buckets = append(grid.buckets, Bucket{Count: int(count), Span: span})
		canonical = append(canonical, fmt.Sprintf("%dx%d%s", count, amount, match[3]))
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
