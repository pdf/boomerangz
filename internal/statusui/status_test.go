package statusui

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
)

var update = flag.Bool("update", false, "rewrite the golden frames under testdata/ from the current output")

// observed is when every fixture snapshot was taken; times in the frames are
// relative to it.
var observed = time.Date(2026, 11, 13, 14, 2, 11, 0, time.UTC)

var zone = time.FixedZone("CET", 60*60)

func at(offset time.Duration) int64 { return observed.Add(offset).UnixNano() }

// busy is a host with something in every section: a destination whose
// permission check failed, an invalid dataset, a blocked target, a failed and
// a retrying transfer, one running and one queued, finished work, member and
// inactive datasets, and a nearly full queue.
func busy() *controlrpc.StatusSnapshot {
	return &controlrpc.StatusSnapshot{
		Revision: 7, Generation: 41, ConfigGeneration: 3, ObservedUnixNano: observed.UnixNano(),
		Destinations: []*controlrpc.DestinationPermissions{
			{Destination: "offsite", Remote: true, Error: "list destination datasets: dial tcp 10.0.0.9:22: connect: connection refused"},
			{Destination: "backup/boomerangz", Dataset: "backup/boomerangz", Account: "boomerangz"},
		},
		Datasets: []*controlrpc.DatasetStatus{
			{Name: "tank/home", Status: "active", Active: true, Recursive: true, NextSnapshotUnixNano: at(13 * time.Minute)},
			{Name: "tank/home/pdf", Status: "member", CoveredBy: "tank/home", NextSnapshotUnixNano: at(13 * time.Minute)},
			{Name: "tank/home/cache", Status: "excluded", CoveredBy: "tank/home"},
			{Name: "tank/vm/images", Status: "active", Active: true, NextSnapshotUnixNano: at(58 * time.Minute)},
			{Name: "tank/db", Status: "active", Active: true, NextSnapshotUnixNano: at(-5 * time.Second), Blocked: []*controlrpc.BlockedTarget{{Target: "backup/boomerangz", Reason: "its replica would overlap the replica of tank/vm"}}},
			{Name: "tank/broken", Status: "invalid"},
			{Name: "tank/scratch", Status: "inactive"},
			{Name: "tank/tmp", Status: "inactive"},
			{Name: "backup/boomerangz/tank/home", Status: "target"},
		},
		Queues: []*controlrpc.QueueStatus{{Name: "management", Capacity: 64, Pending: 1}, {Name: "local", Capacity: 8, Pending: 7}, {Name: "remote", Capacity: 64}},
		Jobs: []*controlrpc.JobStatus{
			{Job: "snapshot:tank/home", Dataset: "tank/home", Target: "tank/home", State: "succeeded", ChangedUnixNano: at(-47 * time.Minute)},
			{Job: "snapshot:tank/vm/images", Dataset: "tank/vm/images", Target: "tank/vm/images", State: "snapshotting", ChangedUnixNano: at(-2 * time.Second)},
			{Job: "prune:tank/home", Dataset: "tank/home", Target: "tank/home", State: "succeeded", ChangedUnixNano: at(-46 * time.Minute)},
			{Job: "local:tank/home:backup/boomerangz", Dataset: "tank/home", Target: "local:backup/boomerangz", State: "succeeded", ChangedUnixNano: at(-12 * time.Minute)},
			{Job: "local:tank/vm/images:backup/boomerangz", Dataset: "tank/vm/images", Target: "local:backup/boomerangz", State: "sending", ChangedUnixNano: at(-90 * time.Second), Bytes: 41 << 30, TotalBytes: 120 << 30, TotalKnown: true, BytesPerSecond: 410 << 20, EtaNanoseconds: int64(197 * time.Second)},
			{Job: "remote:tank/home:offsite", Dataset: "tank/home", Target: "ssh:offsite.example:22", State: "waiting-retry", ChangedUnixNano: at(-30 * time.Second), RetryUnixNano: at(4 * time.Minute), Attempts: 3, Reason: "ssh: connect to host offsite.example port 22: Connection refused"},
			{Job: "remote:tank/vm/images:offsite", Dataset: "tank/vm/images", Target: "ssh:offsite.example:22", State: "pending-remote", QueuePosition: 1, Pending: 1},
			{Job: "local:tank/db:archive/boomerangz", Dataset: "tank/db", Target: "local:archive/boomerangz", State: "failed", ChangedUnixNano: at(-3 * time.Hour), Reason: "cannot receive incremental stream: destination archive/boomerangz/tank/db has been modified since most recent snapshot"},
			{Job: "destination-prune:tank/home:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", Dataset: "tank/home", Target: "local:backup/boomerangz", State: "pruning", ChangedUnixNano: at(-1 * time.Second)},
			{Job: "inactive:tank/scratch:false", Dataset: "tank/scratch", Target: "tank/scratch", State: "succeeded", ChangedUnixNano: at(-2 * time.Hour)},
			{Job: "retire:tank/old", Dataset: "tank/old", Target: "tank/old", State: "cancelled", Reason: "context canceled"},
		},
	}
}

// idle is a healthy host with nothing running.
func idle() *controlrpc.StatusSnapshot {
	return &controlrpc.StatusSnapshot{
		ObservedUnixNano: observed.UnixNano(),
		Datasets: []*controlrpc.DatasetStatus{
			{Name: "tank/home", Status: "active", Active: true, NextSnapshotUnixNano: at(13 * time.Minute)},
			{Name: "tank/scratch", Status: "inactive"},
		},
		Queues: []*controlrpc.QueueStatus{{Name: "local", Capacity: 64}},
		Jobs: []*controlrpc.JobStatus{
			{Job: "snapshot:tank/home", Dataset: "tank/home", Target: "tank/home", State: "succeeded", ChangedUnixNano: at(-47 * time.Minute)},
			{Job: "local:tank/home:backup/boomerangz", Dataset: "tank/home", Target: "local:backup/boomerangz", State: "succeeded", ChangedUnixNano: at(-46 * time.Minute)},
		},
	}
}

// receivingHost is a backup host that other hosts send to: one receive the
// daemon serves with the sender's estimate, one an ssh-shell process reports
// without one into a dataset discovery has not listed yet, an interrupted
// receive found in ZFS, and a sending job of its own beside them.
func receivingHost() *controlrpc.StatusSnapshot {
	return &controlrpc.StatusSnapshot{
		ObservedUnixNano: observed.UnixNano(),
		Datasets: []*controlrpc.DatasetStatus{
			{Name: "tank/home", Status: "active", Active: true, NextSnapshotUnixNano: at(13 * time.Minute)},
			{Name: "backup/boomerangz/host-a/tank/home", Status: "target"},
			{Name: "backup/boomerangz/host-a/tank/db", Status: "target"},
			{Name: "backup/boomerangz/host-b/tank/media", Status: "target"},
		},
		Jobs: []*controlrpc.JobStatus{
			{Job: "remote:tank/home:offsite", Dataset: "tank/home", Target: "native:offsite.example:8443/backup", State: "sending", ChangedUnixNano: at(-20 * time.Second), Bytes: 3 << 30, TotalBytes: 12 << 30, TotalKnown: true, BytesPerSecond: 95 << 20, EtaNanoseconds: int64(97 * time.Second)},
		},
		Receives: []*controlrpc.ReceiveStatus{
			{Dataset: "backup/boomerangz/host-a/tank/home", State: "receiving", SenderHost: "host-a.example", Transport: "native", StartedUnixNano: at(-4 * time.Minute), Bytes: 41 << 30, TotalBytes: 120 << 30, TotalKnown: true, BytesPerSecond: 410 << 20, EtaNanoseconds: int64(197 * time.Second)},
			{Dataset: "backup/boomerangz/host-b/tank/vm", State: "receiving", SenderHost: "host-b", Transport: "ssh-shell", StartedUnixNano: at(-75 * time.Second), Bytes: 5 << 30, BytesPerSecond: 60 << 20},
			{Dataset: "backup/boomerangz/host-a/tank/db", State: "interrupted"},
		},
	}
}

// onlyInactive is a host where nothing is managed yet.
func onlyInactive() *controlrpc.StatusSnapshot {
	return &controlrpc.StatusSnapshot{ObservedUnixNano: observed.UnixNano(), Datasets: []*controlrpc.DatasetStatus{{Name: "tank", Status: "inactive"}, {Name: "tank/data", Status: "inactive"}}}
}

// checkGolden compares got with testdata/name, or rewrites the file under
// -update. A frame is compared whole, so a layout change shows as a diff of
// the screen rather than as a missing substring.
func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test ./internal/statusui -update to create it)", err)
	}
	if got != string(want) {
		t.Errorf("frame differs from %s (run go test ./internal/statusui -update and review the diff):\n--- got\n%s--- want\n%s", path, got, want)
	}
}

func TestTerminalFrames(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		golden   string
		snapshot *controlrpc.StatusSnapshot
		opts     Options
	}{
		{golden: "busy-120.txt", snapshot: busy(), opts: Options{Width: 120}},
		{golden: "busy-80.txt", snapshot: busy(), opts: Options{Width: 80}},
		{golden: "busy-40.txt", snapshot: busy(), opts: Options{Width: 40}},
		{golden: "busy-color.txt", snapshot: busy(), opts: Options{Width: 120, Color: true}},
		{golden: "busy-all.txt", snapshot: busy(), opts: Options{Width: 120, Statuses: []string{"all"}}},
		{golden: "busy-member-target.txt", snapshot: busy(), opts: Options{Width: 120, Statuses: []string{"member", "target"}}},
		{golden: "idle.txt", snapshot: idle(), opts: Options{Width: 80}},
		{golden: "receiving-120.txt", snapshot: receivingHost(), opts: Options{Width: 120}},
		{golden: "receiving-40.txt", snapshot: receivingHost(), opts: Options{Width: 40}},
		{golden: "receiving-active.txt", snapshot: receivingHost(), opts: Options{Width: 80, Statuses: []string{"active"}}},
		{golden: "only-inactive.txt", snapshot: onlyInactive(), opts: Options{Width: 80}},
		{golden: "empty.txt", snapshot: &controlrpc.StatusSnapshot{ObservedUnixNano: observed.UnixNano()}, opts: Options{Width: 80}},
	} {
		t.Run(test.golden, func(t *testing.T) {
			t.Parallel()
			test.opts.Zone = zone
			var frame bytes.Buffer
			if err := Terminal(&frame, test.snapshot, test.opts); err != nil {
				t.Fatal(err)
			}
			checkGolden(t, test.golden, frame.String())
		})
	}
}

func TestTailFrame(t *testing.T) {
	t.Parallel()
	tail := NewTail(5)
	tail.Add([]*controlrpc.JobStatus{
		{Job: "retire:tank/old", Dataset: "tank/old", State: "succeeded", ChangedUnixNano: observed.Add(-30 * time.Hour).UnixNano()},
		{Job: "snapshot:tank/home", Dataset: "tank/home", State: "succeeded", ChangedUnixNano: observed.Add(-20 * time.Hour).UnixNano(), Snapshot: "tank/home@boomerangz_2026-11-12T18:00:00Z"},
		{Job: "local:tank/vm/images:backup/boomerangz", Dataset: "tank/vm/images", State: "sending", ChangedUnixNano: at(-90 * time.Second), Snapshot: "tank/vm/images@boomerangz_2026-11-13T13:00:00Z", Base: "tank/vm/images@boomerangz_2026-11-13T12:00:00Z", Mode: "incremental-latest"},
	})
	tail.Add(nil)
	tail.Add([]*controlrpc.JobStatus{
		{Job: "prune:tank/home", Dataset: "tank/home", State: "succeeded", ChangedUnixNano: at(-60 * time.Second), Destroyed: []string{"tank/home@old"}, DestroyedCount: 1},
		{Job: "remote:tank/home:offsite", Dataset: "tank/home", State: "waiting-retry", ChangedUnixNano: at(-30 * time.Second), Snapshot: "tank/home@boomerangz_2026-11-13T13:15:00Z", Reason: "ssh: connect to host offsite.example port 22: Connection refused"},
		{Job: "config:reload", State: "succeeded", ChangedUnixNano: at(-10 * time.Second), ConfigGeneration: 3},
	})
	var frame bytes.Buffer
	if err := tail.Terminal(&frame, observed, Options{Width: 80, Zone: zone}); err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "tail-80.txt", frame.String())

	var empty bytes.Buffer
	if err := NewTail(4).Terminal(&empty, observed, Options{}); err != nil || empty.Len() != 0 {
		t.Fatalf("an empty tail wrote %q err=%v", empty.String(), err)
	}
}

// busyTail holds transitions with long identities and reasons.
func busyTail() *Tail {
	tail := NewTail(10)
	tail.Add([]*controlrpc.JobStatus{
		{Job: "local:tank/vm/images:backup/boomerangz", Dataset: "tank/vm/images", State: "sending", ChangedUnixNano: at(-90 * time.Second), Snapshot: "tank/vm/images@boomerangz_2026-11-13T13:00:00Z", Base: "tank/vm/images@boomerangz_2026-11-13T12:00:00Z", Mode: "incremental-latest"},
		{Job: "remote:tank/home:offsite", Dataset: "tank/home", State: "waiting-retry", ChangedUnixNano: at(-30 * time.Second), Snapshot: "tank/home@boomerangz_2026-11-13T13:15:00Z", Reason: "ssh: connect to host offsite.example port 22: Connection refused"},
	})
	return tail
}

// TestFramesFitTheWidthWithoutLosingText renders the busy host and a watch
// tail at every width from very narrow to wide, and checks two things: no
// line is wider than the terminal, which would wrap again on screen and
// throw a redrawn frame out of place, and every word of every reason and
// detail is still on screen, since long text wraps rather than being cut.
func TestFramesFitTheWidthWithoutLosingText(t *testing.T) {
	t.Parallel()
	snapshot := busy()
	var words []string
	for _, job := range snapshot.GetJobs() {
		if job.GetState() != "cancelled" {
			words = append(words, strings.Fields(job.GetReason())...)
		}
	}
	for _, destination := range snapshot.GetDestinations() {
		words = append(words, strings.Fields(destination.GetError())...)
	}
	for _, dataset := range snapshot.GetDatasets() {
		for _, blocked := range dataset.GetBlocked() {
			words = append(words, strings.Fields(blocked.GetReason())...)
		}
	}
	for width := 20; width <= 160; width++ {
		var frame bytes.Buffer
		if err := Terminal(&frame, snapshot, Options{Width: width, Zone: zone}); err != nil {
			t.Fatal(err)
		}
		if err := busyTail().Terminal(&frame, observed, Options{Width: width, Zone: zone}); err != nil {
			t.Fatal(err)
		}
		if err := Terminal(&frame, receivingHost(), Options{Width: width, Zone: zone}); err != nil {
			t.Fatal(err)
		}
		for line := range strings.Lines(frame.String()) {
			if len(strings.TrimSuffix(line, "\n")) > width {
				t.Fatalf("width %d: line %q is %d wide:\n%s", width, line, len(line)-1, frame.String())
			}
		}
		// A word broken across lines is whole again once the frame's
		// line breaks and indentation are taken out.
		joined := strings.Join(strings.Fields(frame.String()), "")
		for _, word := range words {
			if !strings.Contains(joined, word) {
				t.Fatalf("width %d lost %q:\n%s", width, word, frame.String())
			}
		}
	}
}

func TestFitKeepsAFrameWithinTheTerminalHeight(t *testing.T) {
	t.Parallel()
	frame := []byte("one\ntwo\nthree\nfour\n")
	for _, test := range []struct {
		height int
		want   string
	}{
		{height: 0, want: "one\ntwo\nthree\nfour\n"},
		{height: 1, want: "one\ntwo\nthree\nfour\n"},
		{height: 4, want: "one\ntwo\nthree\nfour\n"},
		{height: 3, want: "one\ntwo\n... 2 more lines\n"},
		{height: 2, want: "one\n... 3 more lines\n"},
	} {
		if got := string(Fit(frame, test.height)); got != test.want {
			t.Errorf("Fit(height %d) = %q, want %q", test.height, got, test.want)
		}
	}
}

func TestJSONFiltersDatasetsAsDatasetListDoes(t *testing.T) {
	t.Parallel()
	names := func(t *testing.T, statuses []string) []string {
		t.Helper()
		var structured bytes.Buffer
		if err := JSON(&structured, busy(), statuses); err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Datasets []struct {
				Name string `json:"name"`
			} `json:"datasets"`
		}
		if err := json.Unmarshal(structured.Bytes(), &decoded); err != nil {
			t.Fatal(err)
		}
		var result []string
		for _, dataset := range decoded.Datasets {
			result = append(result, dataset.Name)
		}
		return result
	}
	if got, want := names(t, nil), []string{"backup/boomerangz/tank/home", "tank/broken", "tank/db", "tank/home", "tank/home/cache", "tank/home/pdf", "tank/vm/images"}; !slices.Equal(got, want) {
		t.Errorf("default datasets = %q, want every status but inactive %q", got, want)
	}
	if got := names(t, []string{"all"}); len(got) != len(busy().GetDatasets()) {
		t.Errorf("--status all kept %q, want every dataset", got)
	}
	if got, want := names(t, []string{"inactive"}), []string{"tank/scratch", "tank/tmp"}; !slices.Equal(got, want) {
		t.Errorf("--status inactive kept %q, want %q", got, want)
	}

	var empty bytes.Buffer
	if err := JSON(&empty, &controlrpc.StatusSnapshot{}, nil); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(empty.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"datasets", "destinations", "receives"} {
		if string(decoded[field]) != "[]" {
			t.Errorf("%s of an empty snapshot = %s, want an empty array", field, decoded[field])
		}
	}
}

// TestJSONListsReceivesWhateverTheFilter keeps every receive in structured
// output, sorted by dataset, whichever datasets the filter keeps: a receive
// into a dataset discovery has not listed is no less a receive.
func TestJSONListsReceivesWhateverTheFilter(t *testing.T) {
	t.Parallel()
	var structured bytes.Buffer
	if err := JSON(&structured, receivingHost(), []string{"active"}); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Receives []struct {
			Dataset    string  `json:"dataset"`
			State      string  `json:"state"`
			Bytes      uint64  `json:"bytes"`
			TotalKnown bool    `json:"total_known"`
			Rate       float64 `json:"bytes_per_second"`
		} `json:"receives"`
	}
	if err := json.Unmarshal(structured.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, receive := range decoded.Receives {
		got = append(got, receive.Dataset+" "+receive.State)
	}
	want := []string{"backup/boomerangz/host-a/tank/db interrupted", "backup/boomerangz/host-a/tank/home receiving", "backup/boomerangz/host-b/tank/vm receiving"}
	if !slices.Equal(got, want) {
		t.Fatalf("receives = %q, want %q", got, want)
	}
	if home := decoded.Receives[1]; home.Bytes != 41<<30 || !home.TotalKnown || home.Rate != 410<<20 {
		t.Fatalf("receive figures = %+v", home)
	}
}

func TestJSONTimesAreUTC(t *testing.T) {
	t.Parallel()
	var structured bytes.Buffer
	if err := JSON(&structured, idle(), nil); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Observed time.Time `json:"observed"`
	}
	if err := json.Unmarshal(structured.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Observed.Equal(observed) || decoded.Observed.Location() != time.UTC {
		t.Fatalf("observed = %v, want %v in UTC", decoded.Observed, observed)
	}
}

func TestWatchJSONKeepsTransitionsInRecordedOrder(t *testing.T) {
	t.Parallel()
	response := &controlrpc.WatchStatusResponse{
		Status: &controlrpc.StatusSnapshot{Revision: 4, Jobs: []*controlrpc.JobStatus{{Job: "remote:tank/data:offsite", State: "waiting-retry", Reason: "second"}}},
		Transitions: []*controlrpc.JobStatus{
			{Job: "remote:tank/data:offsite", State: "waiting-retry", Reason: "first"},
			{Job: "remote:tank/data:offsite", State: "probing"},
			{Job: "a:earlier-in-sort-order", State: "succeeded"},
			{Job: "remote:tank/data:offsite", State: "waiting-retry", Reason: "second"},
		},
	}
	var structured bytes.Buffer
	if err := WatchJSON(&structured, response, nil); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Revision    uint64 `json:"revision"`
		Transitions []struct {
			Job    string `json:"job"`
			State  string `json:"state"`
			Reason string `json:"reason"`
		} `json:"transitions"`
	}
	if err := json.Unmarshal(structured.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON=%s: %v", structured.String(), err)
	}
	var got []string
	for _, transition := range decoded.Transitions {
		got = append(got, transition.Job+" "+transition.State+" "+transition.Reason)
	}
	want := []string{"remote:tank/data:offsite waiting-retry first", "remote:tank/data:offsite probing ", "a:earlier-in-sort-order succeeded ", "remote:tank/data:offsite waiting-retry second"}
	if decoded.Revision != 4 || !slices.Equal(got, want) {
		t.Fatalf("revision=%d transitions=%q, want %q", decoded.Revision, got, want)
	}

	var empty bytes.Buffer
	if err := WatchJSON(&empty, &controlrpc.WatchStatusResponse{Status: &controlrpc.StatusSnapshot{}}, nil); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(empty.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["transitions"]) != "[]" {
		t.Fatalf("a message without transitions must carry an empty array: %s", empty.String())
	}
}

func TestRedrawOverwritesTheFrameInOneWrite(t *testing.T) {
	t.Parallel()
	var writes countingWriter
	if err := Redraw(&writes, []byte("first\nsecond\n")); err != nil {
		t.Fatal(err)
	}
	if writes.count != 1 {
		t.Fatalf("writes=%d, want one write per frame", writes.count)
	}
	if got, want := writes.String(), "\x1b[H\x1b[2Kfirst\n\x1b[2Ksecond\n\x1b[J"; got != want {
		t.Fatalf("frame=%q, want %q", got, want)
	}
}

type countingWriter struct {
	bytes.Buffer
	count int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.count++
	return w.Buffer.Write(p)
}

func TestRelativeTimesFollowTheClock(t *testing.T) {
	t.Parallel()
	frame := func(now time.Time) string {
		var out bytes.Buffer
		if err := Terminal(&out, busy(), Options{Width: 120, Zone: zone, Now: now}); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if frame(time.Time{}) != frame(observed) {
		t.Fatal("an unset clock does not measure from the snapshot's observed time")
	}
	if frame(observed.Add(2*time.Minute)) == frame(observed) {
		t.Fatal("redrawing the same snapshot two minutes later leaves every relative time as it was")
	}
}
