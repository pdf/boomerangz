package statusui

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
)

func TestTerminalNarrowAndJSON(t *testing.T) {
	t.Parallel()
	snapshot := &controlrpc.StatusSnapshot{Generation: 2, ConfigGeneration: 3, Datasets: []*controlrpc.DatasetStatus{{Name: "tank/data", Active: true}}, Queues: []*controlrpc.QueueStatus{{Name: "management", Capacity: 10, Pending: 2}}}
	var terminal bytes.Buffer
	if err := Terminal(&terminal, snapshot, 24); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(terminal.String(), "tank/data") || strings.Contains(terminal.String(), "[") {
		t.Fatalf("narrow output=%q", terminal.String())
	}
	var structured bytes.Buffer
	if err := JSON(&structured, snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(structured.String(), `"generation":2`) || !strings.Contains(structured.String(), `"config_generation":3`) {
		t.Fatalf("JSON=%s", structured.String())
	}
}

func TestTerminalShowsKnownTransferProgress(t *testing.T) {
	t.Parallel()
	snapshot := &controlrpc.StatusSnapshot{Jobs: []*controlrpc.JobStatus{
		{Job: "local:tank/data:backup/data", State: "sending", Bytes: 512, TotalBytes: 1024, TotalKnown: true, BytesPerSecond: 256, EtaNanoseconds: int64(2 * time.Second)},
		{Job: "inactive:tank/data/child:false", State: "succeeded"},
		{Job: "destination-prune:tank/old:target", State: "cancelled", Reason: "context canceled"},
	}}
	var output bytes.Buffer
	if err := Terminal(&output, snapshot, 140); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "512 B/1.0 KiB") || !strings.Contains(output.String(), "@ 256 B/s") || !strings.Contains(output.String(), "ETA 2s") || !strings.Contains(output.String(), "[") || !strings.Contains(output.String(), "deactivate:tank/data/child") || strings.Contains(output.String(), "tank/old") {
		t.Fatalf("progress output=%q", output.String())
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
	if err := WatchJSON(&structured, response); err != nil {
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
	if err := WatchJSON(&empty, &controlrpc.WatchStatusResponse{Status: &controlrpc.StatusSnapshot{}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(empty.String(), `"transitions":[]`) {
		t.Fatalf("a message without transitions must carry an empty array: %s", empty.String())
	}
}

func TestTailKeepsTheNewestTransitionsOldestFirst(t *testing.T) {
	t.Parallel()
	tail := NewTail(3)
	var output bytes.Buffer
	if err := tail.Terminal(&output, 80); err != nil || output.Len() != 0 {
		t.Fatalf("an empty tail wrote %q err=%v", output.String(), err)
	}
	at := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	tail.Add([]*controlrpc.JobStatus{{Job: "snapshot:tank/a", State: "snapshotting", ChangedUnixNano: at.UnixNano()}, {Job: "snapshot:tank/a", State: "succeeded", ChangedUnixNano: at.UnixNano()}})
	tail.Add(nil)
	tail.Add([]*controlrpc.JobStatus{{Job: "inactive:tank/b:false", State: "retiring"}, {Job: "remote:tank/c:offsite", State: "waiting-retry", Reason: "connection refused by the remote peer"}})
	if err := tail.Terminal(&output, 80); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 4 || lines[0] != "RECENT TRANSITIONS" {
		t.Fatalf("tail=%q", output.String())
	}
	for index, want := range [][]string{{"2026-09-13T12:00:00Z", "snapshot:tank/a", "succeeded"}, {"-", "deactivate:tank/b", "retiring"}, {"-", "remote:tank/c:offsite", "waiting-retry", "connection"}} {
		if fields := strings.Fields(lines[index+1]); len(fields) < len(want) || !slices.Equal(fields[:len(want)], want) {
			t.Fatalf("line %d=%q, want it to begin with %q; tail=%q", index+1, lines[index+1], want, output.String())
		}
	}
	output.Reset()
	if err := tail.Terminal(&output, 30); err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(output.String()) {
		if len(strings.TrimSuffix(line, "\n")) > 30 {
			t.Fatalf("line %q exceeds the width", line)
		}
	}
}

func TestTailNamesWhatEachTransitionActedOn(t *testing.T) {
	t.Parallel()
	tail := NewTail(4)
	tail.Add([]*controlrpc.JobStatus{
		{Job: "local:tank/a:backup/a", State: "sending", Snapshot: "tank/a@two", Base: "tank/a@one", Mode: "incremental-latest"},
		{Job: "prune:tank/a", State: "succeeded", Destroyed: []string{"tank/a@old"}, DestroyedCount: 1},
		{Job: "remote:tank/a:offsite", State: "waiting-retry", Snapshot: "tank/a@two", Reason: "connection refused"},
		{Job: "config:reload", State: "succeeded", ConfigGeneration: 3},
	})
	var output bytes.Buffer
	if err := tail.Terminal(&output, 200); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	for index, want := range []string{
		"snapshot=tank/a@two base=tank/a@one mode=incremental-latest",
		"destroyed=1",
		"snapshot=tank/a@two connection refused",
		"config_generation=3",
	} {
		if !strings.HasSuffix(strings.TrimSpace(lines[index+1]), want) {
			t.Fatalf("line %d = %q, want it to end %q", index+1, lines[index+1], want)
		}
	}
}

func TestTerminalAndJSONReportDestinationPermissionWarnings(t *testing.T) {
	t.Parallel()
	snapshot := &controlrpc.StatusSnapshot{Destinations: []*controlrpc.DestinationPermissions{
		{Destination: "offsite", Remote: true, Error: "list destination datasets: connection refused"},
		{Destination: "backup/boomerangz", Dataset: "backup/boomerangz", Account: "boomerangz", Missing: []string{"canmount", "rename"}},
		{Destination: "backup/complete", Dataset: "backup/complete"},
	}}
	var terminal bytes.Buffer
	// Warnings are never cut to width: the operator needs every name.
	if err := Terminal(&terminal, snapshot, 24); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"WARNINGS\nlocal destination backup/boomerangz: boomerangz lacks delegated ZFS permissions on backup/boomerangz: canmount,rename\n",
		"remote destination offsite: permissions not checked: list destination datasets: connection refused\n",
	} {
		if !strings.Contains(terminal.String(), want) {
			t.Fatalf("terminal output = %q, want %q", terminal.String(), want)
		}
	}
	if strings.Contains(terminal.String(), "backup/complete") {
		t.Fatalf("a destination with nothing missing was warned about: %q", terminal.String())
	}
	var structured bytes.Buffer
	if err := JSON(&structured, snapshot); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Destinations []struct {
			Destination string   `json:"destination"`
			Remote      bool     `json:"remote"`
			Missing     []string `json:"missing"`
			Error       string   `json:"error"`
		} `json:"destinations"`
	}
	if err := json.Unmarshal(structured.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Destinations) != 3 || decoded.Destinations[0].Destination != "backup/boomerangz" || !slices.Equal(decoded.Destinations[0].Missing, []string{"canmount", "rename"}) || decoded.Destinations[2].Destination != "offsite" || !decoded.Destinations[2].Remote || decoded.Destinations[2].Error == "" {
		t.Fatalf("JSON destinations = %+v", decoded.Destinations)
	}
	structured.Reset()
	if err := JSON(&structured, &controlrpc.StatusSnapshot{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(structured.String(), `"destinations":[]`) {
		t.Fatalf("JSON before any check = %s", structured.String())
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
	got := writes.String()
	if strings.Contains(got, "\x1b[2J") {
		t.Fatalf("frame clears the screen before drawing: %q", got)
	}
	if want := "\x1b[H\x1b[2Kfirst\n\x1b[2Ksecond\n\x1b[J"; got != want {
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
