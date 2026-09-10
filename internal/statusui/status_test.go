package statusui

import (
	"bytes"
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
	}}
	var output bytes.Buffer
	if err := Terminal(&output, snapshot, 140); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "512 B/1.0 KiB") || !strings.Contains(output.String(), "@ 256 B/s") || !strings.Contains(output.String(), "ETA 2s") || !strings.Contains(output.String(), "[") || !strings.Contains(output.String(), "deactivate:tank/data/child") {
		t.Fatalf("progress output=%q", output.String())
	}
}
