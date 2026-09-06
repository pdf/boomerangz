package lifecycle

import (
	"context"
	"testing"
)

func TestDeactivationGate(t *testing.T) {
	t.Parallel()
	var g Gate
	if _, err := g.Queue(t.Context(), "tank/data", Management); err == nil {
		t.Fatal("queued disabled work")
	}
	if err := g.SetEnabled("tank/data", true); err != nil {
		t.Fatal(err)
	}
	management, _ := g.Queue(t.Context(), "tank/data", Management)
	defer management.Finish()
	transfer, _ := g.Queue(t.Context(), "tank/data", Transfer)
	defer transfer.Finish()
	queued, _ := g.Queue(t.Context(), "tank/data", Management)
	defer queued.Finish()
	if err := management.Start(); err != nil {
		t.Fatal(err)
	}
	if err := transfer.Start(); err != nil {
		t.Fatal(err)
	}
	if err := g.SetEnabled("tank/data", false); err != nil {
		t.Fatal(err)
	}
	if management.Context().Err() != nil || transfer.Context().Err() == nil || queued.Context().Err() == nil {
		t.Fatal("incorrect deactivation cancellation")
	}
	if err := queued.Start(); err == nil {
		t.Fatal("started discarded queue item")
	}
	status := g.Status("tank/data")
	if status.Enabled || status.Queued != 0 || status.Management != 1 || status.Transfers != 1 {
		t.Fatalf("status=%v", status)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.WaitQuiescent(ctx, "tank/data"); err == nil {
		t.Fatal("reported active operations as quiescent")
	}
	management.Finish()
	transfer.Finish()
	if err := g.WaitQuiescent(t.Context(), "tank/data"); err != nil {
		t.Fatal(err)
	}
	if err := g.SetEnabled("tank/data", true); err != nil {
		t.Fatal(err)
	}
	if err := queued.Start(); err == nil {
		t.Fatal("revived discarded work after re-enable")
	}
}

func TestQuiescenceWaitsForResultReconstruction(t *testing.T) {
	t.Parallel()
	var g Gate
	if err := g.SetEnabled("tank/data", true); err != nil {
		t.Fatal(err)
	}
	job, _ := g.Queue(t.Context(), "tank/data", Management)
	if err := job.Start(); err != nil {
		t.Fatal(err)
	}
	if err := g.SetEnabled("tank/data", false); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.WaitQuiescent(t.Context(), "tank/data") }()
	job.Finish()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestQuiescenceRejectsRelatedActiveRoot(t *testing.T) {
	t.Parallel()
	var gate Gate
	if err := gate.SetEnabled("tank/root", true); err != nil {
		t.Fatal(err)
	}
	if err := gate.WaitQuiescent(t.Context(), "tank/root/child"); err == nil {
		t.Fatal("child was considered quiescent under an active root")
	}
	if err := gate.WaitQuiescent(t.Context(), "tank"); err == nil {
		t.Fatal("ancestor was considered quiescent with an active descendant")
	}
}
