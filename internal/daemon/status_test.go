package daemon

import (
	"reflect"
	"testing"
	"time"
)

func TestStatusRevisionWakesWatchersAndSortsJobs(t *testing.T) {
	t.Parallel()
	store := &StatusStore{}
	done := make(chan error, 1)
	go func() { done <- store.Wait(t.Context(), 0) }()
	store.Record(Event{Job: "z", State: "pending"})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("status watcher was not woken")
	}
	store.Record(Event{Job: "a", State: "running"})
	revision, events := store.SnapshotRevision()
	if revision != 2 || !reflect.DeepEqual([]string{events[0].Job, events[1].Job}, []string{"a", "z"}) {
		t.Fatalf("revision=%d events=%v", revision, events)
	}
}
