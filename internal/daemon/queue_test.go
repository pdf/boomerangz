package daemon

import (
	"context"
	"reflect"
	"testing"
)

func queueJob(id, group, scope string) Job {
	return Job{ID: id, Group: group, Scope: scope, Run: func(context.Context) Outcome { return Outcome{} }}
}

func TestFairQueueDeduplicatesBoundsAndRotatesGroups(t *testing.T) {
	t.Parallel()
	queue, err := NewFairQueue(4)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range []Job{queueJob("a1", "a", "a"), queueJob("a2", "a", "a"), queueJob("b1", "b", "b"), queueJob("b2", "b", "b")} {
		if added, offerErr := queue.Offer(job); offerErr != nil || !added {
			t.Fatalf("offer: %v %v", added, offerErr)
		}
	}
	if added, err := queue.Offer(queueJob("a1", "a", "a")); err != nil || added {
		t.Fatalf("duplicate: %v %v", added, err)
	}
	if _, err := queue.Offer(queueJob("c1", "c", "c")); err == nil {
		t.Fatal("queue exceeded its bound")
	}
	var order []string
	for range 4 {
		job, ok := queue.Pop(t.Context())
		if !ok {
			t.Fatal("queue ended early")
		}
		order = append(order, job.ID)
	}
	if !reflect.DeepEqual(order, []string{"a1", "b1", "a2", "b2"}) {
		t.Fatalf("unfair order: %v", order)
	}
}

func TestFairQueueRemovesDeactivatedScope(t *testing.T) {
	t.Parallel()
	queue, _ := NewFairQueue(4)
	dropped := 0
	job := queueJob("a", "a", "root")
	job.Drop = func() { dropped++ }
	_, _ = queue.Offer(job)
	_, _ = queue.Offer(queueJob("b", "b", "other"))
	if removed := queue.RemoveScope("root"); removed != 1 || dropped != 1 {
		t.Fatalf("removed=%d dropped=%d", removed, dropped)
	}
	remaining, ok := queue.Pop(t.Context())
	if !ok || remaining.ID != "b" {
		t.Fatalf("unexpected remaining job: %#v", remaining)
	}
}
