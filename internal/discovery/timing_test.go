package discovery

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

type slowFirstReader struct {
	*fakeReader
	calls int
	delay time.Duration
}

func (r *slowFirstReader) ListDatasets(ctx context.Context) ([]zfs.Dataset, error) {
	r.calls++
	if r.calls == 1 {
		time.Sleep(r.delay)
	}
	return r.fakeReader.ListDatasets(ctx)
}

func TestRunOverrunCoalescesTicks(t *testing.T) {
	for _, slowCallback := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			reader := &slowFirstReader{fakeReader: fixture()}
			if !slowCallback {
				reader.delay = 3500 * time.Millisecond
			}
			scanner, err := New(reader, Options{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := time.Now()
			var completed []time.Duration
			err = scanner.Run(ctx, time.Second, nil, func(g *Generation, err error) {
				if err != nil || g == nil {
					t.Fatalf("scan: %v", err)
				}
				if slowCallback && len(completed) == 0 {
					time.Sleep(3500 * time.Millisecond)
				}
				completed = append(completed, time.Since(started))
				if len(completed) == 3 {
					cancel()
				}
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			want := []time.Duration{3500 * time.Millisecond, 3500 * time.Millisecond, 4 * time.Second}
			for i, got := range completed {
				if got != want[i] {
					t.Fatalf("slowCallback=%v times=%v want=%v", slowCallback, completed, want)
				}
			}
		})
	}
}

func TestRunReconfigureWakesImmediatelyAndUsesNewInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scanner, err := New(fixture(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started := time.Now()
		var completed []time.Duration
		err = scanner.Run(ctx, time.Hour, nil, func(g *Generation, err error) {
			if err != nil || g == nil {
				t.Fatalf("scan: %v", err)
			}
			completed = append(completed, time.Since(started))
			switch len(completed) {
			case 1:
				if err := scanner.Reconfigure(time.Second, []string{"archive"}); err != nil {
					t.Fatal(err)
				}
			case 3:
				cancel()
			}
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		want := []time.Duration{0, 0, time.Second}
		for i, got := range completed {
			if got != want[i] {
				t.Fatalf("times=%v want=%v", completed, want)
			}
		}
	})
}
