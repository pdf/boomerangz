package cli

import (
	"context"
	"errors"
	"testing"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
)

type drawn struct {
	revision uint64
	fresh    bool
	now      time.Time
}

func TestWatchRedrawsTheLatestUpdateOnEveryTick(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	responses := make(chan *controlrpc.WatchStatusResponse)
	ended := errors.New("stream ended")
	receive := func() (*controlrpc.WatchStatusResponse, error) {
		select {
		case response, ok := <-responses:
			if !ok {
				return nil, ended
			}
			return response, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ticks := make(chan time.Time)
	draws := make(chan drawn)
	result := make(chan error, 1)
	go func() {
		result <- watchFrames(ctx, receive, ticks, func(response *controlrpc.WatchStatusResponse, fresh bool, now time.Time) error {
			draws <- drawn{response.GetStatus().GetRevision(), fresh, now}
			return nil
		})
	}()
	next := func() drawn {
		t.Helper()
		select {
		case draw := <-draws:
			return draw
		case <-time.After(5 * time.Second):
			t.Fatal("nothing was drawn")
			return drawn{}
		}
	}

	// A tick before the first update has nothing to redraw.
	ticks <- time.Unix(100, 0)
	responses <- &controlrpc.WatchStatusResponse{Status: &controlrpc.StatusSnapshot{Revision: 1}}
	if draw := next(); draw.revision != 1 || !draw.fresh {
		t.Fatalf("first draw = %+v, want revision 1 drawn fresh", draw)
	}
	tick := time.Unix(200, 0)
	ticks <- tick
	if draw := next(); draw.revision != 1 || draw.fresh || !draw.now.Equal(tick) {
		t.Fatalf("tick draw = %+v, want revision 1 redrawn at %v", draw, tick)
	}
	responses <- &controlrpc.WatchStatusResponse{Status: &controlrpc.StatusSnapshot{Revision: 2}}
	if draw := next(); draw.revision != 2 || !draw.fresh {
		t.Fatalf("second update draw = %+v, want revision 2 drawn fresh", draw)
	}
	close(responses)
	if err := <-result; !errors.Is(err, ended) {
		t.Fatalf("watch ended with %v, want the stream's error", err)
	}
}
