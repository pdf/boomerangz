package statuswait

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// daemonLogger writes to log the way the daemon command does.
func daemonLogger(log *Log) *slog.Logger {
	return slog.New(slog.NewJSONHandler(log, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// logTransition writes a job state line with the keys the daemon's log
// subscriber writes.
func logTransition(logger *slog.Logger, runID uint64, job, state, reason string, extra ...any) {
	args := append([]any{"run_id", runID, "pool", "management", "job", job, "scope", "tank/a", "target", "", "state", state, "reason", reason, "pending", 0}, extra...)
	switch state {
	case "failed":
		logger.Error(MessageTransition, args...)
	case "blocked", "waiting-retry":
		logger.Warn(MessageTransition, args...)
	default:
		logger.Info(MessageTransition, args...)
	}
}

func TestLogDecodesTheContractLines(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logger.Info(MessageStarted, "management_workers", 1, "local_transfer_workers", 2, "remote_transfer_workers", 1)
	logger.Info(MessageDiscovery, "generation", uint64(3), "datasets", 5)
	logTransition(logger, 7, "prune:tank/a", "succeeded", "", "destroyed", []string{"tank/a@old"}, "destroyed_count", 1)
	logTransition(logger, 8, "snapshot:tank/a", "scheduled", "existing owned snapshot sets the next deadline", "snapshot", "tank/a@one")
	logTransition(logger, 9, "config:reload", "succeeded", "", "config_generation", uint64(2))
	logger.Error("discovery failed", "error", "zfs list: exit status 1")
	if _, err := log.Write([]byte("boomerangz: error: not a log record\n")); err != nil {
		t.Fatal(err)
	}

	lines := log.Lines()
	if len(lines) != 7 {
		t.Fatalf("decoded %d lines, want 7:\n%s", len(lines), log.String())
	}
	if lines[0].Kind != LineOther || lines[0].Message != MessageStarted {
		t.Fatalf("start line = %+v", lines[0])
	}
	if got := lines[1]; got.Kind != LineDiscovery || got.Generation != 3 || got.Datasets != 5 {
		t.Fatalf("discovery line = %+v", got)
	}
	prune := lines[2].Event
	if lines[2].Kind != LineTransition || prune.RunID != 7 || prune.Job != "prune:tank/a" || prune.State != "succeeded" || prune.DestroyedCount != 1 || len(prune.Destroyed) != 1 || prune.Destroyed[0] != "tank/a@old" {
		t.Fatalf("prune line = %+v", lines[2])
	}
	if prune.At.IsZero() || !prune.At.Equal(lines[2].Time) || lines[2].Level != "INFO" {
		t.Fatalf("a transition's time and level come from the line: %+v", lines[2])
	}
	if scheduled := lines[3].Event; scheduled.Reason != "existing owned snapshot sets the next deadline" || scheduled.Snapshot != "tank/a@one" {
		t.Fatalf("scheduled line = %+v", lines[3])
	}
	if reload := lines[4].Event; reload.ConfigGeneration != 2 {
		t.Fatalf("reload line = %+v", lines[4])
	}
	if lines[5].Kind != LineOther || lines[5].Level != "ERROR" || lines[6].Kind != LineOther || lines[6].Message != "" {
		t.Fatalf("other lines = %+v, %+v", lines[5], lines[6])
	}
	for _, line := range lines {
		if line.Err != nil {
			t.Fatalf("line %q failed to decode: %v", line.Text, line.Err)
		}
	}
}

func TestLogJoinsLinesSplitAcrossWrites(t *testing.T) {
	log := NewLog()
	line := `{"time":"2026-09-14T00:00:00Z","level":"INFO","msg":"worker state","run_id":1,"job":"snapshot:tank/a","state":"succeeded"}`
	for _, part := range []string{line[:10], line[10:40], line[40:] + "\n" + `{"msg":"daemon st`, `arted"}`} {
		if _, err := log.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	if got := log.Mark(); got != 1 {
		t.Fatalf("decoded %d lines before the second completed, want 1", got)
	}
	log.End(nil)
	lines := log.Lines()
	if len(lines) != 2 || lines[0].Event.Job != "snapshot:tank/a" || lines[1].Message != MessageStarted {
		t.Fatalf("lines = %+v", lines)
	}
	if log.String() != line+"\n"+`{"msg":"daemon started"}` {
		t.Fatalf("raw output = %q", log.String())
	}
}

func TestLogCountsOccurrences(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logTransition(logger, 1, "snapshot:tank/a", "snapshotting", "")
	logTransition(logger, 1, "snapshot:tank/a", "succeeded", "", "snapshot", "tank/a@one")

	firstFound := make(chan struct{})
	written := make(chan struct{})
	go func() {
		defer close(written)
		<-firstFound
		time.Sleep(20 * time.Millisecond) // a fixture: let the second wait block first
		logTransition(logger, 2, "snapshot:tank/a", "pending-management", "")
		logTransition(logger, 2, "snapshot:tank/a", "scheduled", "existing owned snapshot sets the next deadline")
		logTransition(logger, 3, "snapshot:tank/a", "succeeded", "", "snapshot", "tank/a@two")
	}()
	var second string
	message := failure(t, func(tb testing.TB) {
		first, after := log.Outcome(tb, time.Second, 0, "snapshot:tank/a", "succeeded")
		if first.Snapshot != "tank/a@one" {
			tb.Fatalf("first = %+v", first)
		}
		close(firstFound)
		event, _ := log.Outcome(tb, 5*time.Second, after, "snapshot:tank/a", "succeeded", "scheduled")
		second = event.Snapshot
	})
	<-written
	if message != "" {
		t.Fatal(message)
	}
	if second != "tank/a@two" {
		t.Fatalf("second occurrence named %q, want tank/a@two", second)
	}
}

func TestLogOutcomeFailsAtOnceOnAnotherOutcome(t *testing.T) {
	log := NewLog()
	logTransition(daemonLogger(log), 1, "snapshot:tank/a", "succeeded", "")

	message := failure(t, func(tb testing.TB) {
		log.Outcome(tb, time.Hour, 0, "snapshot:tank/a", "scheduled")
	})
	if !strings.Contains(message, "snapshot:tank/a ended succeeded") || !strings.Contains(message, "state=succeeded") {
		t.Fatalf("failure = %q, want the other outcome and the lines decoded", message)
	}
}

func TestLogEndedReturnsTheFirstOutcomeNotRetried(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logTransition(logger, 1, "local:tank/a:backup/a", "planning", "")
	logTransition(logger, 1, "local:tank/a:backup/a", "waiting-retry", "source changed")
	logTransition(logger, 2, "local:tank/b:backup/b", "blocked", "another job")
	logTransition(logger, 3, "local:tank/a:backup/a", "blocked", "reseed required")

	event, at := log.Ended(t, time.Second, 0, "local:tank/a:backup/a", "waiting-retry")
	if event.State != "blocked" || event.Reason != "reseed required" || at != 4 {
		t.Fatalf("ended = %+v at %d, want the block at 4", event, at)
	}
}

func TestLogFailsAtOnceOnAGapLine(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logTransition(logger, 1, "snapshot:tank/a", "succeeded", "")
	mark := log.Mark()
	first, last := time.Date(2026, 9, 14, 1, 2, 3, 0, time.UTC), time.Date(2026, 9, 14, 1, 2, 4, 0, time.UTC)
	logger.Error(MessageGap, "count", 12, "first", first, "last", last)
	logTransition(logger, 2, "snapshot:tank/a", "succeeded", "")

	// The gap is before the cursor and the wait is already satisfied past it,
	// and it still fails: nothing counted past a gap can be trusted.
	message := failure(t, func(tb testing.TB) {
		log.Next(tb, "a later success", time.Hour, mark+1, Transition(Job("snapshot:tank/a", "succeeded")))
	})
	if !strings.Contains(message, "the daemon log dropped 12 transitions between 2026-09-14T01:02:03Z and 2026-09-14T01:02:04Z") {
		t.Fatalf("failure = %q, want the gap named", message)
	}
}

func TestLogFailsAtOnceOnAnUndecodableContractLine(t *testing.T) {
	log := NewLog()
	if _, err := fmt.Fprintln(log, `{"time":"2026-09-14T00:00:00Z","level":"INFO","msg":"worker state","run_id":"seven","job":"snapshot:tank/a","state":"succeeded"}`); err != nil {
		t.Fatal(err)
	}
	message := failure(t, func(tb testing.TB) {
		log.Next(tb, "anything", time.Hour, 0, func(Line) bool { return true })
	})
	if !strings.Contains(message, `undecodable "worker state" line`) {
		t.Fatalf("failure = %q, want the undecodable line named", message)
	}
}

func TestLogListsEveryDecodedLineOnExpiry(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logger.Info(MessageStarted)
	logger.Info(MessageDiscovery, "generation", uint64(1), "datasets", 2)
	logTransition(logger, 1, "snapshot:tank/a", "snapshotting", "")
	mark := log.Mark()

	message := failure(t, func(tb testing.TB) {
		log.Outcome(tb, 50*time.Millisecond, mark, "snapshot:tank/a", "succeeded")
	})
	for _, want := range []string{
		"timed out after 50ms waiting for snapshot:tank/a to report succeeded",
		"3 daemon log lines decoded, the wait reading from line 3",
		`"msg":"daemon started"`,
		"discovery complete generation=1 datasets=2",
		"snapshot:tank/a pool=management state=snapshotting",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure = %q, missing %q", message, want)
		}
	}
}

func TestLogFailsWhenTheDaemonExits(t *testing.T) {
	log := NewLog()
	logger := daemonLogger(log)
	logger.Info(MessageStarted)
	log.End(errors.New("signal: killed"))

	message := failure(t, func(tb testing.TB) {
		log.Next(tb, "discovery", time.Hour, 0, Discovered)
	})
	if !strings.Contains(message, "daemon log ended: daemon exited: signal: killed") || !strings.Contains(message, `"msg":"daemon started"`) {
		t.Fatalf("failure = %q, want the exit and the lines decoded", message)
	}

	// A wait the log satisfied before the exit still returns.
	if line, at := log.Next(t, "the start line", time.Second, 0, Message(MessageStarted)); line.Message != MessageStarted || at != 1 {
		t.Fatalf("start = %+v at %d", line, at)
	}
}
