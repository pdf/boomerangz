package statuswait

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

// The daemon log's messages a Log decodes. The job state, discovery, and gap
// lines are the log contract the operations guide documents; the start line
// is written once the control socket is serving, because the command binds
// the socket before it runs the daemon.
const (
	MessageTransition = "worker state"
	MessageDiscovery  = "discovery complete"
	MessageGap        = "status log dropped transitions"
	MessageStarted    = "daemon started"
)

// LineKind says what a Line decoded to.
type LineKind uint8

const (
	// LineOther is any other line: another message, or output that is not a
	// log record at all.
	LineOther LineKind = iota
	// LineTransition is a job state line; Event holds the transition.
	LineTransition
	// LineDiscovery is a discovery line; Generation and Datasets hold it.
	LineDiscovery
	// LineGap marks transitions the daemon's log discarded; Count, First and
	// Last describe them.
	LineGap
)

// Line is one line of daemon output.
type Line struct {
	Kind    LineKind
	Text    string // the line as written
	Time    time.Time
	Level   string
	Message string
	// Event is the transition a job state line records, with At taken from
	// the line's time.
	Event      daemonstate.Event
	Generation uint64
	Datasets   int
	Count      int
	First      time.Time
	Last       time.Time
	// Err is set when a line carries a contract message it does not decode
	// as: a changed key or type, which fails every wait rather than reading
	// as a line the wait is not looking for.
	Err error
}

// Log decodes a daemon subprocess's output as it is written, and wakes waits
// on every line. Attach it as the process's stdout and stderr before starting
// it, and call End once the process has exited.
//
// The log carries every transition from process start in the order the
// daemon recorded them, so a wait can name an occurrence - the second
// succeeded of a job - by the cursor an earlier wait returned. A line saying
// the log dropped transitions breaks that, and fails every wait at once.
type Log struct {
	seq     *sequence[Line]
	raw     bytes.Buffer // guarded by seq.mu
	partial []byte       // an incomplete last line; guarded by seq.mu
}

const logSource = "daemon log"

// NewLog returns an empty Log.
func NewLog() *Log {
	return &Log{seq: newSequence[Line]()}
}

// Write records p and decodes every line it completes.
func (l *Log) Write(p []byte) (int, error) {
	l.seq.update(func() {
		l.raw.Write(p)
		l.partial = append(l.partial, p...)
		for {
			index := bytes.IndexByte(l.partial, '\n')
			if index < 0 {
				break
			}
			l.seq.items = append(l.seq.items, decodeLine(l.partial[:index]))
			l.partial = l.partial[index+1:]
		}
		l.partial = slices.Clone(l.partial)
	})
	return len(p), nil
}

// End decodes any incomplete last line and records that the process exited
// with err, which fails every wait that has not already been satisfied.
func (l *Log) End(err error) {
	l.seq.update(func() {
		if len(l.partial) > 0 {
			l.seq.items = append(l.seq.items, decodeLine(l.partial))
			l.partial = nil
		}
		l.seq.ended = true
		l.seq.err = errors.New("daemon exited")
		if err != nil {
			l.seq.err = fmt.Errorf("daemon exited: %w", err)
		}
	})
}

// String returns everything written, as written.
func (l *Log) String() string {
	l.seq.mu.Lock()
	defer l.seq.mu.Unlock()
	return l.raw.String()
}

// Lines returns a copy of every line decoded so far.
func (l *Log) Lines() []Line { return l.seq.clone() }

// Mark returns the position after every line decoded so far.
func (l *Log) Mark() Cursor { return l.seq.mark() }

// Describe lists every line decoded so far, for a failure the test reports
// itself.
func (l *Log) Describe() string {
	l.seq.mu.Lock()
	defer l.seq.mu.Unlock()
	return describeLines(l.seq.items, 0)
}

// LineCondition inspects the lines decoded from a wait's cursor. It reports
// whether the wait is satisfied, or an error that fails it at once.
type LineCondition func(lines []Line) (bool, error)

// Until waits up to bound for condition to hold over the lines decoded from
// from, rechecking on every line. It fails the test at once when the log
// holds a gap line or a contract line it could not decode, anywhere in the
// log; when condition returns an error; when the process has exited; and when
// bound expires. Each failure lists every line decoded.
func (l *Log) Until(t testing.TB, description string, bound time.Duration, from Cursor, condition LineCondition) {
	t.Helper()
	l.seq.wait(t, description, bound, from, logSource, func(lines []Line) (bool, error) {
		// The whole log, not only what follows the cursor: an occurrence
		// counted across a gap is not the occurrence it claims to be.
		for _, line := range l.seq.items {
			switch {
			case line.Kind == LineGap:
				return false, fmt.Errorf("the daemon log dropped %d transitions between %s and %s", line.Count, line.First.Format(time.RFC3339Nano), line.Last.Format(time.RFC3339Nano))
			case line.Err != nil:
				return false, line.Err
			}
		}
		return condition(lines)
	}, describeLines)
}

// Next waits up to bound for the first line after from that match accepts,
// and returns it with the cursor just past it, from which a later wait finds
// the next occurrence.
func (l *Log) Next(t testing.TB, description string, bound time.Duration, from Cursor, match func(Line) bool) (Line, Cursor) {
	t.Helper()
	var (
		found Line
		at    Cursor
	)
	l.Until(t, description, bound, from, func(lines []Line) (bool, error) {
		for index, line := range lines {
			if match(line) {
				found, at = line, from+Cursor(index)+1
				return true, nil
			}
		}
		return false, nil
	})
	return found, at
}

// Outcome waits up to bound for job to report want after from, by the same
// rule as Waiter.Outcome: running states and the outcomes in retried are
// skipped, and any other outcome of job fails the wait at once.
func (l *Log) Outcome(t testing.TB, bound time.Duration, from Cursor, job, want string, retried ...string) (daemonstate.Event, Cursor) {
	t.Helper()
	var (
		found daemonstate.Event
		at    Cursor
	)
	l.Until(t, outcomeDescription(job, want, retried), bound, from, func(lines []Line) (bool, error) {
		for index, line := range lines {
			if line.Kind != LineTransition {
				continue
			}
			matched, err := outcomeOf(line.Event, job, want, retried)
			if err != nil {
				return false, err
			}
			if matched {
				found, at = line.Event, from+Cursor(index)+1
				return true, nil
			}
		}
		return false, nil
	})
	return found, at
}

// Ended waits up to bound for the first outcome of job after from other than
// the ones in retried, and returns it with the cursor just past it. It is for
// a test that asserts which outcome a job reached and says why when it is the
// wrong one; Outcome fails on the wrong one itself.
func (l *Log) Ended(t testing.TB, bound time.Duration, from Cursor, job string, retried ...string) (daemonstate.Event, Cursor) {
	t.Helper()
	description := job + " to report an outcome"
	if len(retried) > 0 {
		description += " other than " + strings.Join(retried, ", ")
	}
	line, at := l.Next(t, description, bound, from, func(line Line) bool {
		return line.Kind == LineTransition && endedOf(line.Event, job, retried)
	})
	return line.Event, at
}

// Transition matches a job state line whose transition match accepts.
func Transition(match func(daemonstate.Event) bool) func(Line) bool {
	return func(line Line) bool { return line.Kind == LineTransition && match(line.Event) }
}

// Discovered matches a discovery line.
func Discovered(line Line) bool { return line.Kind == LineDiscovery }

// Message matches any line carrying message.
func Message(message string) func(Line) bool {
	return func(line Line) bool { return line.Message == message }
}

// logRecord is the union of the keys the daemon's JSON log handler writes for
// the lines a Log decodes.
type logRecord struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"msg"`
	daemonstate.Event
	Generation uint64    `json:"generation"`
	Datasets   int       `json:"datasets"`
	Count      int       `json:"count"`
	First      time.Time `json:"first"`
	Last       time.Time `json:"last"`
}

func decodeLine(raw []byte) Line {
	line := Line{Text: string(bytes.TrimRight(raw, "\r"))}
	var header struct {
		Message string `json:"msg"`
	}
	if json.Unmarshal(raw, &header) != nil {
		return line
	}
	line.Message = header.Message
	var kind LineKind
	switch header.Message {
	case MessageTransition:
		kind = LineTransition
	case MessageDiscovery:
		kind = LineDiscovery
	case MessageGap:
		kind = LineGap
	}
	// Other messages carry keys of their own, which need not fit the
	// contract's types; only the message and its time are read from them.
	var record logRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		if kind != LineOther {
			line.Err = fmt.Errorf("undecodable %q line: %w: %s", header.Message, err, line.Text)
		}
		return line
	}
	line.Kind, line.Time, line.Level = kind, record.Time, record.Level
	switch kind {
	case LineTransition:
		line.Event = record.Event
		line.Event.Kind, line.Event.At = daemonstate.EventTransition, record.Time
		if line.Event.Job == "" || line.Event.State == "" {
			line.Err = fmt.Errorf("%q line without a job and state: %s", header.Message, line.Text)
		}
	case LineDiscovery:
		line.Generation, line.Datasets = record.Generation, record.Datasets
		if line.Generation == 0 {
			line.Err = fmt.Errorf("%q line without a generation: %s", header.Message, line.Text)
		}
	case LineGap:
		line.Count, line.First, line.Last = record.Count, record.First, record.Last
	case LineOther:
	}
	return line
}

func describeLines(lines []Line, from Cursor) string {
	if len(lines) == 0 {
		return "no daemon log lines decoded"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d daemon log lines decoded, the wait reading from line %d:", len(lines), from)
	for index, line := range lines {
		fmt.Fprintf(&b, "\n  %4d ", index)
		switch line.Kind {
		case LineTransition:
			describeEvent(&b, line.Event)
		case LineDiscovery:
			fmt.Fprintf(&b, "%s %s generation=%d datasets=%d", line.Time.Format(time.RFC3339Nano), line.Message, line.Generation, line.Datasets)
		case LineGap:
			fmt.Fprintf(&b, "%s %s count=%d first=%s last=%s", line.Time.Format(time.RFC3339Nano), line.Message, line.Count, line.First.Format(time.RFC3339Nano), line.Last.Format(time.RFC3339Nano))
		case LineOther:
			b.WriteString(line.Text)
		}
	}
	return b.String()
}
