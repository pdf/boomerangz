package daemon

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/discovery"
)

// receiveTraceWindow is how long a resume token must go unchanged before the
// receive that set it counts as interrupted. ZFS rewrites the token as each
// transaction group carrying the receive's data syncs, every five seconds by
// default, so a running receive changes it well within this.
const receiveTraceWindow = 15 * time.Second

// resumeTokenReader reads resume tokens beneath destination roots.
type resumeTokenReader interface {
	ResumeTokens(context.Context, []string) (map[string]string, error)
}

// tracedToken is the last token read on one dataset and when it last changed.
// settled is set once the token has been seen to change: until then the
// daemon has not seen the receive move, so cannot call it running.
type tracedToken struct {
	token   string
	changed time.Time
	settled bool
}

// receiveTraces finds receives no endpoint reports, as a plain ssh transfer's
// is: a receive run with -s keeps a resume token on its destination while it
// runs and leaves it there once interrupted. One reading cannot tell the two
// apart, so each token is followed over time: one that changed within
// receiveTraceWindow belongs to a running receive, one unchanged for longer to
// an interrupted or stalled one, and a token not yet seen to change is shown
// only once the window has passed.
type receiveTraces struct {
	mu     sync.Mutex
	tokens map[string]tracedToken
}

// observe classifies a reading taken at now. recheck is when a reading would
// next change what it shows, zero when none would.
func (t *receiveTraces) observe(current map[string]string, now time.Time) (receives []daemonstate.Receive, recheck time.Time) {
	next := make(map[string]tracedToken, len(current))
	for _, dataset := range slices.Sorted(maps.Keys(current)) {
		token := current[dataset]
		entry, seen := t.tokens[dataset]
		switch {
		case !seen:
			entry = tracedToken{token: token, changed: now}
		case entry.token != token:
			entry = tracedToken{token: token, changed: now, settled: true}
		}
		next[dataset] = entry
		due := entry.changed.Add(receiveTraceWindow)
		switch {
		case !now.Before(due):
			receives = append(receives, daemonstate.Receive{Dataset: dataset, State: daemonstate.ReceiveInterrupted})
			continue
		case entry.settled:
			receives = append(receives, daemonstate.Receive{Dataset: dataset, State: daemonstate.ReceiveReceiving})
		}
		if recheck.IsZero() || due.Before(recheck) {
			recheck = due
		}
	}
	t.tokens = next
	return receives, recheck
}

// traceRoots names where receives may land on this host: every target root
// discovery found and every configured replication root that exists, keeping
// only the topmost of any that nest, since one recursive read covers both.
func traceRoots(entries []discovery.Entry, replicationRoots []string) []string {
	existing := make(map[string]bool, len(entries))
	var roots []string
	for _, entry := range entries {
		existing[entry.Dataset.Name] = true
		if entry.Status() == discovery.StatusTarget {
			roots = append(roots, entry.Dataset.Name)
		}
	}
	for _, root := range replicationRoots {
		if existing[root] {
			roots = append(roots, root)
		}
	}
	slices.Sort(roots)
	roots = slices.Compact(roots)
	var topmost []string
	for _, root := range roots {
		if len(topmost) == 0 || (root != topmost[len(topmost)-1] && !strings.HasPrefix(root, topmost[len(topmost)-1]+"/")) {
			topmost = append(topmost, root)
		}
	}
	return topmost
}

// traceReceives reads the resume tokens beneath this host's destination
// roots and publishes the receives they show. It runs after each discovery
// generation, and again whenever a token's state is due to change.
func (r *Runtime) traceReceives() {
	reader, ok := r.backend.(resumeTokenReader)
	if !ok {
		return
	}
	r.mu.Lock()
	var entries []discovery.Entry
	if r.generation != nil {
		entries = r.generation.Entries()
	}
	replicationRoots := slices.Clone(r.config.SSHShell.ReplicationRoots)
	for _, listener := range r.config.Listeners {
		replicationRoots = append(replicationRoots, listener.ReplicationRoots...)
	}
	r.mu.Unlock()
	roots := traceRoots(entries, replicationRoots)
	r.traces.mu.Lock()
	defer r.traces.mu.Unlock()
	current := map[string]string{}
	if len(roots) != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		read, err := reader.ResumeTokens(ctx, roots)
		if err != nil {
			// A root destroyed since discovery fails the whole read; the
			// next generation reads again without it.
			r.logger.Warn("read receive resume tokens", "error", err)
			return
		}
		current = read
	}
	receives, recheck := r.traces.observe(current, time.Now())
	r.status.publishTraced(receives)
	if !recheck.IsZero() {
		r.schedule("receive-traces", recheck, r.traceReceives)
	}
}
