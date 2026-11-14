// Package statusui renders control snapshots for terminals and structured output.
package statusui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/discovery"
)

type output struct {
	Revision         uint64                      `json:"revision"`
	Observed         time.Time                   `json:"observed"`
	Generation       uint64                      `json:"generation"`
	ConfigGeneration uint64                      `json:"config_generation"`
	Datasets         []*controlrpc.DatasetStatus `json:"datasets"`
	Queues           []*controlrpc.QueueStatus   `json:"queues"`
	Jobs             []*controlrpc.JobStatus     `json:"jobs"`
	// Destinations is every destination's latest permission check, missing
	// permissions and failed checks included; it is an empty array, not
	// absent, before any check.
	Destinations []*controlrpc.DestinationPermissions `json:"destinations"`
	// Receives is every receive into this host in progress, and every
	// interrupted one; an empty array, not absent, when there are none.
	Receives []*controlrpc.ReceiveStatus `json:"receives"`
}

// normalized sorts the snapshot's lists and keeps the datasets statuses asks
// for, as discovery.StatusFilter decides; hidden counts the datasets the
// default filter left out.
func normalized(snapshot *controlrpc.StatusSnapshot, statuses []string) (result output, hidden int) {
	result = output{Revision: snapshot.GetRevision(), Generation: snapshot.GetGeneration(), ConfigGeneration: snapshot.GetConfigGeneration(), Queues: slices.Clone(snapshot.GetQueues()), Jobs: slices.Clone(snapshot.GetJobs()), Destinations: slices.Clone(snapshot.GetDestinations()), Receives: slices.Clone(snapshot.GetReceives())}
	keep, defaulted := discovery.StatusFilter(statuses)
	result.Datasets = []*controlrpc.DatasetStatus{}
	for _, dataset := range snapshot.GetDatasets() {
		if keep(dataset.GetStatus()) {
			result.Datasets = append(result.Datasets, dataset)
		} else if defaulted {
			hidden++
		}
	}
	if result.Destinations == nil {
		result.Destinations = []*controlrpc.DestinationPermissions{}
	}
	if result.Receives == nil {
		result.Receives = []*controlrpc.ReceiveStatus{}
	}
	if snapshot.GetObservedUnixNano() != 0 {
		result.Observed = time.Unix(0, snapshot.GetObservedUnixNano()).UTC()
	}
	slices.SortFunc(result.Datasets, func(a, b *controlrpc.DatasetStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(result.Queues, func(a, b *controlrpc.QueueStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(result.Jobs, func(a, b *controlrpc.JobStatus) int { return strings.Compare(a.GetJob(), b.GetJob()) })
	slices.SortStableFunc(result.Receives, func(a, b *controlrpc.ReceiveStatus) int {
		if order := strings.Compare(a.GetDataset(), b.GetDataset()); order != 0 {
			return order
		}
		return int(min(max(a.GetStartedUnixNano()-b.GetStartedUnixNano(), -1), 1))
	})
	slices.SortStableFunc(result.Destinations, func(a, b *controlrpc.DestinationPermissions) int {
		if a.GetRemote() != b.GetRemote() {
			if a.GetRemote() {
				return 1
			}
			return -1
		}
		return strings.Compare(a.GetDestination(), b.GetDestination())
	})
	return result, hidden
}

// JSON writes one stable JSON object followed by a newline, with the datasets
// statuses asks for.
func JSON(writer io.Writer, snapshot *controlrpc.StatusSnapshot, statuses []string) error {
	view, _ := normalized(snapshot, statuses)
	return json.NewEncoder(writer).Encode(view)
}

type watchOutput struct {
	output
	Transitions []*controlrpc.JobStatus `json:"transitions"`
}

// WatchJSON writes one watch message as a stable JSON object followed by a
// newline: the snapshot's fields, and "transitions", every transition since the
// previous message in the order the daemon recorded them. Transitions are never
// sorted, and the field is an empty array rather than absent when there are
// none.
func WatchJSON(writer io.Writer, response *controlrpc.WatchStatusResponse, statuses []string) error {
	transitions := slices.Clone(response.GetTransitions())
	if transitions == nil {
		transitions = []*controlrpc.JobStatus{}
	}
	view, _ := normalized(response.GetStatus(), statuses)
	return json.NewEncoder(writer).Encode(watchOutput{output: view, Transitions: transitions})
}

// Options shapes a terminal display.
type Options struct {
	// Width is the terminal width; 80 when unset.
	Width int
	// Statuses selects the datasets listed, as `dataset list --status` does.
	Statuses []string
	// Zone is where times are shown; the local zone when unset.
	Zone *time.Location
	// Color marks headings and problems with ANSI attributes.
	Color bool
	// Now is the clock relative times ("in 6m", "30s ago") are measured
	// against. Unset, it is the snapshot's observed time. A display that
	// redraws one snapshot as time passes sets it on every frame.
	Now time.Time
}

// ANSI attributes the display uses.
const (
	styleBold   = "1"
	styleRed    = "31"
	styleYellow = "33"
)

// paint wraps text in an ANSI attribute when color is on.
func paint(text, style string, color bool) string {
	if !color || style == "" || text == "" {
		return text
	}
	return "\x1b[" + style + "m" + text + "\x1b[0m"
}

func heading(out *strings.Builder, title string, color bool) {
	out.WriteString("\n" + paint(title, styleBold, color) + "\n")
}

func (o Options) normalized() Options {
	if o.Width <= 0 {
		o.Width = 80
	}
	if o.Zone == nil {
		o.Zone = time.Local
	}
	return o
}

// jobClass groups job states by what an operator does about them.
type jobClass int

const (
	classDone jobClass = iota
	classQueued
	classRunning
	classProblem
)

func classify(job *controlrpc.JobStatus) jobClass {
	switch state := job.GetState(); {
	case state == "failed" || state == "blocked" || state == "waiting-retry":
		return classProblem
	case state == "succeeded" || state == "cancelled" || state == "scheduled":
		return classDone
	case strings.HasPrefix(state, "pending-") || job.GetQueuePosition() > 0:
		return classQueued
	default:
		return classRunning
	}
}

// problemRank orders problems most severe first.
func problemRank(state string) int {
	switch state {
	case "failed":
		return 0
	case "blocked":
		return 1
	default:
		return 2
	}
}

// jobKind is the kind prefix of a job ID.
func jobKind(job *controlrpc.JobStatus) string {
	kind, _, _ := strings.Cut(job.GetJob(), ":")
	return kind
}

// jobRest is what follows "<kind>:<dataset>:" in a job ID. Dataset names may
// contain colons, so the dataset is cut by the job's own dataset field rather
// than by splitting on colons.
func jobRest(job *controlrpc.JobStatus) (string, bool) {
	return strings.CutPrefix(job.GetJob(), jobKind(job)+":"+job.GetDataset()+":")
}

// targetName shows a canonical target the way configuration names it: a
// local destination root bare, anything else as given.
func targetName(canonical string) string {
	if root, ok := strings.CutPrefix(canonical, "local:"); ok {
		return root
	}
	return canonical
}

// destination names what a transfer job sends to, and whether it is one.
func destination(job *controlrpc.JobStatus) (string, bool) {
	rest, ok := jobRest(job)
	if !ok {
		return "", false
	}
	switch jobKind(job) {
	case "local":
		return rest, true
	case "remote":
		return "remote:" + rest, true
	}
	return "", false
}

// jobLabel names a job in words: the dataset it acts on and what it does.
func jobLabel(job *controlrpc.JobStatus) string {
	dataset := job.GetDataset()
	if target, ok := destination(job); ok {
		return dataset + " -> " + target
	}
	switch kind := jobKind(job); kind {
	case "snapshot", "prune", "retire":
		if dataset != "" {
			return dataset + " " + kind
		}
	case "destination-prune":
		if dataset != "" {
			return dataset + " prune on " + targetName(job.GetTarget())
		}
	case "inactive":
		switch rest, _ := jobRest(job); rest {
		case "true":
			return dataset + " activate"
		case "false":
			return dataset + " deactivate"
		}
	case "config":
		if job.GetJob() == "config:reload" {
			return "configuration reload"
		}
	}
	return job.GetJob()
}

// ago says how long before now at was, coarsely: "now", "47s ago", "12m
// ago", "3h ago", "2d ago".
func ago(at, now time.Time) string {
	if at.IsZero() {
		return ""
	}
	elapsed := now.Sub(at)
	if elapsed < time.Second {
		return "now"
	}
	return coarse(elapsed) + " ago"
}

// until says how long after now at is, coarsely: "in 13m", or "due" once it
// has passed.
func until(at, now time.Time) string {
	if at.IsZero() {
		return ""
	}
	remaining := at.Sub(now)
	if remaining < time.Second {
		return "due"
	}
	return "in " + coarse(remaining)
}

// coarse rounds a duration to its largest whole unit.
func coarse(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d/time.Second))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	default:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
}

func unixTime(nanos int64) time.Time {
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

func byteSize(bytes uint64) string {
	const unit = uint64(1024)
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value := float64(bytes)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= float64(unit)
		if value < float64(unit) {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f PiB", value/float64(unit))
}

// figures are how far a stream has got, sending or receiving.
type figures struct {
	bytes, total uint64
	known        bool
	rate         float64
	eta          int64
}

func jobFigures(job *controlrpc.JobStatus) figures {
	return figures{bytes: job.GetBytes(), total: job.GetTotalBytes(), known: job.GetTotalKnown(), rate: job.GetBytesPerSecond(), eta: job.GetEtaNanoseconds()}
}

func receiveFigures(receive *controlrpc.ReceiveStatus) figures {
	return figures{bytes: receive.GetBytes(), total: receive.GetTotalBytes(), known: receive.GetTotalKnown(), rate: receive.GetBytesPerSecond(), eta: receive.GetEtaNanoseconds()}
}

// percent is how much of a known total a stream has moved, whole and capped
// at 100.
func percent(f figures) (int, bool) {
	if !f.known || f.total == 0 {
		return 0, false
	}
	return int(min(f.bytes, f.total) * 100 / f.total), true
}

// progress describes a moving stream: how far, how fast, how long left, as
// parts that are never split across lines.
func progress(f figures) []string {
	var parts []string
	if share, known := percent(f); known {
		parts = append(parts, fmt.Sprintf("%d%%", share), byteSize(f.bytes)+" of "+byteSize(f.total))
	} else if f.bytes != 0 {
		parts = append(parts, byteSize(f.bytes))
	}
	if f.rate > 0 {
		parts = append(parts, byteSize(uint64(f.rate))+"/s")
	}
	if f.eta > 0 {
		parts = append(parts, "ETA "+time.Duration(f.eta).Round(time.Second).String())
	}
	return parts
}

// pack joins parts with sep into lines at most width wide, never splitting a
// part; a part wider than width stands on a line of its own. A line that
// ends early keeps sep's comma, if it has one.
func pack(parts []string, sep string, width int) string {
	var lines []string
	line := ""
	for _, part := range parts {
		switch {
		case line == "":
			line = part
		case len(line)+len(sep)+len(part) <= width:
			line += sep + part
		default:
			lines = append(lines, line+strings.TrimRight(sep, " "))
			line = part
		}
	}
	return strings.Join(append(lines, line), "\n")
}

func progressBar(share, width int) string {
	filled := share * width / 100
	return "[" + strings.Repeat("=", filled) + strings.Repeat(" ", width-filled) + "]"
}

// problemDetail says what went wrong with a job, how long ago, and when it
// is tried again.
func problemDetail(job *controlrpc.JobStatus, now time.Time) string {
	parts := []string{job.GetState()}
	if since := ago(unixTime(job.GetChangedUnixNano()), now); since != "" {
		parts = append(parts, since)
	}
	if attempts := job.GetAttempts(); attempts > 1 {
		parts = append(parts, fmt.Sprintf("%d failed attempts", attempts))
	}
	if next := until(unixTime(job.GetRetryUnixNano()), now); next != "" {
		if next == "due" {
			parts = append(parts, "retry due")
		} else {
			parts = append(parts, "retry "+next)
		}
	}
	detail := strings.Join(parts, ", ")
	if reason := job.GetReason(); reason != "" {
		detail += ": " + reason
	}
	return detail
}

// stateText is a job's state in a dataset row: "ok" for success, "queued"
// for work waiting in a queue, "retrying" for a transfer waiting to try again,
// and a sending transfer's share sent when it knows its total.
func stateText(job *controlrpc.JobStatus) string {
	switch classify(job) {
	case classQueued:
		return "queued"
	case classRunning:
		if share, known := percent(jobFigures(job)); known && job.GetState() == "sending" {
			return fmt.Sprintf("sending %d%%", share)
		}
	case classProblem:
		if job.GetState() == "waiting-retry" {
			return "retrying"
		}
	case classDone:
		if job.GetState() == "succeeded" {
			return "ok"
		}
	}
	return job.GetState()
}

// receiving reports whether a receive is in progress, rather than
// interrupted.
func receiving(receive *controlrpc.ReceiveStatus) bool {
	return receive.GetState() == "receiving"
}

// receiveState is a receive's state in a dataset row: its share received
// when it knows its total.
func receiveState(receive *controlrpc.ReceiveStatus) string {
	if share, known := percent(receiveFigures(receive)); known && receiving(receive) {
		return fmt.Sprintf("receiving %d%%", share)
	}
	return receive.GetState()
}

// receiveLabel names a receive in words: where it goes and, when the sender
// named itself, where it comes from.
func receiveLabel(receive *controlrpc.ReceiveStatus) string {
	if sender := receive.GetSenderHost(); sender != "" {
		return receive.GetDataset() + " <- " + sender
	}
	return receive.GetDataset()
}

// Terminal writes the status display: a summary line, then what needs
// attention, what is running, every listed dataset with its schedule and
// destinations, and the queues. Long text wraps rather than being cut, and
// times are shown relative to opts.Now, in opts.Zone. Structured output stays
// in UTC.
func Terminal(writer io.Writer, snapshot *controlrpc.StatusSnapshot, opts Options) error {
	opts = opts.normalized()
	view, hidden := normalized(snapshot, opts.Statuses)
	now := opts.Now
	if now.IsZero() {
		now = view.Observed
	}
	if now.IsZero() {
		now = time.Now()
	}
	jobs := slices.DeleteFunc(slices.Clone(view.Jobs), func(job *controlrpc.JobStatus) bool { return job.GetState() == "cancelled" })

	var out strings.Builder
	problems, severities := problemRows(view, snapshot.GetDatasets(), jobs, now)
	var running, queued []*controlrpc.JobStatus
	for _, job := range jobs {
		switch classify(job) {
		case classRunning:
			running = append(running, job)
		case classQueued:
			queued = append(queued, job)
		case classDone, classProblem:
			// Finished work shows in the dataset rows, problems above.
		}
	}
	var receives []*controlrpc.ReceiveStatus
	for _, receive := range view.Receives {
		if receiving(receive) {
			receives = append(receives, receive)
		}
	}
	writeSummary(&out, snapshot.GetDatasets(), len(running), len(receives), len(queued), len(problems), now, opts)
	if len(problems) != 0 {
		heading(&out, "PROBLEMS", opts.Color)
		writeTable(&out, nil, problems, opts.Width, func(row int) string {
			if opts.Color {
				return severities[row]
			}
			return ""
		})
	}
	if len(running)+len(receives)+len(queued) != 0 {
		heading(&out, "IN PROGRESS", opts.Color)
		writeTable(&out, nil, progressRows(running, receives, queued, opts.Width), opts.Width, nil)
	}
	writeDatasets(&out, view.Datasets, snapshot.GetDatasets(), jobs, view.Receives, hidden, now, opts)
	writeQueues(&out, view.Queues, opts.Width)
	_, err := io.WriteString(writer, out.String())
	return err
}

func writeSummary(out *strings.Builder, all []*controlrpc.DatasetStatus, running, receiving, queued, problems int, now time.Time, opts Options) {
	active := 0
	for _, dataset := range all {
		if dataset.GetStatus() == discovery.StatusActive {
			active++
		}
	}
	parts := []string{"boomerangz", now.In(opts.Zone).Format("15:04:05 MST"), plural(active, "active dataset", "active datasets")}
	if running != 0 {
		parts = append(parts, fmt.Sprintf("%d running", running))
	}
	if receiving != 0 {
		parts = append(parts, fmt.Sprintf("%d receiving", receiving))
	}
	if queued != 0 {
		parts = append(parts, fmt.Sprintf("%d queued", queued))
	}
	if problems != 0 {
		parts = append(parts, plural(problems, "problem", "problems"))
	}
	// pack keeps each part whole, so "3 active datasets" never splits.
	out.WriteString(pack(parts, "  ", opts.Width) + "\n")
}

// writeText writes a line of prose wrapped to width, continuation lines
// indented.
func writeText(out *strings.Builder, text string, width int) {
	lines := wrap(text, width)
	out.WriteString(lines[0] + "\n")
	if len(lines) > 1 {
		for _, line := range wrap(strings.Join(lines[1:], " "), width-continuation) {
			out.WriteString(strings.Repeat(" ", continuation) + line + "\n")
		}
	}
}

func plural(count int, one, many string) string {
	if count == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", count, many)
}

// problemRows lists everything that needs the operator: destinations whose
// permission check found something missing or could not run, datasets with
// invalid policy or blocked targets, and jobs that failed, are blocked, or
// are waiting to retry, most severe first. A problem is listed whatever the
// dataset filter, since hiding one would hide the reason something is not
// happening. severities holds each row's color: yellow for a retry that may
// yet succeed on its own, red for the rest.
func problemRows(view output, datasets []*controlrpc.DatasetStatus, jobs []*controlrpc.JobStatus, now time.Time) (rows [][]string, severities []string) {
	add := func(severity string, cells ...string) {
		rows = append(rows, cells)
		severities = append(severities, severity)
	}
	for _, destination := range view.Destinations {
		name := destination.GetDestination()
		if destination.GetRemote() {
			name = "remote:" + name
		}
		switch {
		case len(destination.GetMissing()) != 0:
			add(styleRed, name, fmt.Sprintf("%s lacks delegated ZFS permissions on %s: %s", destination.GetAccount(), destination.GetDataset(), strings.Join(destination.GetMissing(), ",")))
		case destination.GetError() != "":
			add(styleRed, name, "permissions not checked: "+destination.GetError())
		}
	}
	sorted := slices.SortedFunc(slices.Values(datasets), func(a, b *controlrpc.DatasetStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	for _, dataset := range sorted {
		if dataset.GetStatus() == discovery.StatusInvalid {
			add(styleRed, dataset.GetName(), "invalid policy; inspect with: boomerangz dataset inspect "+dataset.GetName())
		}
		for _, blocked := range dataset.GetBlocked() {
			add(styleRed, dataset.GetName()+" -> "+blockedName(blocked), "blocked: "+blocked.GetReason())
		}
	}
	var failing []*controlrpc.JobStatus
	for _, job := range jobs {
		if classify(job) == classProblem {
			failing = append(failing, job)
		}
	}
	slices.SortStableFunc(failing, func(a, b *controlrpc.JobStatus) int {
		if rank := problemRank(a.GetState()) - problemRank(b.GetState()); rank != 0 {
			return rank
		}
		return strings.Compare(jobLabel(a), jobLabel(b))
	})
	for _, job := range failing {
		severity := styleRed
		if job.GetState() == "waiting-retry" {
			severity = styleYellow
		}
		add(severity, jobLabel(job), problemDetail(job, now))
	}
	return rows, severities
}

// blockedName names a blocked target: a local destination root as
// configured, a remote as "remote:<name>".
func blockedName(blocked *controlrpc.BlockedTarget) string {
	if blocked.GetRemote() {
		return "remote:" + blocked.GetTarget()
	}
	return blocked.GetTarget()
}

// progressRows lists running jobs, then receives into this host, then queued
// jobs in queue order, each with its state and what there is to say about it.
// A sending transfer or a receive shows its figures, and under them a progress
// bar when it knows its total.
func progressRows(running []*controlrpc.JobStatus, receives []*controlrpc.ReceiveStatus, queued []*controlrpc.JobStatus, width int) [][]string {
	labelWidth := 0
	for _, job := range append(slices.Clone(running), queued...) {
		labelWidth = max(labelWidth, len(jobLabel(job)))
	}
	for _, receive := range receives {
		labelWidth = max(labelWidth, len(receiveLabel(receive)))
	}
	// The detail sits beside the label when it has minWrap or more, and
	// otherwise on its own lines under it; see writeTable.
	room := width - labelWidth - columnGap
	if room < minWrap {
		room = width - continuation
	}
	moving := func(state string, f figures) string {
		detail := pack(append([]string{state}, progress(f)...), ", ", room)
		if share, known := percent(f); known {
			detail += "\n" + progressBar(share, min(room-2, maxBar))
		}
		return detail
	}
	var rows [][]string
	for _, job := range running {
		detail := job.GetState()
		switch {
		case job.GetState() == "sending":
			detail = moving(detail, jobFigures(job))
		case job.GetReason() != "":
			detail += ": " + job.GetReason()
		}
		rows = append(rows, []string{jobLabel(job), detail})
	}
	for _, receive := range receives {
		rows = append(rows, []string{receiveLabel(receive), moving(receive.GetState(), receiveFigures(receive))})
	}
	slices.SortStableFunc(queued, func(a, b *controlrpc.JobStatus) int { return int(a.GetQueuePosition()) - int(b.GetQueuePosition()) })
	for _, job := range queued {
		detail := "queued"
		if position := job.GetQueuePosition(); position > 0 {
			detail += fmt.Sprintf(", position %d of %d", position, job.GetPending())
		}
		rows = append(rows, []string{jobLabel(job), detail})
	}
	return rows
}

const (
	columnGap = 2
	maxBar    = 40
)

// writeDatasets lists the datasets the filter keeps, as `dataset list` does,
// in one grid: each dataset's row carries its next snapshot and the state of
// its latest snapshot job, and under it a row per destination carries the
// state of its latest transfer, so every state lines up in one column. A
// dataset being received into, or holding an interrupted receive, carries
// that receive's state instead; one discovery has not listed yet, as a full
// receive's new dataset is until it completes, has a row of its own as a
// target, when the filter keeps targets.
func writeDatasets(out *strings.Builder, datasets, all []*controlrpc.DatasetStatus, jobs []*controlrpc.JobStatus, receives []*controlrpc.ReceiveStatus, hidden int, now time.Time, opts Options) {
	out.WriteString("\n")
	incoming := map[string]*controlrpc.ReceiveStatus{}
	for _, receive := range receives {
		// The newest receive into a dataset is the one its row shows.
		incoming[receive.GetDataset()] = receive
	}
	listed := map[string]bool{}
	for _, dataset := range all {
		listed[dataset.GetName()] = true
	}
	if keep, _ := discovery.StatusFilter(opts.Statuses); keep(discovery.StatusTarget) {
		datasets = slices.Clone(datasets)
		for name := range incoming {
			if !listed[name] {
				datasets = append(datasets, &controlrpc.DatasetStatus{Name: name, Status: discovery.StatusTarget})
			}
		}
		slices.SortFunc(datasets, func(a, b *controlrpc.DatasetStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	}
	if len(datasets) == 0 {
		if hidden > 0 {
			writeText(out, fmt.Sprintf("No datasets found other than %d inactive; show them with: boomerangz status --status all", hidden), opts.Width)
		} else {
			writeText(out, "No datasets found.", opts.Width)
		}
		return
	}
	snapshots := map[string]*controlrpc.JobStatus{}
	transfers := map[string][]*controlrpc.JobStatus{}
	for _, job := range jobs {
		switch jobKind(job) {
		case "snapshot":
			snapshots[job.GetDataset()] = job
		case "local", "remote":
			if _, ok := destination(job); ok {
				transfers[job.GetDataset()] = append(transfers[job.GetDataset()], job)
			}
		}
	}
	rows := make([][]string, 0, len(datasets))
	for _, dataset := range datasets {
		name := dataset.GetName()
		if dataset.GetRecursive() {
			name += " (recursive)"
		}
		state, since := "-", "-"
		if job, found := snapshots[dataset.GetName()]; found {
			state, since = stateText(job), dash(ago(unixTime(job.GetChangedUnixNano()), now))
		}
		if receive, found := incoming[dataset.GetName()]; found {
			state, since = receiveState(receive), dash(ago(unixTime(receive.GetStartedUnixNano()), now))
		}
		rows = append(rows, []string{dataset.GetStatus(), name, dash(until(unixTime(dataset.GetNextSnapshotUnixNano()), now)), state, since})
		var targets [][]string
		for _, job := range transfers[dataset.GetName()] {
			target, _ := destination(job)
			targets = append(targets, []string{"", "  -> " + target, "", stateText(job), dash(ago(unixTime(job.GetChangedUnixNano()), now))})
		}
		for _, blocked := range dataset.GetBlocked() {
			targets = append(targets, []string{"", "  -> " + blockedName(blocked), "", "blocked", "-"})
		}
		slices.SortFunc(targets, func(a, b []string) int { return strings.Compare(a[1], b[1]) })
		rows = append(rows, targets...)
	}
	writeTable(out, []string{"STATUS", "DATASET", "NEXT SNAPSHOT", "STATE", "SINCE"}, rows, opts.Width, nil)
	switch {
	case hidden == 1:
		out.WriteString("\n")
		writeText(out, "1 inactive dataset hidden; show it with: boomerangz status --status all", opts.Width)
	case hidden > 1:
		out.WriteString("\n")
		writeText(out, fmt.Sprintf("%d inactive datasets hidden; show them with: boomerangz status --status all", hidden), opts.Width)
	}
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// writeQueues summarizes every queue on one line, marking one that is nearly
// full: queues are usually close to empty, so a bar per queue says little.
func writeQueues(out *strings.Builder, queues []*controlrpc.QueueStatus, width int) {
	if len(queues) == 0 {
		return
	}
	parts := make([]string, 0, len(queues))
	for _, queue := range queues {
		part := fmt.Sprintf("%s %d/%d", queue.GetName(), queue.GetPending(), queue.GetCapacity())
		if capacity := queue.GetCapacity(); capacity != 0 && queue.GetPending()*4 >= capacity*3 {
			part += " (nearly full)"
		}
		parts = append(parts, part)
	}
	out.WriteString("\n")
	writeTable(out, nil, [][]string{{"queues", strings.Join(parts, ", ")}}, width, nil)
}

// writeTable lays rows out in columns, each line of a row painted with what
// style returns for it, when style is set. Every cell but a row's last is
// padded to the widest such cell of its column; the row's last cell takes the
// rest of the line and wraps, continuation lines aligned under it, so a short
// row's last cell spans the columns it leaves empty. A cell may hold several
// lines.
//
// No line is ever wider than width, since a terminal would wrap it again and
// throw a redrawn frame out of place. When a row leaves its last cell less
// than minWrap, that cell moves onto its own lines under the row, indented.
// When the padded leading columns alone are wider than width, the table
// stacks: each row's cells run together on one line, wrapped. Nothing is cut:
// a word wider than a line is broken across lines.
func writeTable(out *strings.Builder, header []string, rows [][]string, width int, style func(row int) string) {
	offset := 0
	if header != nil {
		rows = append([][]string{header}, rows...)
		offset = 1
	}
	var widths []int
	for _, row := range rows {
		for index, cell := range row[:len(row)-1] {
			if index == len(widths) {
				widths = append(widths, 0)
			}
			for line := range strings.Lines(cell) {
				widths[index] = max(widths[index], len(strings.TrimSuffix(line, "\n")))
			}
		}
	}
	leading := 0
	for _, column := range widths {
		leading += column + columnGap
	}
	stacked := leading-columnGap > width
	for number, row := range rows {
		painted := ""
		if style != nil && number >= offset {
			painted = style(number - offset)
		}
		var lines []string
		switch {
		case stacked && number < offset:
			// A stacked row labels its own cells from the header.
		case stacked:
			lines = stackRow(row, header, width)
		default:
			lines = alignRow(row, widths, width)
		}
		for _, line := range lines {
			out.WriteString(paint(strings.TrimRight(line, " "), painted, true) + "\n")
		}
	}
}

// continuation is how far a wrapped cell's own lines are indented when they
// cannot sit beside the row.
const continuation = 4

// alignRow lays one row out in the table's columns.
func alignRow(row []string, widths []int, width int) []string {
	indent := 0
	for _, column := range widths[:len(row)-1] {
		indent += column + columnGap
	}
	last := row[len(row)-1]
	beside := width-indent >= minWrap
	cells := make([][]string, len(row))
	height := 0
	for index, cell := range row {
		switch {
		case index < len(row)-1:
			cells[index] = strings.Split(cell, "\n")
		case beside:
			cells[index] = wrap(cell, width-indent)
		}
		height = max(height, len(cells[index]))
	}
	var lines []string
	for line := range height {
		var text strings.Builder
		for index, cell := range cells {
			value := ""
			if line < len(cell) {
				value = cell[line]
			}
			if index < len(cells)-1 {
				text.WriteString(value + strings.Repeat(" ", widths[index]-len(value)+columnGap))
			} else {
				text.WriteString(value)
			}
		}
		lines = append(lines, text.String())
	}
	if !beside && last != "" {
		for _, line := range wrap(last, width-continuation) {
			lines = append(lines, strings.Repeat(" ", continuation)+line)
		}
	}
	return lines
}

// stackRow lays out one row of a table too wide for the terminal: the cells
// that name what the row is about (the first, or the first two of a wider
// row) on one line, and the rest under them, indented and run together, each labelled from header when there
// is one and a cell that says nothing ("-") left out.
func stackRow(row, header []string, width int) []string {
	first, start := row[0], 1
	if len(row) > 2 {
		start = 2
		switch {
		case strings.TrimSpace(first) == "":
			first = row[1]
		case strings.TrimSpace(row[1]) != "":
			first += "  " + row[1]
		}
	}
	indent := len(first) - len(strings.TrimLeft(first, " ")) + continuation
	var rest []string
	for index := start; index < len(row); index++ {
		value := strings.Join(strings.Fields(row[index]), " ")
		if value == "" || (header != nil && value == "-") {
			continue
		}
		if header != nil && index < len(header) {
			value = strings.ToLower(header[index]) + " " + value
		}
		rest = append(rest, value)
	}
	lines := wrap(first, width)
	if len(rest) != 0 {
		for _, line := range wrap(strings.Join(rest, ", "), width-indent) {
			lines = append(lines, strings.Repeat(" ", indent)+line)
		}
	}
	return lines
}

// minWrap is the narrowest the last column of a table wraps to, however
// little room the line leaves it.
const minWrap = 20

// wrap breaks text into lines at most width wide, at spaces, keeping any
// line breaks it already has. A line that already fits is kept as it is,
// spacing and all; a word wider than width is broken across lines.
func wrap(text string, width int) []string {
	var lines []string
	for paragraph := range strings.SplitSeq(text, "\n") {
		if len(paragraph) <= width {
			lines = append(lines, paragraph)
			continue
		}
		line := ""
		for _, word := range strings.Fields(paragraph) {
			for len(word) > width {
				if line != "" {
					lines = append(lines, line)
					line = ""
				}
				// Break on a character boundary, never inside one.
				at := width
				for at > 1 && !utf8.RuneStart(word[at]) {
					at--
				}
				lines = append(lines, word[:at])
				word = word[at:]
			}
			switch {
			case line == "":
				line = word
			case len(line)+1+len(word) <= width:
				line += " " + word
			default:
				lines = append(lines, line)
				line = word
			}
		}
		lines = append(lines, line)
	}
	return lines
}

// Tail holds the most recent transitions a watch has received, oldest first.
type Tail struct {
	limit  int
	events []*controlrpc.JobStatus
}

// NewTail returns a Tail that keeps at most limit transitions.
func NewTail(limit int) *Tail {
	return &Tail{limit: max(limit, 0)}
}

// Add appends transitions in the order received, discarding the oldest beyond
// the limit.
func (t *Tail) Add(transitions []*controlrpc.JobStatus) {
	t.events = append(t.events, transitions...)
	if excess := len(t.events) - t.limit; excess > 0 {
		t.events = slices.Delete(t.events, 0, excess)
	}
}

// Terminal writes the held transitions under a heading, oldest first, with
// times of day in opts.Zone, dated when not on now's day. Details wrap rather
// than being cut. It writes nothing when none are held.
func (t *Tail) Terminal(writer io.Writer, now time.Time, opts Options) error {
	if len(t.events) == 0 {
		return nil
	}
	opts = opts.normalized()
	today := now.In(opts.Zone).Format(time.DateOnly)
	rows := make([][]string, 0, len(t.events))
	for _, event := range t.events {
		at := "-"
		if changed := unixTime(event.GetChangedUnixNano()); !changed.IsZero() {
			local := changed.In(opts.Zone)
			at = local.Format(time.TimeOnly)
			if local.Format(time.DateOnly) != today {
				at = local.Format("Jan 02 15:04:05")
			}
		}
		detail := identitySummary(event)
		if reason := event.GetReason(); reason != "" {
			detail = strings.TrimSpace(detail + " " + reason)
		}
		rows = append(rows, []string{at, jobLabel(event), event.GetState(), detail})
	}
	var out strings.Builder
	heading(&out, "RECENT TRANSITIONS", opts.Color)
	writeTable(&out, nil, rows, opts.Width, nil)
	_, err := io.WriteString(writer, out.String())
	return err
}

// identitySummary names what a transition acted on, as the key=value pairs
// it sets, in a fixed order. A snapshot of the job's own dataset is shown
// from its "@", since the dataset is already in the label.
func identitySummary(event *controlrpc.JobStatus) string {
	short := func(name string) string {
		if suffix, ok := strings.CutPrefix(name, event.GetDataset()+"@"); ok && event.GetDataset() != "" {
			return "@" + suffix
		}
		return name
	}
	var parts []string
	for _, field := range []struct{ key, value string }{
		{"snapshot", short(event.GetSnapshot())},
		{"base", short(event.GetBase())},
		{"mode", event.GetMode()},
		{"destination", event.GetDestination()},
		{"marker", event.GetMarker()},
	} {
		if field.value != "" {
			parts = append(parts, field.key+"="+field.value)
		}
	}
	if count := event.GetDestroyedCount(); count > 0 {
		parts = append(parts, fmt.Sprintf("destroyed=%d", count))
	}
	if generation := event.GetConfigGeneration(); generation > 0 {
		parts = append(parts, fmt.Sprintf("config_generation=%d", generation))
	}
	return strings.Join(parts, " ")
}

// Fit keeps a frame within height terminal lines, so redrawing it never
// scrolls the screen: a taller frame keeps its top lines and ends with one
// saying how many more there are. A height under two leaves the frame as it
// is, since there is no room to say anything.
func Fit(frame []byte, height int) []byte {
	lines := bytes.SplitAfter(frame, []byte("\n"))
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if height < 2 || len(lines) <= height {
		return frame
	}
	kept := height - 1
	fitted := bytes.Join(lines[:kept], nil)
	return fmt.Appendf(fitted, "... %d more lines\n", len(lines)-kept)
}

// Redraw replaces the previous frame on a terminal with frame in a single
// write. Clearing the screen first and then writing the frame line by line
// lets the terminal paint the blank screen between the two, which flickers
// on every update. Instead the cursor returns home and each line is erased
// as it is overwritten, then whatever the previous frame left below this one
// is erased. Each line is erased before it is written rather than after,
// because a line exactly the terminal width leaves the cursor on its last
// column, where erasing to the end of the line would remove that column's
// character.
func Redraw(writer io.Writer, frame []byte) error {
	var screen bytes.Buffer
	screen.Grow(len(frame) + 64)
	screen.WriteString("\x1b[H")
	for line := range bytes.Lines(frame) {
		screen.WriteString("\x1b[2K")
		screen.Write(line)
	}
	screen.WriteString("\x1b[J")
	_, err := writer.Write(screen.Bytes())
	return err
}
