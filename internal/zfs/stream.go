package zfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

// SendOptions describes a snapshot stream, never arbitrary command flags.
type SendOptions struct {
	// Source is used to validate local stream topology. It is normally derived
	// from Snapshot, but is required when resuming from an opaque token.
	Source        string
	Snapshot      string
	ResumeToken   string
	Base          string
	Intermediates bool
	Recursive     bool
	LargeBlocks   bool
	Compressed    bool
	EmbeddedData  bool
	Raw           bool
	Properties    bool
}

// ReceiveDiscard controls the native receive path mapping.
type ReceiveDiscard string

// Receive path modes match the public discard property.
const (
	ReceiveExact     ReceiveDiscard = "none"
	ReceiveDropFirst ReceiveDiscard = "first"
	ReceiveDropAll   ReceiveDiscard = "all"
)

// ReceiveOptions always use an unmounted, resumable receive without force.
type ReceiveOptions struct {
	Root    string
	Discard ReceiveDiscard
	Set     map[string]string
	Exclude []string
}

// MapReceiveDataset predicts the exact root created/updated by native mapping.
func MapReceiveDataset(source, root string, discard ReceiveDiscard) (string, error) {
	if err := validateDataset(source); err != nil {
		return "", err
	}
	if err := validateDataset(root); err != nil {
		return "", err
	}
	switch discard {
	case ReceiveExact:
		return root, nil
	case ReceiveDropFirst:
		_, suffix, found := strings.Cut(source, "/")
		if !found {
			return root, nil
		}
		return root + "/" + suffix, nil
	case ReceiveDropAll:
		return root + "/" + source[strings.LastIndexByte(source, '/')+1:], nil
	default:
		return "", fmt.Errorf("invalid receive discard mode")
	}
}

func sendArgs(options SendOptions, estimate bool) ([]string, error) {
	if options.ResumeToken != "" {
		if err := validateResumeToken(options.ResumeToken); err != nil {
			return nil, err
		}
		if options.Snapshot != "" || options.Base != "" || options.Intermediates || options.Recursive || options.LargeBlocks || options.Compressed || options.EmbeddedData || options.Raw || options.Properties {
			return nil, fmt.Errorf("resume token cannot be combined with snapshot send options")
		}
		args := []string{"send"}
		if estimate {
			args = append(args, "-nP")
		}
		return append(args, "-t", options.ResumeToken), nil
	}
	if err := validateSnapshot(options.Snapshot); err != nil {
		return nil, err
	}
	args := []string{"send"}
	if estimate {
		args = append(args, "-nP")
	}
	for _, flag := range []struct {
		on  bool
		arg string
	}{{options.LargeBlocks, "-L"}, {options.Compressed, "-c"}, {options.EmbeddedData, "-e"}, {options.Raw, "-w"}, {options.Properties, "-p"}, {options.Recursive, "-R"}} {
		if flag.on {
			args = append(args, flag.arg)
		}
	}
	if options.Base != "" {
		if err := validateObject(options.Base); err != nil {
			return nil, err
		}
		snapshot := strings.Contains(options.Base, "@")
		bookmark := strings.Contains(options.Base, "#")
		if !snapshot && !bookmark {
			return nil, fmt.Errorf("incremental base must be a snapshot or bookmark")
		}
		if bookmark && (options.Intermediates || options.Recursive) {
			return nil, fmt.Errorf("intermediate/recursive sends require a snapshot base")
		}
		source, _, _ := strings.Cut(options.Snapshot, "@")
		base := strings.FieldsFunc(options.Base, func(r rune) bool { return r == '@' || r == '#' })[0]
		if source != base || options.Base == options.Snapshot {
			return nil, fmt.Errorf("incremental base must be a distinct object on the source dataset")
		}
		flag := "-i"
		if options.Intermediates {
			flag = "-I"
		}
		args = append(args, flag, options.Base)
	}
	return append(args, options.Snapshot), nil
}

func validateResumeToken(token string) error {
	if len(token) > 1024*1024 || strings.HasPrefix(token, "-") {
		return fmt.Errorf("invalid receive resume token")
	}
	for _, r := range token {
		if r < 0x21 || r > 0x7e {
			return fmt.Errorf("invalid receive resume token")
		}
	}
	return nil
}

// SendArguments returns the validated argv for a zfs send operation.
func SendArguments(options SendOptions, estimate bool) ([]string, error) {
	return sendArgs(options, estimate)
}

func streamProperty(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != ':' && r != '_' && r != '-' && r != '.' {
			return false
		}
	}
	return true
}

func receiveArgs(options ReceiveOptions) ([]string, error) {
	if err := validateDataset(options.Root); err != nil {
		return nil, err
	}
	args := []string{"receive", "-u", "-s"}
	switch options.Discard {
	case ReceiveExact:
	case ReceiveDropFirst:
		args = append(args, "-d")
	case ReceiveDropAll:
		args = append(args, "-e")
	default:
		return nil, fmt.Errorf("invalid receive discard mode")
	}
	var keys []string
	for key := range options.Set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if !streamProperty(key) || strings.HasPrefix(key, propertyNamespace) || strings.ContainsRune(options.Set[key], 0) {
			return nil, fmt.Errorf("invalid or reserved receive override %q", key)
		}
		args = append(args, "-o", key+"="+options.Set[key])
	}
	excluded := slices.Clone(options.Exclude)
	slices.Sort(excluded)
	excluded = slices.Compact(excluded)
	for _, key := range excluded {
		if !streamProperty(key) {
			return nil, fmt.Errorf("invalid receive exclusion %q", key)
		}
		if _, set := options.Set[key]; set {
			continue
		}
		args = append(args, "-x", key)
	}
	return append(args, options.Root), nil
}

// ReceiveArguments returns the validated argv for a zfs receive operation.
func ReceiveArguments(options ReceiveOptions) ([]string, error) {
	return receiveArgs(options)
}

// Estimate is explicitly unknown when the installed ZFS cannot estimate a send.
type Estimate struct {
	Known  bool   `json:"known"`
	Bytes  uint64 `json:"bytes"`
	Reason string `json:"reason,omitempty"`
}

// EstimateSend performs a bounded dry-run query; failure does not invent a size.
func (d *Direct) EstimateSend(ctx context.Context, options SendOptions) (Estimate, error) {
	args, err := sendArgs(options, true)
	if err != nil {
		return Estimate{}, err
	}
	output, err := d.runner.Run(ctx, args...)
	if err != nil {
		if ctx.Err() != nil {
			return Estimate{}, ctx.Err()
		}
		return Estimate{Reason: "send estimate unavailable"}, nil
	}
	estimate := Estimate{Reason: "send estimate unavailable"}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "size" {
			n, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil || estimate.Known {
				return Estimate{}, fmt.Errorf("invalid ZFS send estimate output")
			}
			estimate = Estimate{Known: true, Bytes: n}
		}
	}
	return estimate, nil
}

// Progress counts stream bytes accepted by the receiver pipe, not verified data.
// Completed only means the subprocess pipeline exited successfully; callers must
// still verify snapshot GUIDs. ETA is absent when size/rate cannot support it.
type Progress struct {
	Bytes          uint64         `json:"bytes"`
	Estimate       Estimate       `json:"estimate"`
	BytesPerSecond float64        `json:"bytes_per_second"`
	ETA            *time.Duration `json:"eta,omitempty"`
	Completed      bool           `json:"completed"`
}

type countWriter struct {
	io.Writer
	bytes       uint64
	start, last time.Time
	estimate    Estimate
	report      func(Progress)
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes += uint64(n)
	if time.Since(w.last) >= 250*time.Millisecond {
		w.emit(false)
	}
	return n, err
}
func (w *countWriter) emit(completed bool) Progress {
	now := time.Now()
	p := Progress{Bytes: w.bytes, Estimate: w.estimate, Completed: completed}
	if elapsed := now.Sub(w.start).Seconds(); elapsed > 0 {
		p.BytesPerSecond = float64(w.bytes) / elapsed
	}
	if w.estimate.Known && w.estimate.Bytes >= w.bytes && p.BytesPerSecond > 0 {
		seconds := float64(w.estimate.Bytes-w.bytes) / p.BytesPerSecond
		if seconds < float64(time.Duration(1<<63-1))/float64(time.Second) {
			eta := time.Duration(seconds * float64(time.Second))
			p.ETA = &eta
		}
	}
	w.last = now
	if w.report != nil {
		w.report(p)
	}
	return p
}

// diagnosticBuffer drains stderr while retaining only a bounded prefix.
type diagnosticBuffer struct {
	data      []byte
	truncated bool
}

func (b *diagnosticBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 64*1024 - len(b.data)
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	b.data = append(b.data, p...)
	return n, nil
}
func (b *diagnosticBuffer) String() string {
	value := strings.TrimSpace(string(b.data))
	if b.truncated {
		value += " [truncated]"
	}
	return value
}

// LocalStream runs a shell-free, bounded-memory local stream. Both processes are
// reaped on every path; process errors and copy errors are reported separately.
// Progress callbacks must return promptly and are called serially.
type LocalStream struct {
	command func(context.Context, ...string) *exec.Cmd
}

// CommandFactory constructs one process from an already validated argument
// vector. Transport packages use it to add a constrained process boundary.
type CommandFactory func(context.Context, []string) *exec.Cmd

// NewLocalStream selects an explicit ZFS executable, independently of query APIs.
func NewLocalStream(path string) (*LocalStream, error) {
	if path == "" {
		return nil, fmt.Errorf("ZFS executable is required")
	}
	return &LocalStream{command: func(ctx context.Context, args ...string) *exec.Cmd { return exec.CommandContext(ctx, path, args...) }}, nil
}

// Run copies one validated local ZFS send stream into a validated receive process.
func (s *LocalStream) Run(ctx context.Context, send SendOptions, receive ReceiveOptions, estimate Estimate, report func(Progress)) (Progress, error) {
	source := send.Source
	if source == "" {
		source, _, _ = strings.Cut(send.Snapshot, "@")
	}
	if err := validateDataset(source); err != nil {
		return Progress{}, err
	}
	target, err := MapReceiveDataset(source, receive.Root, receive.Discard)
	if err != nil {
		return Progress{}, err
	}
	if source == target || strings.HasPrefix(source, target+"/") || strings.HasPrefix(target, source+"/") {
		return Progress{}, fmt.Errorf("source and destination scopes overlap")
	}
	return RunPipeline(ctx, send, receive, estimate, report,
		func(ctx context.Context, args []string) *exec.Cmd { return s.command(ctx, args...) },
		func(ctx context.Context, args []string) *exec.Cmd { return s.command(ctx, args...) },
	)
}

// RunPipeline validates both ZFS operations and copies a bounded stream between
// transport-owned sender and receiver processes.
func RunPipeline(ctx context.Context, send SendOptions, receive ReceiveOptions, estimate Estimate, report func(Progress), senderCommand, receiverCommand CommandFactory) (Progress, error) {
	if senderCommand == nil || receiverCommand == nil {
		return Progress{}, fmt.Errorf("sender and receiver commands are required")
	}
	sendArg, err := sendArgs(send, false)
	if err != nil {
		return Progress{}, err
	}
	receiveArg, err := receiveArgs(receive)
	if err != nil {
		return Progress{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sender, receiver := senderCommand(runCtx, sendArg), receiverCommand(runCtx, receiveArg)
	sender.WaitDelay = 2 * time.Second
	receiver.WaitDelay = 2 * time.Second
	var sendLog, receiveLog diagnosticBuffer
	sender.Stderr = &sendLog
	receiver.Stderr = &receiveLog
	output, err := sender.StdoutPipe()
	if err != nil {
		return Progress{}, err
	}
	input, err := receiver.StdinPipe()
	if err != nil {
		return Progress{}, errors.Join(err, output.Close())
	}
	if err := receiver.Start(); err != nil {
		return Progress{}, errors.Join(err, output.Close(), input.Close())
	}
	if err := sender.Start(); err != nil {
		cancel()
		return Progress{}, errors.Join(err, output.Close(), input.Close(), receiver.Wait())
	}
	received := make(chan error, 1)
	go func() {
		err := receiver.Wait()
		if err != nil {
			cancel()
		}
		received <- err
	}()
	writer := countWriter{Writer: input, start: time.Now(), last: time.Now(), estimate: estimate, report: report}
	writer.emit(false)
	_, copyErr := io.CopyBuffer(&writer, output, make([]byte, 128*1024))
	closeErr := input.Close()
	if copyErr != nil {
		cancel()
	}
	sendErr := sender.Wait()
	if sendErr != nil {
		cancel()
	}
	receiveErr := <-received
	err = errors.Join(copyErr, closeErr, sendErr, receiveErr, ctx.Err())
	result := writer.emit(err == nil)
	if err != nil {
		return result, fmt.Errorf("stream pipeline: %w; sender: %s; receiver: %s", err, sendLog.String(), receiveLog.String())
	}
	return result, nil
}
