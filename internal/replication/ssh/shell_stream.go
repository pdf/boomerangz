package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ShellStream sends a local ZFS stream through the shared gRPC receive service.
type ShellStream struct {
	shell  *Shell
	sender zfs.CommandFactory
}

type streamProgress struct {
	bytes    uint64
	estimate zfs.Estimate
	start    time.Time
	last     time.Time
	report   func(zfs.Progress)
}

func (p *streamProgress) emit(completed bool) zfs.Progress {
	now := time.Now()
	result := zfs.Progress{Bytes: p.bytes, Estimate: p.estimate, Completed: completed}
	if elapsed := now.Sub(p.start).Seconds(); elapsed > 0 {
		result.BytesPerSecond = float64(p.bytes) / elapsed
	}
	if p.estimate.Known && p.estimate.Bytes >= p.bytes && result.BytesPerSecond > 0 {
		result.ETA = durationPointer(time.Duration(float64(p.estimate.Bytes-p.bytes) / result.BytesPerSecond * float64(time.Second)))
	}
	p.last = now
	if p.report != nil {
		p.report(result)
	}
	return result
}

func durationPointer(value time.Duration) *time.Duration { return &value }

// Run executes one bounded sender and gRPC client-streaming receive.
func (s *ShellStream) Run(ctx context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	args, err := zfs.SendArguments(send, false)
	if err != nil {
		return zfs.Progress{}, err
	}
	if _, err := zfs.ReceiveArguments(receive); err != nil {
		return zfs.Progress{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.shell.remote.Receive(runCtx)
	if err != nil {
		return zfs.Progress{}, normalizeRPCError(err)
	}
	options := &remoterpc.ReceiveOptions{Root: receive.Root, Discard: string(receive.Discard), Set: receive.Set, Exclude: receive.Exclude}
	if err := stream.Send(&remoterpc.ReceiveRequest{Options: options}); err != nil {
		return zfs.Progress{}, normalizeRPCError(err)
	}
	sender := s.sender(runCtx, args)
	sender.WaitDelay = 2 * time.Second
	var diagnostics boundedOutput
	sender.Stderr = &diagnostics
	output, err := sender.StdoutPipe()
	if err != nil {
		return zfs.Progress{}, err
	}
	if err := sender.Start(); err != nil {
		_ = output.Close()
		return zfs.Progress{}, err
	}
	progress := streamProgress{estimate: estimate, start: time.Now(), last: time.Now(), report: report}
	progress.emit(false)
	buffer := make([]byte, 128*1024)
	var copyErr error
	for {
		n, readErr := output.Read(buffer)
		if n > 0 {
			if sendErr := stream.Send(&remoterpc.ReceiveRequest{Data: buffer[:n]}); sendErr != nil {
				copyErr = normalizeRPCError(sendErr)
				cancel()
				break
			}
			progress.bytes += uint64(n)
			if time.Since(progress.last) >= 250*time.Millisecond {
				progress.emit(false)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			copyErr = readErr
			cancel()
			break
		}
	}
	senderErr := sender.Wait()
	response, receiveErr := stream.CloseAndRecv()
	receiveErr = normalizeRPCError(receiveErr)
	if response != nil && response.GetBytes() != progress.bytes {
		receiveErr = errors.Join(receiveErr, fmt.Errorf("remote receive byte count differs from sent stream"))
	}
	err = errors.Join(copyErr, senderErr, receiveErr, ctx.Err())
	result := progress.emit(err == nil)
	if err != nil {
		return result, fmt.Errorf("SSH-shell stream: %w; sender: %s", err, diagnostics.buffer.String())
	}
	return result, nil
}
