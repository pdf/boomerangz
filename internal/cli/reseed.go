package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

func runReseed(ctx context.Context, out io.Writer, cfg config.Config, reader discovery.Reader, executor zfs.Executor, dataset, target string, apply bool) (resultErr error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	if target == "" {
		return fmt.Errorf("configured target is required")
	}
	if apply {
		lock, err := lifecycleLock(cfg)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	if err := (standaloneSafety{socket: cfg.Paths.SocketPath}).Quiescent(ctx, []string{dataset}); err != nil {
		return err
	}
	installation, err := identity.LoadOrCreate(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	remoteNames := make([]string, 0, len(cfg.Remotes))
	for name := range cfg.Remotes {
		remoteNames = append(remoteNames, name)
	}
	scanner, err := discovery.New(reader, discovery.Options{Remotes: remoteNames})
	if err != nil {
		return err
	}
	generation, err := scanner.Scan(ctx, []string{dataset})
	if err != nil {
		return err
	}
	entry, exists := generation.Inspect(dataset)
	if !exists || !entry.Inspected {
		return fmt.Errorf("dataset policy inspection is incomplete")
	}
	if err := entry.ErrCovered(); err != nil {
		return err
	}
	if err := lifecycle.ActiveRoot(entry.Policy, dataset); err != nil {
		return err
	}
	request, local, err := reseedRequest(generation, entry, target)
	if err != nil {
		return err
	}
	destination := zfs.ReseedExecutor(nil)
	closeTarget := func() error { return nil }
	if local {
		request.DestinationRoot = target
		var ok bool
		destination, ok = executor.(zfs.ReseedExecutor)
		if !ok {
			return fmt.Errorf("local destination reseed operations are unavailable")
		}
	} else {
		checker, checkerErr := daemon.NewTargetChecker(executor, cfg.Remotes, cfg.Paths.CredentialsDir)
		if checkerErr != nil {
			return checkerErr
		}
		endpoint, openErr := checker.OpenConfigured(ctx, target)
		if openErr != nil {
			return openErr
		}
		closeTarget = endpoint.Close
		defer func() { resultErr = errors.Join(resultErr, closeTarget()) }()
		var ok bool
		destination, ok = endpoint.Executor.(zfs.ReseedExecutor)
		if !ok {
			return fmt.Errorf("remote destination does not support reseed operations")
		}
		request.DestinationRoot = endpoint.Root
		request.Transport = endpoint.Transport
		request.RemoteName = target
		request.CanonicalTarget = endpoint.CanonicalTarget
	}
	service, err := transfer.NewReseedService(executor, destination, installation)
	if err != nil {
		return err
	}
	var plan transfer.ReseedPlan
	if apply {
		plan, err = service.Apply(ctx, request)
	} else {
		plan, err = service.Plan(ctx, request)
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return errors.Join(err, encoder.Encode(plan))
}

// reseedRequest resolves target for an actor root. A target belongs to the root
// when the root or any of its members sends there, since the binding and every
// stream's proof for it live on the root; the request carries the members that
// send there so preflight covers their receives too.
func reseedRequest(generation *discovery.Generation, entry discovery.Entry, target string) (transfer.Request, bool, error) {
	dataset := entry.Dataset.Name
	request := transfer.Request{Source: dataset, Policy: entry.Policy}
	senders := []policy.Effective{entry.Policy}
	if !entry.Policy.Send.Replicate {
		for _, name := range entry.Members {
			member, found := generation.Inspect(name)
			if !found || !member.Member || name == dataset {
				continue
			}
			senders = append(senders, member.Policy)
			if slices.Contains(member.Policy.Local, target) || slices.Contains(member.Policy.Remote, target) {
				request.Members = append(request.Members, transfer.Member{Dataset: name, Policy: member.Policy})
			}
		}
	}
	local := slices.ContainsFunc(senders, func(p policy.Effective) bool { return slices.Contains(p.Local, target) })
	remote := slices.ContainsFunc(senders, func(p policy.Effective) bool { return slices.Contains(p.Remote, target) })
	if local == remote {
		return request, false, fmt.Errorf("target %q must identify exactly one effective local destination or remote", target)
	}
	return request, local, nil
}
