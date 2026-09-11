//go:build integration

package transfer_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationnative "github.com/pdf/boomerangz/internal/replication/native"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	// The interrupted-receive phase cuts the send itself at an exact byte
	// offset. Every transport spawns its sender as
	// exec.Command(zfsPath, "send", ...) and only the receiving half differs,
	// so pointing zfsPath at a wrapper that truncates a send - and execs the
	// real zfs for everything else - puts the fault on the transfer path for
	// all four of them, at a byte count the test chose.
	//
	// Nothing else reaches that far in. The progress callback is the only
	// hook the Stream interface exposes, and both RunPipeline and the RPC
	// stream emit one report as the copy opens and then sample no more often
	// than every 250ms: a local send of this payload moved 179MB inside that
	// first window in the guest, so a byte threshold there would be a cut
	// wherever the sampling happened to land, and none at all on a faster
	// host.
	parityResumeSeedMiB = 32
	parityTruncateBytes = 4 << 20
)

// parityTarget is one transport bound to one destination root. The root is
// part of the canonical target identity, so a target is per-root rather than
// per-transport: two roots reached over one connection would collide in the
// source-side binding.
type parityTarget struct {
	engine *transfer.Local
	// destination is the executor the engine reconciles the receiving side
	// through: the transport's own for a remote, the local one for local.
	destination zfs.Executor
	transport   string
	remote      string
	canonical   string
	binding     string
	root        string
}

// target is the identity the source-side binding and recovery records are
// keyed by. The engine derives it the same way for a local destination.
func (t parityTarget) target() string {
	if t.canonical != "" {
		return t.canonical
	}
	return "local:" + t.root
}

// TestGuestTransportParity runs the behaviour table the local engine is tested
// for against every transport boomerangz can replicate over.
//
// TestGuestSSHTransfer proves one full bootstrap per remote mode. Everything
// after a bootstrap - incremental -i and -I, a bookmark base, resuming an
// interrupted receive, refusing an unrelated destination - goes through a
// different zfs.Executor and a different Stream for each transport, and has
// only ever been exercised locally. local is included as the control arm: the
// same table, the same source shape, the same assertions, so a difference is
// attributable to the transport rather than to the test.
//
// Destination state is inspected with the local executor throughout. The guest
// is both source and destination host, so that reads the same pool the remote
// side wrote; using the transport's own executor would let a broken remote
// view agree with itself.
func TestGuestTransportParity(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_REMOTE_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_REMOTE_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
	}
	key := os.Getenv("BOOMERANGZ_REMOTE_GUEST_KEY")
	shellPath := os.Getenv("BOOMERANGZ_REMOTE_GUEST_CLI")
	if !filepath.IsAbs(key) || !filepath.IsAbs(shellPath) {
		t.Fatal("guest SSH key and boomerangz executable paths are required")
	}
	directUser := os.Getenv("BOOMERANGZ_REMOTE_DIRECT_SSH_USER")
	shellUser := os.Getenv("BOOMERANGZ_REMOTE_GUEST_USER")
	if directUser == "" || shellUser == "" {
		t.Fatal("direct and restricted SSH users are required")
	}
	sourceDevice := os.Getenv("BOOMERANGZ_INTEGRATION_SOURCE_DEVICE")
	destinationDevice := os.Getenv("BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE")
	if sourceDevice == "" || destinationDevice == "" {
		t.Fatal("source and destination test devices are required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, sourceDevice)
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, destinationDevice)
	if err != nil {
		t.Fatal(err)
	}
	// Takes the running *testing.T rather than closing over the parent's: a
	// helper bound to the parent reports a phase's failure against the whole
	// test, and this test has fourteen phases whose names are the diagnosis.
	command := func(t *testing.T, args ...string) string {
		t.Helper()
		out, cmdErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if cmdErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], cmdErr, out)
		}
		return strings.TrimSpace(string(out))
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().UTC().Format("150405")
	container := func(mode string) string {
		return destinationPool + "/data/parity-" + mode + "-" + suffix
	}

	// One native listener serves every parity root. The RPC server scopes each
	// operation with scope.Inside against its allowed roots, so the shared
	// destination container covers all of them, while each connection still
	// carries its own root and therefore its own canonical target.
	pkiDir := t.TempDir()
	nativeConfig := config.Defaults()
	nativeConfig.Paths.SocketPath = filepath.Join(pkiDir, "control.sock")
	nativeConfig.Paths.IdentityDir = filepath.Join(pkiDir, "identity")
	listenerConfig := config.ListenerConfig{
		Network:           "tcp",
		Address:           "127.0.0.1:0",
		AdvertisedAddress: "localhost:7443",
		AuthMode:          "token",
		ReplicationRoots:  []string{destinationPool + "/data"},
	}
	nativeConfig.Listeners["replication"] = listenerConfig
	server, err := control.StartServerWithReplication(nativeConfig, nil, direct, "zfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	addresses := server.Addresses("tcp")
	if len(addresses) != 1 {
		t.Fatalf("native listener addresses=%v", addresses)
	}
	_, port, err := net.SplitHostPort(addresses[0])
	if err != nil {
		t.Fatal(err)
	}
	listenerConfig.AdvertisedAddress = net.JoinHostPort("localhost", port)
	store, err := control.NewTokenStore(nativeConfig.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := control.CreateListenerPairing(store, nativeConfig.Paths.IdentityDir, "replication", listenerConfig, "", "", []string{"replicate"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	openSSH := func(t *testing.T, mode, user, root, zfsPath string) parityTarget {
		t.Helper()
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{
			Host: "127.0.0.1", Port: 22, User: user, Root: root,
			IdentityFile: key, ShellPath: shellPath, ConnectTimeout: 5 * time.Second,
		})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		endpoint, endpointErr := replicationssh.OpenEndpoint(t.Context(), client, zfsPath, mode)
		if endpointErr != nil {
			t.Fatal(endpointErr)
		}
		t.Cleanup(func() {
			if closeErr := endpoint.Close(); closeErr != nil {
				t.Errorf("close %s endpoint for %s: %v", mode, root, closeErr)
			}
		})
		if endpoint.Mode != mode {
			t.Fatalf("endpoint negotiated mode %q, want %q", endpoint.Mode, mode)
		}
		engine, engineErr := transfer.NewRemote(direct, endpoint.Executor, endpoint.Stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		return parityTarget{engine: engine, destination: endpoint.Executor, transport: "ssh", remote: "home", canonical: client.CanonicalTarget(), binding: "ssh", root: root}
	}
	openNative := func(t *testing.T, root, zfsPath string) parityTarget {
		t.Helper()
		endpoint, openErr := replicationnative.Open(t.Context(), bundle, root, zfsPath)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() {
			if closeErr := endpoint.Close(); closeErr != nil {
				t.Errorf("close native endpoint for %s: %v", root, closeErr)
			}
		})
		engine, engineErr := transfer.NewRemote(direct, endpoint.Executor(), endpoint.Stream(), installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		return parityTarget{engine: engine, destination: endpoint.Executor(), transport: "native", remote: "home", canonical: endpoint.CanonicalTarget(), binding: "native", root: root}
	}
	openLocal := func(t *testing.T, root, zfsPath string) parityTarget {
		t.Helper()
		// LocalStream uses one executable for both halves of the pipeline, so
		// the wrapper has to pass a receive through untouched; the remote
		// transports never reach it with anything but a send.
		stream, streamErr := zfs.NewLocalStream(zfsPath)
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		engine, engineErr := transfer.NewLocal(direct, stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		return parityTarget{engine: engine, destination: direct, binding: "local", root: root}
	}

	// request resolves the source's policy from the pool on every call, the
	// way the daemon does, so a phase that changes a property is reflected in
	// the next plan without the test restating it.
	request := func(t *testing.T, target parityTarget, source string, kind zfs.DatasetType, snapshot string) transfer.Request {
		t.Helper()
		rows, rowErr := direct.GetStoredProperties(t.Context(), []string{source})
		if rowErr != nil {
			t.Fatal(rowErr)
		}
		effective := policy.Resolve(zfs.Dataset{Name: source, Type: kind, EncryptionRoot: "-"}, nil, rows, map[string]struct{}{"home": {}})
		return transfer.Request{
			Source:          source,
			DestinationRoot: target.root,
			Snapshot:        snapshot,
			Policy:          effective,
			Transport:       target.transport,
			RemoteName:      target.remote,
			CanonicalTarget: target.canonical,
		}
	}

	// The truncating sender. Every Stream implementation spawns its send as
	// exec.Command(zfsPath, args...), so this is the one seam that reaches
	// the transfer path of all four transports; head closes the pipe at an
	// exact offset, which is the same fault delegated-matrix.sh and
	// TestGuestInterruptedTransferRecovery inject, applied from the sending
	// side so it does not depend on owning the receiver.
	truncatingZFS := filepath.Join(t.TempDir(), "zfs-truncating-send")
	wrapper := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "send" ]; then
	zfs "$@" | head -c %d
	exit 1
fi
exec zfs "$@"
`, parityTruncateBytes)
	if err := os.WriteFile(truncatingZFS, []byte(wrapper), 0755); err != nil {
		t.Fatal(err)
	}

	// One payload for every transport's resume phase, and for nothing else.
	// Each transport receives it into its own root under its own canonical
	// target, so the only thing shared is the bytes - four copies of the same
	// seed would cost a stage's worth of writes without proving anything the
	// transports do not already have in common. It only has to outrun the
	// truncation point; the rest is what the resume then carries. Registered
	// here, at the scope that declares it, so it outlives the phases using it.
	resumeSource := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "parity-resume"), 256, parityResumeSeedMiB)
	command(t, "set",
		policy.Namespace+"enabled=on",
		policy.Namespace+"incremental=latest",
		policy.Namespace+"remote=home",
		policy.Namespace+"local="+container("local")+"/resume",
		resumeSource)
	resumeMetadata, err := snapshots.CreateSnapshot(t.Context(), resumeSource, false, time.Now().UTC(),
		request(t, parityTarget{root: container("local") + "/resume"}, resumeSource, zfs.Volume, "").Policy)
	if err != nil {
		t.Fatal(err)
	}

	for _, transport := range []struct {
		name string
		open func(t *testing.T, root, zfsPath string) parityTarget
	}{
		{"local", openLocal},
		{"ssh-direct", func(t *testing.T, root, zfsPath string) parityTarget {
			return openSSH(t, "direct", directUser, root, zfsPath)
		}},
		{"ssh-shell", func(t *testing.T, root, zfsPath string) parityTarget {
			return openSSH(t, "ssh-shell", shellUser, root, zfsPath)
		}},
		{"native", openNative},
	} {
		t.Run(transport.name, func(t *testing.T) {
			root := container(transport.name)
			command(t, "create", "-u", root)
			zfstest.RegisterCleanup(t, root)
			source := zfstest.FixtureName(sourcePool, "parity-"+transport.name)
			command(t, "create", "-u", source)
			zfstest.RegisterCleanup(t, source)

			latest, all := root+"/latest", root+"/all"
			command(t, "set",
				policy.Namespace+"enabled=on",
				policy.Namespace+"incremental=latest",
				policy.Namespace+"remote=home",
				policy.Namespace+"local="+latest+","+all,
				source)
			latestTarget := transport.open(t, latest, "zfs")
			allTarget := transport.open(t, all, "zfs")
			now := time.Now().UTC()

			// The incremental, bookmark and refusal phases each build on the
			// destination state the previous one left, so a failure early in
			// that chain leaves the rest testing nothing. chain runs a phase
			// only while its predecessors have passed and marks the remainder
			// skipped; it calls t.Run and so is the documented exception to
			// not capturing an outer t.
			chainOK := true
			chain := func(name string, fn func(*testing.T)) {
				if !chainOK {
					t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
					return
				}
				chainOK = t.Run(name, fn)
			}
			// Created by incremental-modes, destroyed by bookmark-incremental.
			var last lifecycle.Metadata

			chain("full-bootstrap", func(t *testing.T) {
				first, snapErr := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-4*time.Hour),
					request(t, latestTarget, source, zfs.Filesystem, "").Policy)
				if snapErr != nil {
					t.Fatal(snapErr)
				}
				for _, target := range []parityTarget{latestTarget, allTarget} {
					req := request(t, target, source, zfs.Filesystem, source+"@"+first.Name())
					preview, previewErr := target.engine.Preview(t.Context(), req)
					if previewErr != nil || preview.Mode != "full" {
						t.Fatalf("full preview=%v err=%v", preview, previewErr)
					}
					result, applyErr := target.engine.Apply(t.Context(), req, nil)
					if applyErr != nil || !result.Verified || result.Plan.Mode != "full" {
						t.Fatalf("full result=%+v err=%v", result, applyErr)
					}
					if result.Plan.TargetBinding.Transport != target.binding {
						t.Fatalf("target binding=%+v want transport %q", result.Plan.TargetBinding, target.binding)
					}
				}
			})

			chain("incremental-modes", func(t *testing.T) {
				if _, snapErr := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-3*time.Hour),
					request(t, latestTarget, source, zfs.Filesystem, "").Policy); snapErr != nil {
					t.Fatal(snapErr)
				}
				// A snapshot boomerangz does not own, between two it does: -I
				// must carry it and -i must not.
				command(t, "snapshot", source+"@foreign-"+suffix)
				var snapErr error
				last, snapErr = snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-2*time.Hour),
					request(t, latestTarget, source, zfs.Filesystem, "").Policy)
				if snapErr != nil {
					t.Fatal(snapErr)
				}
				command(t, "set", policy.Namespace+"incremental=latest", source)
				result, applyErr := latestTarget.engine.Apply(t.Context(), request(t, latestTarget, source, zfs.Filesystem, ""), nil)
				if applyErr != nil || !result.Verified || result.Plan.Mode != "incremental-latest" {
					t.Fatalf("latest=%+v err=%v", result, applyErr)
				}
				state, stateErr := direct.InspectState(t.Context(), latest, false)
				if stateErr != nil {
					t.Fatal(stateErr)
				}
				if got := len(lifecycle.Snapshots(state, latest)); got != 2 {
					t.Fatalf("-i produced %d destination snapshots, want 2", got)
				}
				command(t, "set", policy.Namespace+"incremental=all", policy.Namespace+"props=on", policy.Namespace+"set_prop:readonly=on", source)
				result, applyErr = allTarget.engine.Apply(t.Context(), request(t, allTarget, source, zfs.Filesystem, ""), nil)
				if applyErr != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
					t.Fatalf("all=%+v err=%v", result, applyErr)
				}
				state, stateErr = direct.InspectState(t.Context(), all, false)
				if stateErr != nil {
					t.Fatal(stateErr)
				}
				if got := len(lifecycle.Snapshots(state, all)); got != 4 {
					t.Fatalf("-I produced %d destination snapshots, want 4", got)
				}
				for _, property := range state.Properties {
					if policy.IsPublic(property.Name) {
						t.Fatalf("source public configuration remained effective: %v", property)
					}
				}
				if got := command(t, "get", "-H", "-o", "value", "readonly", all); got != "on" {
					t.Fatalf("receive override readonly=%s, want on", got)
				}
			})

			chain("bookmark-incremental", func(t *testing.T) {
				// Advance the incremental-all target so it no longer protects
				// last, then prune last from the source. The latest-only
				// target must fall back to its bookmark.
				if _, snapErr := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-time.Hour),
					request(t, allTarget, source, zfs.Filesystem, "").Policy); snapErr != nil {
					t.Fatal(snapErr)
				}
				result, applyErr := allTarget.engine.Apply(t.Context(), request(t, allTarget, source, zfs.Filesystem, ""), nil)
				if applyErr != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
					t.Fatalf("all base rotation=%+v err=%v", result, applyErr)
				}
				if destroyErr := direct.DestroySnapshot(t.Context(), source+"@"+last.Name()); destroyErr != nil {
					t.Fatal(destroyErr)
				}
				command(t, "set", policy.Namespace+"incremental=latest", policy.Namespace+"props=off", source)
				result, applyErr = latestTarget.engine.Apply(t.Context(), request(t, latestTarget, source, zfs.Filesystem, ""), nil)
				if applyErr != nil || !result.Verified || !strings.Contains(result.Plan.Base, "#") {
					t.Fatalf("bookmark=%+v err=%v", result, applyErr)
				}
			})

			chain("unrelated-destination-refused", func(t *testing.T) {
				// The chain gives weak readiness control - this transport
				// succeeded a phase ago - but nothing that attributes a
				// failure here to the planted snapshot rather than to the
				// transport. Previewing the identical request first, and
				// requiring a plan back, is what does that.
				if _, snapErr := snapshots.CreateSnapshot(t.Context(), source, false, now,
					request(t, latestTarget, source, zfs.Filesystem, "").Policy); snapErr != nil {
					t.Fatal(snapErr)
				}
				plan, previewErr := latestTarget.engine.Preview(t.Context(), request(t, latestTarget, source, zfs.Filesystem, ""))
				if previewErr != nil || plan.Mode == "" {
					t.Fatalf("control preview=%+v err=%v", plan, previewErr)
				}
				command(t, "snapshot", latest+"@foreign-destination")
				if plan, previewErr := latestTarget.engine.Preview(t.Context(), request(t, latestTarget, source, zfs.Filesystem, "")); previewErr == nil {
					t.Fatalf("accepted unrelated destination snapshot: %+v", plan)
				}
			})

			chain("reseed-recovery", func(t *testing.T) {
				// Model a receive that created the destination before the
				// source-side binding could be persisted, and recover the
				// target the refusal above left blocked. This is the only
				// phase that touches zfs.ReseedExecutor: AbortReceive and
				// DestroyDataset are not part of zfs.Executor, and the CLI
				// refuses the whole operation when an endpoint fails that
				// assertion, so a remote executor missing them is a defect no
				// bootstrap can see.
				reseedDestination, ok := latestTarget.destination.(zfs.ReseedExecutor)
				if !ok {
					t.Fatalf("%s destination executor does not implement zfs.ReseedExecutor", transport.name)
				}
				command(t, "inherit", policy.StateNamespace+"target:"+lifecycle.TargetID(latestTarget.target()), source)
				reseed, reseedErr := transfer.NewReseedService(direct, reseedDestination, installation)
				if reseedErr != nil {
					t.Fatal(reseedErr)
				}
				req := request(t, latestTarget, source, zfs.Filesystem, "")
				plan, planErr := reseed.Plan(t.Context(), req)
				if planErr != nil || plan.BindingStored || !plan.DestinationExists || len(plan.DestinationObjects) == 0 {
					t.Fatalf("reseed plan=%+v err=%v", plan, planErr)
				}
				applied, applyErr := reseed.Apply(t.Context(), req)
				if applyErr != nil || !applied.Applied {
					t.Fatalf("reseed result=%+v err=%v", applied, applyErr)
				}
				if exists := exec.CommandContext(t.Context(), "zfs", "list", "-H", latest).Run(); exists == nil {
					t.Fatal("reseed retained the mapped destination")
				}
				result, transferErr := latestTarget.engine.Apply(t.Context(), request(t, latestTarget, source, zfs.Filesystem, ""), nil)
				if transferErr != nil || !result.Verified || result.Plan.Mode != "full" {
					t.Fatalf("post-reseed transfer=%+v err=%v", result, transferErr)
				}
			})

			// Self-contained: its own root and the shared bulk payload, so it
			// reports whatever the chain above did.
			t.Run("resume-after-interruption", func(t *testing.T) {
				resumeRoot := root + "/resume"
				// Two targets on one root, and so on one canonical target and
				// one binding: the first sends through the truncating wrapper
				// and must fail mid-receive, the second is an ordinary
				// transport that has to pick the interrupted receive up from
				// the token alone.
				truncated := transport.open(t, resumeRoot, truncatingZFS)
				result, applyErr := truncated.engine.Apply(t.Context(),
					request(t, truncated, resumeSource, zfs.Volume, resumeSource+"@"+resumeMetadata.Name()), nil)
				if applyErr == nil {
					t.Fatalf("a send truncated at %d bytes was accepted: %+v", parityTruncateBytes, result)
				}
				if len(result.ResumeDatasets) != 1 {
					t.Fatalf("truncated transfer left resume datasets %v: %v", result.ResumeDatasets, applyErr)
				}
				intact := transport.open(t, resumeRoot, "zfs")
				recovered, recoverErr := intact.engine.Apply(t.Context(),
					request(t, intact, resumeSource, zfs.Volume, resumeSource+"@"+resumeMetadata.Name()), nil)
				if recoverErr != nil || !recovered.Verified || recovered.Plan.Mode != "resume" {
					t.Fatalf("resumed result=%+v err=%v", recovered, recoverErr)
				}
				state, stateErr := direct.InspectState(t.Context(), resumeRoot, true)
				if stateErr != nil {
					t.Fatal(stateErr)
				}
				if len(state.ResumeTokens) != 0 {
					t.Fatalf("successful recovery retained resume token: %v", state.ResumeTokens)
				}
			})

			// Self-contained: its own root, its own source tree, and its own
			// canonical target, so it reports whatever the chain above did.
			// TestGuestLocalTransfer/recursive-mapping owns this behaviour
			// locally; what a remote adds is that the destination inventory,
			// the ancestor preparation and the scope filters all run through
			// the transport - and a recursive receive is the first thing that
			// asks those filters about children rather than about the root.
			t.Run("recursive-mapping", func(t *testing.T) {
				for _, discard := range []string{"first", "all"} {
					t.Run(discard, func(t *testing.T) {
						// Mapping is part of the persistent target binding,
						// so each discard mode gets its own source root and
						// its own destination root rather than rebinding one
						// source when its policy changes.
						recursiveRoot := root + "/recursive-" + discard
						command(t, "create", "-u", recursiveRoot)
						tree := zfstest.FixtureName(sourcePool, "parity-recursive-"+discard)
						command(t, "create", "-u", tree)
						command(t, "create", "-u", tree+"/child")
						zfstest.RegisterCleanup(t, tree)
						// A recursive snapshot boomerangz does not own, taken
						// before the ones it does.
						command(t, "snapshot", "-r", tree+"@foreign-recursive")
						command(t, "set",
							policy.Namespace+"enabled=on",
							policy.Namespace+"replicate=on",
							policy.Namespace+"discard="+discard,
							policy.Namespace+"remote=home",
							policy.Namespace+"local="+recursiveRoot,
							tree)
						target := transport.open(t, recursiveRoot, "zfs")
						recursiveRequest := func(t *testing.T) transfer.Request {
							t.Helper()
							return request(t, target, tree, zfs.Filesystem, "")
						}
						bootstrap, snapErr := snapshots.CreateSnapshot(t.Context(), tree, true, now,
							recursiveRequest(t).Policy)
						if snapErr != nil {
							t.Fatal(snapErr)
						}
						result, applyErr := target.engine.Apply(t.Context(), recursiveRequest(t), nil)
						if applyErr != nil || !result.Verified || result.Plan.Mode != "full" {
							t.Fatalf("recursive %s bootstrap=%+v err=%v", discard, result, applyErr)
						}
						mapped := result.Plan.Destination
						// Read the destination with the local executor: the
						// guest is both hosts, so this reads the pool the
						// remote side wrote rather than trusting the view
						// that wrote it. command fatals on a missing dataset,
						// so naming the snapshot is the assertion.
						for _, dataset := range []string{mapped, mapped + "/child"} {
							command(t, "list", "-H", "-o", "name", dataset+"@"+bootstrap.Name())
						}
						// discard=first maps below an intermediate that does
						// not exist yet, so the transport has to create it;
						// discard=all maps directly under the root, which is
						// the case CreateReceiveParent must refuse to touch.
						ancestor, prepared := recursiveRoot+"/data", discard == "first"
						if prepared != strings.HasPrefix(mapped, ancestor+"/") {
							t.Fatalf("discard=%s mapped to %s, want an ancestor of %s prepared=%v", discard, mapped, ancestor, prepared)
						}
						if prepared {
							if got := command(t, "get", "-H", "-o", "value", "canmount", ancestor); got != "noauto" {
								t.Fatalf("prepared receive ancestor %s canmount=%s, want noauto", ancestor, got)
							}
						}

						follow, snapErr := snapshots.CreateSnapshot(t.Context(), tree, true, now.Add(time.Minute),
							recursiveRequest(t).Policy)
						if snapErr != nil {
							t.Fatal(snapErr)
						}
						result, applyErr = target.engine.Apply(t.Context(), recursiveRequest(t), nil)
						if applyErr != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
							t.Fatalf("recursive %s incremental=%+v err=%v", discard, result, applyErr)
						}
						// The sibling bases a recursive incremental gathers
						// come from the destination inventory the transport
						// reports, so the child carrying the follow-up is
						// what proves that inventory was right.
						for _, dataset := range []string{mapped, mapped + "/child"} {
							command(t, "list", "-H", "-o", "name", dataset+"@"+follow.Name())
						}

						// The refusal below has to be attributable to the
						// planted snapshot rather than to any fault in the
						// transport, so preview the identical request first
						// and require a real plan back.
						if _, snapErr := snapshots.CreateSnapshot(t.Context(), tree, true, now.Add(2*time.Minute),
							recursiveRequest(t).Policy); snapErr != nil {
							t.Fatal(snapErr)
						}
						if plan, previewErr := target.engine.Preview(t.Context(), recursiveRequest(t)); previewErr != nil || plan.Mode != "incremental-all" {
							t.Fatalf("recursive %s control preview=%+v err=%v", discard, plan, previewErr)
						}
						command(t, "snapshot", mapped+"@foreign-destination")
						if plan, previewErr := target.engine.Preview(t.Context(), recursiveRequest(t)); previewErr == nil {
							t.Fatalf("accepted a foreign snapshot on the recursive destination: %+v", plan)
						}
					})
				}
			})
		})
	}
}
