package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/discovery"
	installidentity "github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/migrate"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const adminInstallation = "33333333-3333-4333-8333-333333333333"

// adminConfig writes a configuration whose paths live under a fresh
// directory, with the installation identity fixed so output is stable, and
// returns the arguments that select it.
func adminConfig(t *testing.T) (config.Config, []string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Paths.CredentialsDir = filepath.Join(dir, "credentials")
	if err := os.MkdirAll(cfg.Paths.IdentityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Paths.IdentityDir, installidentity.Filename), []byte(adminInstallation+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.toml")
	document := fmt.Sprintf("[paths]\nsocket_path=%q\nidentity_dir=%q\ncredentials_dir=%q\n", cfg.Paths.SocketPath, cfg.Paths.IdentityDir, cfg.Paths.CredentialsDir)
	if err := os.WriteFile(configPath, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfg, []string{"--config", configPath, "--config-dir", filepath.Join(dir, "missing")}
}

func adoptionFixture() *adoptionExecutor {
	return &adoptionExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: policy.Namespace + "local", Value: "backup/data", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: "11111111-1111-4111-8111-111111111111", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.VersionProperty, Value: "2", Source: zfs.SourceLocal},
	}}
}

func migrationFixture() *adoptionExecutor {
	return &adoptionExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: "11111111-1111-4111-8111-111111111111", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal},
	}}
}

func recoveryFixture() recoveryExecutor {
	return recoveryExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: "11111111-1111-4111-8111-111111111111", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.VersionProperty, Value: "2", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal},
	}}
}

// TestAdminCommandsRenderTextByDefaultAndJSONOnRequest runs each
// preview/apply command through the command line. --json must reproduce,
// byte for byte, the JSON these commands printed before they had a text
// rendering (testdata/admin), and the text must carry everything an operator
// reviews before --apply.
func TestAdminCommandsRenderTextByDefaultAndJSONOnRequest(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		golden  string
		command []string
		reader  func() discovery.Reader
		want    []string
	}{
		{
			name: "clean preview", golden: "clean-preview.json",
			command: []string{"dataset", "clean", "tank/data"},
			reader:  func() discovery.Reader { return cleanFixture() },
			want:    []string{"Dataset: tank/data\n", "Options: none\n", "Actions:\n  inherit tank/data org.boomerangz:enabled\n", "Warnings:\n  - inherit masks received properties", "Applied: 0 of 1 actions\n", "Preview only; nothing was changed. Apply with --apply.\n"},
		},
		{
			name: "clean apply", golden: "clean-apply.json",
			command: []string{"dataset", "clean", "tank/data", "--apply"},
			reader:  func() discovery.Reader { return cleanFixture() },
			want:    []string{"Dataset: tank/data\n", "inherit tank/data org.boomerangz:enabled\n", "Applied: 1 of 1 actions\n"},
		},
		{
			name: "adopt preview", golden: "adopt-preview.json",
			command: []string{"dataset", "adopt", "tank"},
			reader:  func() discovery.Reader { return adoptionFixture() },
			want:    []string{"Dataset: tank\n", "Lineage: 11111111-1111-4111-8111-111111111111\n", "Current owner: 22222222-2222-4222-8222-222222222222\n", "New owner: " + adminInstallation + "\n", "Local destinations: backup/data\n", "Local targets:\n  backup/data: unbound (endpoint local:backup/data; mapped to backup/data; binding not yet stored)\n", "Warnings:\n  - transferred policy may name absent", "Applied: no\n", "Preview only"},
		},
		{
			name: "adopt apply", golden: "adopt-apply.json",
			command: []string{"dataset", "adopt", "tank", "--apply"},
			reader:  func() discovery.Reader { return adoptionFixture() },
			want:    []string{"Dataset: tank\n", "Applied: yes\n"},
		},
		{
			name: "migrate preview", golden: "migrate-preview.json",
			command: []string{"migrate", "backup", "tank"},
			reader:  func() discovery.Reader { return migrationFixture() },
			want:    []string{"Dataset: backup\nStatus: unmanaged\nFormat: none; keeps no state\n", "Dataset: tank\nStatus: pending\nFormat: v1 to v2\n", "Actions:\n  set org.boomerangz:state:version\n    to:   2\n", "Applied: 0 of 1 actions\n", "Preview only"},
		},
		{
			name: "migrate apply", golden: "migrate-apply.json",
			command: []string{"migrate", "tank", "--apply"},
			reader:  func() discovery.Reader { return migrationFixture() },
			want:    []string{"Dataset: tank\nStatus: migrated\n", "Applied: 1 of 1 actions\n"},
		},
		{
			name: "identity recover preview", golden: "identity-recover-preview.json",
			command: []string{"identity", "recover"},
			reader:  func() discovery.Reader { return recoveryFixture() },
			want:    []string{"Current identity: " + adminInstallation + "\n", "Recovered identity: 11111111-1111-4111-8111-111111111111\n", "Roots owned by the recovered identity:\n  tank (lineage 22222222-2222-4222-8222-222222222222)\n", "Applied: no\n", "Preview only"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			run := func(t *testing.T, extra ...string) string {
				t.Helper()
				_, common := adminConfig(t)
				// Global options belong to the command group that declares them.
				args := append([]string{test.command[0]}, common...)
				args = append(args, test.command[1:]...)
				args = append(args, extra...)
				var output bytes.Buffer
				if err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, test.reader()); err != nil {
					t.Fatalf("%v: %s", err, output.String())
				}
				return output.String()
			}
			golden, err := os.ReadFile(filepath.Join("testdata", "admin", test.golden))
			if err != nil {
				t.Fatal(err)
			}
			if got := run(t, "--json"); got != string(golden) {
				t.Errorf("--json differs from %s:\n%s", test.golden, got)
			}
			text := run(t)
			for _, want := range test.want {
				if !strings.Contains(text, want) {
					t.Errorf("text output missing %q:\n%s", want, text)
				}
			}
			if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
				t.Errorf("text output is JSON:\n%s", text)
			}
			if applying := strings.Contains(strings.Join(test.command, " "), "--apply"); applying == strings.Contains(text, "Preview only") {
				t.Errorf("preview note present=%v while applying=%v:\n%s", !applying, applying, text)
			}
		})
	}
}

func cleanFixture() *cleanExecutor {
	return &cleanExecutor{properties: []zfs.Property{{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "off", Source: zfs.SourceLocal}}}
}

// TestDaemonCleanRendersTextByDefaultAndJSONOnRequest covers clean when the
// daemon runs it, whose plans arrive over the control socket.
func TestDaemonCleanRendersTextByDefaultAndJSONOnRequest(t *testing.T) {
	t.Parallel()
	cfg, common := adminConfig(t)
	server, err := control.StartServer(cfg, &cliControlRuntime{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	run := func(extra ...string) string {
		args := append(append([]string{"dataset"}, common...), "clean", "--apply", "--forget-target", "home-backup", "--forget-target", "backup/archive", "tank/data")
		var output bytes.Buffer
		if err := runWithReader(t.Context(), append(args, extra...), &output, io.Discard, BuildInfo{}, nil); err != nil {
			t.Fatal(err)
		}
		return output.String()
	}
	golden, err := os.ReadFile(filepath.Join("testdata", "admin", "clean-daemon.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := run("--json"); got != string(golden) {
		t.Errorf("--json differs from clean-daemon.json:\n%s", got)
	}
	want := "Dataset: tank/data\nOptions: forget target home-backup, forget target backup/archive\n\nActions: none\n\nApplied: 1 of 0 actions\n"
	if got := run(); got != want {
		t.Errorf("text=%q, want %q", got, want)
	}
}

func sampleReseedPlan() transfer.ReseedPlan {
	return transfer.ReseedPlan{
		Dataset:            "tank/home",
		Target:             "ssh://backup@example.net:22/pool/replicas",
		Binding:            transfer.TargetBinding{Transport: "ssh", CanonicalTarget: "ssh://backup@example.net:22/pool/replicas", DestinationRoot: "pool/replicas", MappedDataset: "pool/replicas/home", Pool: "pool", PoolGUID: 101, Anchor: "pool/replicas/home", AnchorGUID: 202, RelativePath: "home"},
		BindingStored:      true,
		ReceiveAnchor:      "pool/replicas",
		DestinationExists:  true,
		DestinationObjects: []string{"pool/replicas/home", "pool/replicas/home@boomerangz-1"},
		SetAside:           "pool/replicas/.home.boomerangz-aside",
		ResumeDatasets:     []string{"pool/replicas/home/docs"},
		References:         []lifecycle.Reference{{TargetID: "t-1", Dataset: "tank/home", Metadata: lifecycle.Metadata{Lineage: "22222222-2222-4222-8222-222222222222", Snapshot: "44444444-4444-4444-8444-444444444444", Created: time.Date(2026, 10, 1, 2, 3, 4, 0, time.UTC)}, Recursive: true}},
		Suspended:          true,
		Warning:            "applying this plan permanently destroys the mapped destination replica and resets its replication history",
	}
}

func sampleAbortPlan() transfer.AbortPlan {
	return transfer.AbortPlan{
		Dataset:  "tank/home",
		Target:   "ssh://backup@example.net:22/pool/replicas",
		Receives: []string{"pool/replicas/home", "pool/replicas/home/docs"},
		Warning:  "applying this plan discards the partially received data of each listed receive; the next transfer sends it again",
	}
}

// TestTargetOperationRenderings pins the reseed and abort-receive renderings.
// Their JSON is what runTargetOperation printed before the text rendering
// existed.
func TestTargetOperationRenderings(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, golden string, encode func(io.Writer) error, text func(io.Writer) error, want string) {
		t.Helper()
		expected, err := os.ReadFile(filepath.Join("testdata", "admin", golden))
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err := encode(&output); err != nil {
			t.Fatal(err)
		}
		if output.String() != string(expected) {
			t.Errorf("JSON differs from %s:\n%s", golden, output.String())
		}
		output.Reset()
		if err := text(&output); err != nil {
			t.Fatal(err)
		}
		if output.String() != want {
			t.Errorf("text=\n%s\nwant\n%s", output.String(), want)
		}
	}
	reseed := sampleReseedPlan()
	check(t, "reseed-preview.json", func(w io.Writer) error { return writeJSON(w, reseed) }, func(w io.Writer) error { return writeReseedPlan(w, reseed, false) }, `Dataset: tank/home
Target: ssh://backup@example.net:22/pool/replicas
Mapped replica: pool/replicas/home
Destination pool: pool (guid 101)
Binding anchor: pool/replicas/home (guid 202)
Binding stored: yes
Receive anchor: pool/replicas
Replica exists: yes
Set-aside placeholder: pool/replicas/.home.boomerangz-aside
Target suspended: yes

Destination objects to destroy:
  pool/replicas/home
  pool/replicas/home@boomerangz-1

Interrupted receives to discard:
  pool/replicas/home/docs

Source proofs to release:
  tank/home: snapshot 44444444-4444-4444-8444-444444444444 of lineage 22222222-2222-4222-8222-222222222222, created 2026-10-01T02:03:04Z, recursive

Warnings:
  - applying this plan permanently destroys the mapped destination replica and resets its replication history

Applied: no

Preview only; nothing was changed. Apply with --apply.
`)
	abort := sampleAbortPlan()
	check(t, "abort-receive-preview.json", func(w io.Writer) error { return writeJSON(w, abort) }, func(w io.Writer) error { return writeAbortPlan(w, abort, false) }, `Dataset: tank/home
Target: ssh://backup@example.net:22/pool/replicas

Interrupted receives to discard:
  pool/replicas/home
  pool/replicas/home/docs

Warnings:
  - applying this plan discards the partially received data of each listed receive; the next transfer sends it again

Applied: no

Preview only; nothing was changed. Apply with --apply.
`)

	// An operation that failed before planning returns no plan; the error
	// alone says why, so the text rendering prints nothing.
	var output bytes.Buffer
	if err := writeReseedPlan(&output, transfer.ReseedPlan{}, true); err != nil || output.Len() != 0 {
		t.Fatalf("empty reseed plan rendered %q: %v", output.String(), err)
	}
	if err := writeAbortPlan(&output, transfer.AbortPlan{}, true); err != nil || output.Len() != 0 {
		t.Fatalf("empty abort plan rendered %q: %v", output.String(), err)
	}
}

// TestCleanTextNamesEveryActionBlockerAndWarning covers the parts of a clean
// plan the command fixtures do not reach.
func TestCleanTextNamesEveryActionBlockerAndWarning(t *testing.T) {
	t.Parallel()
	plans := []lifecycle.CleanPlan{
		{
			Dataset: "tank/a",
			Options: lifecycle.CleanOptions{Recursive: true, DestroyOwnedSnapshots: true},
			Actions: []lifecycle.CleanAction{
				{Operation: "release", Object: "tank/a@s1", Property: "boomerangz-hold", GUID: 7},
				{Operation: "destroy-bookmark", Object: "tank/a#b1", GUID: 8},
				{Operation: "destroy-snapshot", Object: "tank/a@s2", GUID: 9},
				{Operation: "inherit", Object: "tank/a", Property: "org.boomerangz:owner"},
			},
			Blockers: []string{"snapshot tank/a@s2 has dependent clones"},
			Warnings: []string{"hidden received values may remain"},
		},
		{Dataset: "tank/b"},
	}
	var output bytes.Buffer
	if err := writeCleanPlans(&output, plans, true); err != nil {
		t.Fatal(err)
	}
	want := `Dataset: tank/a
Options: recursive, destroy owned snapshots

Actions:
  release tank/a@s1 boomerangz-hold (guid 7)
  destroy-bookmark tank/a#b1 (guid 8)
  destroy-snapshot tank/a@s2 (guid 9)
  inherit tank/a org.boomerangz:owner

Blockers:
  - snapshot tank/a@s2 has dependent clones

Warnings:
  - hidden received values may remain

Applied: 0 of 4 actions

Dataset: tank/b
Options: none

Actions: none

Applied: 0 of 0 actions
`
	if output.String() != want {
		t.Errorf("text=\n%s\nwant\n%s", output.String(), want)
	}
}

// TestMigrationTextShowsPreviousValuesAndBlockers covers what the migrate
// fixture does not reach: a rewritten property and a blocked root.
func TestMigrationTextShowsPreviousValuesAndBlockers(t *testing.T) {
	t.Parallel()
	plans := []migrate.Plan{
		{Dataset: "tank", Status: migrate.StatusPending, From: 1, To: 2, Actions: []migrate.Action{{Property: "org.boomerangz:state:binding:x", Previous: `{"v":1}`, Value: `{"mapped":"backup/tank"}`}}},
		{Dataset: "old", Status: migrate.StatusBlocked, Blockers: []string{"state on old is unreadable"}},
		{Dataset: "new", Status: migrate.StatusBlocked, From: 3, To: 2, Blockers: []string{"state on new is format v3"}},
		{Dataset: "done", Status: migrate.StatusCurrent, From: 2, To: 2},
	}
	var output bytes.Buffer
	if err := writeMigrationPlans(&output, plans, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Actions:\n  set org.boomerangz:state:binding:x\n    from: {\"v\":1}\n    to:   {\"mapped\":\"backup/tank\"}\n",
		"Dataset: old\nStatus: blocked\nFormat: unreadable\n\nBlockers:\n  - state on old is unreadable\n",
		"Format: v3, newer than this build's v2\n",
		"Dataset: done\nStatus: current\nFormat: v2 (current)\n",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("text missing %q:\n%s", want, output.String())
		}
	}
}

// TestReseedAndAbortAcceptJSON checks the target operations accept --json:
// the command parses and runs, failing only for want of an executor.
func TestReseedAndAbortAcceptJSON(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"reseed", "abort-receive"} {
		_, common := adminConfig(t)
		args := append(append([]string{"dataset"}, common...), command, "--json", "tank/a/home", "backup")
		err := runWithReader(t.Context(), args, io.Discard, io.Discard, BuildInfo{}, collidingReader{})
		if err == nil || !strings.Contains(err.Error(), "lifecycle executor unavailable") {
			t.Fatalf("%s --json: %v", command, err)
		}
	}
}
