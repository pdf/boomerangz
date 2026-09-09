package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/identity"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
)

type commandLine struct {
	Config   configCommand   `cmd:"" help:"Inspect and validate daemon configuration."`
	Version  versionCommand  `cmd:"" help:"Show version information."`
	Daemon   daemonCommand   `cmd:"" help:"Run snapshot and replication management in the foreground."`
	Status   statusCommand   `cmd:"" help:"Show live daemon status."`
	Trigger  triggerCommand  `cmd:"" help:"Queue an immediate snapshot for selected active roots."`
	Pairing  pairingCommand  `cmd:"" help:"Manage authenticated API pairings."`
	SSHShell sshShellCommand `cmd:"" name:"ssh-shell" hidden:"" help:"Serve the restricted replication protocol over SSH."`
	Dataset  datasetCommand  `cmd:"" help:"Inspect and administer dataset lifecycle."`
	Identity identityCommand `cmd:"" help:"Inspect and recover installation identity."`
}

type commandEnvironment struct {
	Context context.Context
	Stdout  io.Writer
	Stderr  io.Writer
	Build   BuildInfo
	Reader  discovery.Reader
	Root    *commandLine
}

type configOptions struct {
	Path    string `name:"config" default:"/etc/boomerangz/config.toml" help:"Primary configuration file."`
	DropIns string `name:"config-dir" default:"/etc/boomerangz/config.d" help:"Configuration drop-in directory."`
}

func (o configOptions) load() (config.Loaded, error) { return config.Load(o.Path, o.DropIns) }

type configCommand struct {
	Check configCheckCommand `cmd:"" help:"Validate configuration and all drop-ins."`
	Show  configShowCommand  `cmd:"" help:"Show merged, effective configuration."`
}

type configCheckCommand struct {
	configOptions `embed:""`
}

func (c *configCheckCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(env.Stdout, "configuration valid (%d source files)\n", len(loaded.Sources))
	return err
}

type configShowCommand struct {
	configOptions `embed:""`
}

func (c *configShowCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	encoded, err := config.MarshalRedacted(loaded.Config)
	if err != nil {
		return fmt.Errorf("encode configuration: %w", err)
	}
	_, err = env.Stdout.Write(encoded)
	return err
}

type versionCommand struct {
	JSON bool `help:"Emit JSON."`
}

func (c *versionCommand) Run(env *commandEnvironment) error {
	if c.JSON {
		return json.NewEncoder(env.Stdout).Encode(env.Build)
	}
	_, err := fmt.Fprintf(env.Stdout, "boomerangz %s (commit %s at %s)\n", env.Build.Version, env.Build.Commit, env.Build.Date)
	return err
}

type daemonCommand struct {
	configOptions `embed:""`
}

func (c *daemonCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	lock, err := lifecycleLock(loaded.Config)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	installation, err := identity.LoadOrCreate(loaded.Config.Paths.IdentityDir)
	if err != nil {
		return err
	}
	var source daemonBackend
	if env.Reader != nil {
		source, _ = env.Reader.(daemonBackend)
	}
	if source == nil {
		source, err = zfs.NewDirect("zfs")
		if err != nil {
			return err
		}
	}
	logger := slog.New(slog.NewJSONHandler(env.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	runtime, err := daemon.New(loaded.Config, source, installation, logger)
	if err != nil {
		return err
	}
	server, err := control.StartServerWithReplication(loaded.Config, runtime, source, "zfs", logger)
	if err != nil {
		return err
	}
	finished := make(chan struct{})
	go func() {
		select {
		case <-env.Context.Done():
			_ = server.Close()
		case <-finished:
		}
	}()
	runErr := runtime.Run(env.Context)
	close(finished)
	return errors.Join(runErr, server.Close())
}

type statusCommand struct {
	configOptions `embed:""`
	Credential    string        `help:"Imported pairing name or absolute bundle path."`
	Watch         bool          `short:"w" help:"Continuously watch status."`
	Interval      time.Duration `short:"i" default:"2s" help:"Maximum interval between watch updates."`
}

func (c *statusCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	return runStatus(env.Context, env.Stdout, loaded.Config, c.Credential, c.Watch, c.Interval)
}

type triggerCommand struct {
	configOptions `embed:""`
	Credential    string   `help:"Imported pairing name or absolute bundle path."`
	Datasets      []string `arg:"" optional:"" help:"Active scheduling roots; empty selects all."`
}

func (c *triggerCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	return runTrigger(env.Context, env.Stdout, loaded.Config, c.Credential, c.Datasets)
}

type pairingCommand struct {
	configOptions `embed:""`
	Create        pairingCreateCommand `cmd:"" help:"Create a one-time pairing bundle."`
	Import        pairingImportCommand `cmd:"" help:"Import a pairing bundle for client use."`
	List          pairingListCommand   `cmd:"" help:"List issued and imported pairings."`
	Revoke        pairingRevokeCommand `cmd:"" help:"Revoke an issued pairing."`
}

type pairingCreateCommand struct {
	Listener   string        `help:"Configured TCP listener name; inferred when exactly one is configured."`
	ClientCert string        `name:"client-cert" type:"existingfile" help:"External mTLS client certificate."`
	ClientKey  string        `name:"client-key" type:"existingfile" help:"External mTLS client private key."`
	Scopes     []string      `name:"scope" default:"status" enum:"status,trigger,replicate,prune,admin" help:"Authorized scope; repeat as needed. Allowed: ${enum}."`
	ExpiresIn  time.Duration `name:"expires-in" default:"0s" help:"Token lifetime; zero means no expiry."`
}

func (c *pairingCreateCommand) BeforeApply() error {
	if (c.ClientCert == "") != (c.ClientKey == "") {
		return errors.New("--client-cert and --client-key must be provided together")
	}
	return nil
}

func (c *pairingCreateCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Pairing.load()
	if err != nil {
		return err
	}
	return runPairingCreate(env.Stdout, loaded.Config, c.Listener, c.ClientCert, c.ClientKey, c.Scopes, c.ExpiresIn)
}

type pairingImportCommand struct {
	Name   string `arg:"" required:"" help:"Local credential name."`
	Bundle string `arg:"" required:"" type:"existingfile" help:"Pairing bundle JSON file."`
}

func (c *pairingImportCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Pairing.load()
	if err != nil {
		return err
	}
	return runPairingImport(env.Stdout, loaded.Config, c.Name, c.Bundle)
}

type pairingListCommand struct{}

func (*pairingListCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Pairing.load()
	if err != nil {
		return err
	}
	return runPairingList(env.Stdout, loaded.Config)
}

type pairingRevokeCommand struct {
	ID string `arg:"" required:"" help:"Pairing identifier."`
}

func (c *pairingRevokeCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Pairing.load()
	if err != nil {
		return err
	}
	return runPairingRevoke(env.Stdout, loaded.Config, c.ID)
}

type sshShellCommand struct {
	configOptions `embed:""`
}

func (c *sshShellCommand) Run(env *commandEnvironment) error {
	loaded, err := c.load()
	if err != nil {
		return err
	}
	roots := loaded.Config.SSHShell.ReplicationRoots
	if len(roots) == 0 {
		return fmt.Errorf("ssh_shell.replication_roots must configure at least one destination root")
	}
	executor, err := zfs.NewDirect("zfs")
	if err != nil {
		return err
	}
	service, err := remoterpc.NewServerForRoots(executor, roots, "zfs")
	if err != nil {
		return err
	}
	return remoterpc.ServeStdio(env.Context, service, os.Stdin, env.Stdout)
}

type datasetCommand struct {
	configOptions `embed:""`
	List          datasetListCommand    `cmd:"" help:"List sparse inventory and active policies as JSON."`
	Inspect       datasetInspectCommand `cmd:"" help:"Inspect effective policy and stored properties as JSON."`
	Adopt         datasetAdoptCommand   `cmd:"" help:"Preview transfer of an existing lineage to this installation."`
	Clean         datasetCleanCommand   `cmd:"" help:"Preview explicit local decommissioning; preserve snapshots by default."`
}

func datasetExecutor(env *commandEnvironment) (discovery.Reader, zfs.Executor, error) {
	reader := env.Reader
	if reader == nil {
		direct, err := zfs.NewDirect("zfs")
		if err != nil {
			return nil, nil, err
		}
		reader = direct
	}
	executor, ok := reader.(zfs.Executor)
	if !ok {
		return nil, nil, errors.New("lifecycle executor unavailable")
	}
	return reader, executor, nil
}

func scanDatasets(env *commandEnvironment, options configOptions, inspect []string) (*discovery.Generation, error) {
	loaded, err := options.load()
	if err != nil {
		return nil, err
	}
	reader := env.Reader
	if reader == nil {
		reader, err = zfs.NewDirect("zfs")
		if err != nil {
			return nil, err
		}
	}
	remotes := make([]string, 0, len(loaded.Config.Remotes))
	for name := range loaded.Config.Remotes {
		remotes = append(remotes, name)
	}
	scanner, err := discovery.New(reader, discovery.Options{Remotes: remotes})
	if err != nil {
		return nil, err
	}
	return scanner.Scan(env.Context, inspect)
}

type datasetListCommand struct{}

func (*datasetListCommand) Run(env *commandEnvironment) error {
	generation, err := scanDatasets(env, env.Root.Dataset.configOptions, nil)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(env.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(generation.Entries())
}

type datasetInspectCommand struct {
	Dataset string `arg:"" required:"" help:"Exact ZFS dataset name."`
}

func (c *datasetInspectCommand) Run(env *commandEnvironment) error {
	generation, err := scanDatasets(env, env.Root.Dataset.configOptions, []string{c.Dataset})
	if err != nil {
		return err
	}
	entry, exists := generation.Inspect(c.Dataset)
	if !exists {
		return fmt.Errorf("dataset %q was not found", c.Dataset)
	}
	encoder := json.NewEncoder(env.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(entry)
}

type datasetAdoptCommand struct {
	Dataset string `arg:"" required:"" help:"Exact ZFS dataset name."`
	Apply   bool   `help:"Apply the adoption after revalidation."`
}

func (c *datasetAdoptCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Dataset.load()
	if err != nil {
		return err
	}
	_, executor, err := datasetExecutor(env)
	if err != nil {
		return err
	}
	return runAdopt(env.Context, env.Stdout, loaded.Config, executor, c.Dataset, c.Apply)
}

type datasetCleanCommand struct {
	Datasets              []string `arg:"" optional:"" help:"Exact ZFS dataset scopes."`
	Recursive             bool     `help:"Include descendants of the selected scopes."`
	All                   bool     `help:"Explicitly select all local datasets recursively."`
	DestroyOwnedSnapshots bool     `name:"destroy-owned-snapshots" help:"Also delete fully proven owned snapshots without dependencies."`
	Apply                 bool     `help:"Apply clean after revalidation."`
}

func (c *datasetCleanCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Dataset.load()
	if err != nil {
		return err
	}
	if handled, controlErr := runDaemonClean(env.Context, env.Stdout, loaded.Config, c.Datasets, c.Recursive, c.All, c.DestroyOwnedSnapshots, c.Apply); handled {
		return controlErr
	}
	_, executor, err := datasetExecutor(env)
	if err != nil {
		return err
	}
	return runClean(env.Context, env.Stdout, loaded.Config, executor, c.Datasets, c.Recursive, c.All, c.DestroyOwnedSnapshots, c.Apply)
}

type identityCommand struct {
	configOptions `embed:""`
	Recover       identityRecoverCommand `cmd:"" help:"Preview recovery from local source-root owner markers."`
}

type identityRecoverCommand struct {
	Owner string `help:"Explicit owner UUID when more than one candidate exists."`
	Apply bool   `help:"Apply the identity replacement after revalidation."`
}

func (c *identityRecoverCommand) Run(env *commandEnvironment) error {
	loaded, err := env.Root.Identity.load()
	if err != nil {
		return err
	}
	reader, executor, err := datasetExecutor(env)
	if err != nil {
		return err
	}
	return runIdentityRecover(env.Context, env.Stdout, loaded.Config, reader, executor, c.Owner, c.Apply)
}
