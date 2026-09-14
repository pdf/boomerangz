package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	replicationnative "github.com/pdf/boomerangz/internal/replication/native"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/zfs"
)

type remoteStream interface {
	Run(context.Context, zfs.SendOptions, zfs.ReceiveOptions, zfs.Estimate, func(zfs.Progress)) (zfs.Progress, error)
}

type openedRemote struct {
	mode     string
	executor zfs.Executor
	stream   remoteStream
	close    func() error
}

type remoteClient interface {
	CanonicalTarget() string
	Transport() string
	Open(context.Context) (openedRemote, error)
}

type sshRemote struct {
	client  *replicationssh.Client
	setting config.RemoteConfig
}

func (r *sshRemote) CanonicalTarget() string { return r.client.CanonicalTarget() }
func (*sshRemote) Transport() string         { return "ssh" }
func (r *sshRemote) Open(ctx context.Context) (openedRemote, error) {
	endpoint, err := replicationssh.OpenEndpoint(ctx, r.client, "zfs", r.setting.Endpoint)
	if err != nil {
		return openedRemote{}, err
	}
	return openedRemote{mode: endpoint.Mode, executor: endpoint.Executor, stream: endpoint.Stream, close: endpoint.Close}, nil
}

type nativeRemote struct {
	bundle    control.PairingBundle
	root      string
	canonical string
}

func (r *nativeRemote) CanonicalTarget() string { return r.canonical }
func (*nativeRemote) Transport() string         { return "native" }
func (r *nativeRemote) Open(ctx context.Context) (openedRemote, error) {
	endpoint, err := replicationnative.Open(ctx, r.bundle, r.root, "zfs")
	if err != nil {
		return openedRemote{}, err
	}
	return openedRemote{mode: "native", executor: endpoint.Executor(), stream: endpoint.Stream(), close: endpoint.Close}, nil
}

// sameRemoteClients reports whether two sets of remote clients would reach the
// same remotes the same way. Clients are compared by what they were built
// from - an SSH remote's setting, a native remote's pairing bundle and root -
// so a native credential file whose contents changed counts as a change even
// when the configuration naming it did not.
func sameRemoteClients(a, b map[string]remoteClient) bool {
	if len(a) != len(b) {
		return false
	}
	for name, left := range a {
		switch left := left.(type) {
		case *sshRemote:
			right, ok := b[name].(*sshRemote)
			if !ok || !reflect.DeepEqual(left.setting, right.setting) {
				return false
			}
		case *nativeRemote:
			right, ok := b[name].(*nativeRemote)
			if !ok || !reflect.DeepEqual(*left, *right) {
				return false
			}
		default:
			// A client this cannot inspect is never assumed unchanged.
			return false
		}
	}
	return true
}

func buildRemoteClients(settings map[string]config.RemoteConfig, credentialsDir string) (map[string]remoteClient, error) {
	clients := make(map[string]remoteClient, len(settings))
	for name, setting := range settings {
		switch setting.Transport {
		case "ssh":
			client, err := replicationssh.New("ssh", replicationssh.Config{Host: setting.Host, Port: setting.Port, User: setting.User, Root: setting.Root, IdentityFile: setting.IdentityFile, ShellPath: setting.SSHShellPath, ConnectTimeout: setting.ConnectTimeout.Duration})
			if err != nil {
				return nil, fmt.Errorf("remote %s: %w", name, err)
			}
			clients[name] = &sshRemote{client: client, setting: setting}
		case "native":
			path := setting.Credential
			if !filepath.IsAbs(path) {
				path = filepath.Join(credentialsDir, path+".json")
			}
			bundle, err := control.LoadPairingBundle(path)
			if err != nil {
				return nil, fmt.Errorf("remote %s credential: %w", name, err)
			}
			canonical, err := replicationnative.CanonicalTarget(bundle.Endpoint, setting.Root)
			if err != nil {
				return nil, fmt.Errorf("remote %s: %w", name, err)
			}
			clients[name] = &nativeRemote{bundle: bundle, root: setting.Root, canonical: canonical}
		default:
			return nil, fmt.Errorf("remote %s uses unsupported transport %q", name, setting.Transport)
		}
	}
	return clients, nil
}
