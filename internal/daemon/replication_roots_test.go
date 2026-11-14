package daemon

import (
	"context"
	"log/slog"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/zfs"
)

// receivingHost is a host with no actor roots of its own that receives a
// remote source's replica beneath backup/remote, carrying nothing else that
// marks it as one.
type receivingHost struct{ *memoryZFS }

func (receivingHost) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}

func receivingPool(t *testing.T) receivingHost {
	t.Helper()
	pool, _ := newReportPool(t)
	pool.inventory = append(pool.inventory,
		zfs.Dataset{Name: "backup/remote", Type: zfs.Filesystem, EncryptionRoot: "-"},
		zfs.Dataset{Name: "backup/remote/laptop", Type: zfs.Filesystem, EncryptionRoot: "-"},
	)
	return receivingHost{pool}
}

// scannedStatus scans with the daemon's own scanner and returns each
// dataset's status as the status view would list it.
func scannedStatus(t *testing.T, runtime *Runtime) map[string]string {
	t.Helper()
	generation, err := runtime.scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	statuses := map[string]string{}
	for _, entry := range generation.Entries() {
		statuses[entry.Dataset.Name] = entry.Status()
	}
	return statuses
}

func TestDaemonMarksReplicasBeneathItsReplicationRoots(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.SSHShell.ReplicationRoots = []string{"backup/remote"}
	pool := receivingPool(t)
	runtime, err := NewWithLocalStream(cfg, pool, reportInstallation, slog.New(slog.DiscardHandler), memoryStream{source: pool.memoryZFS, destination: pool.memoryZFS})
	if err != nil {
		t.Fatal(err)
	}
	if got := scannedStatus(t, runtime)["backup/remote/laptop"]; got != discovery.StatusTarget {
		t.Fatalf("a replica beneath a replication root is %q, want %q as dataset list reports it", got, discovery.StatusTarget)
	}
}

func TestReloadMarksReplicasBeneathNewReplicationRoots(t *testing.T) {
	t.Parallel()
	pool := receivingPool(t)
	runtime, err := NewWithLocalStream(config.Defaults(), pool, reportInstallation, slog.New(slog.DiscardHandler), memoryStream{source: pool.memoryZFS, destination: pool.memoryZFS})
	if err != nil {
		t.Fatal(err)
	}
	if got := scannedStatus(t, runtime)["backup/remote/laptop"]; got != discovery.StatusInactive {
		t.Fatalf("without a replication root the replica is %q, want %q", got, discovery.StatusInactive)
	}
	next := config.Defaults()
	next.Listeners = map[string]config.ListenerConfig{}
	next.SSHShell.ReplicationRoots = []string{"backup/remote"}
	if _, err := runtime.ApplyConfig(next); err != nil {
		t.Fatal(err)
	}
	if got := scannedStatus(t, runtime)["backup/remote/laptop"]; got != discovery.StatusTarget {
		t.Fatalf("after a reload adding its replication root the replica is %q, want %q", got, discovery.StatusTarget)
	}
}
