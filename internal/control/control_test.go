package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRuntime struct {
	mu        sync.Mutex
	revision  uint64
	changed   chan struct{}
	triggered []string
}

type remoteTestBackend struct{ zfs.Executor }

func (remoteTestBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, {Name: "tank/backups", Type: zfs.Filesystem}, {Name: "other/private", Type: zfs.Filesystem}}, nil
}
func (remoteTestBackend) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 10, Pool: "tank", PoolGUID: 11}, nil
}

func (f *fakeRuntime) ControlStatus() daemonstate.ControlSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return daemonstate.ControlSnapshot{Revision: f.revision, Observed: time.Unix(10, 0), Generation: 7, Datasets: []daemonstate.DatasetStatus{{Name: "tank/data", Active: true}}, Queues: map[string]daemonstate.QueueSnapshot{"management": {Capacity: 8}}}
}
func (f *fakeRuntime) WaitStatus(ctx context.Context, after uint64) error {
	f.mu.Lock()
	if f.revision > after {
		f.mu.Unlock()
		return nil
	}
	if f.changed == nil {
		f.changed = make(chan struct{})
	}
	changed := f.changed
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	}
}
func (f *fakeRuntime) Trigger(names []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.triggered = append([]string(nil), names...)
	return names, nil
}
func (*fakeRuntime) Reconcile() {}
func (*fakeRuntime) Clean(_ context.Context, names []string, recursive, _, destroy, apply bool) ([]lifecycle.CleanPlan, error) {
	return []lifecycle.CleanPlan{{Dataset: names[0], Options: lifecycle.CleanOptions{Recursive: recursive, DestroyOwnedSnapshots: destroy}, Applied: map[bool]int{true: 1}[apply]}}, nil
}

func TestUnixControlAPI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	runtime := &fakeRuntime{}
	server, err := StartServer(cfg, runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	statusResponse, err := client.Status.GetStatus(t.Context(), &controlrpc.GetStatusRequest{})
	if err != nil || statusResponse.GetStatus().GetGeneration() != 7 {
		t.Fatalf("status=%v err=%v", statusResponse, err)
	}
	trigger, err := client.Control.Trigger(t.Context(), &controlrpc.TriggerRequest{Datasets: []string{"tank/data"}})
	if err != nil || !reflect.DeepEqual(trigger.GetAccepted(), []string{"tank/data"}) {
		t.Fatalf("trigger=%v err=%v", trigger, err)
	}
	clean, err := client.Control.Clean(t.Context(), &controlrpc.CleanRequest{Datasets: []string{"tank/data"}, Apply: true})
	if err != nil || clean.GetPlans()[0].GetApplied() != 1 {
		t.Fatalf("clean=%v err=%v", clean, err)
	}
	info, err := os.Stat(cfg.Paths.SocketPath)
	if err != nil || info.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode=%v err=%v", info.Mode(), err)
	}
}

func TestServerNeverReplacesNonSocket(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Paths.SocketPath = path
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	if _, err := StartServer(cfg, &fakeRuntime{}, nil); err == nil {
		t.Fatal("non-socket was replaced")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}

func writeCertificate(t *testing.T, dir string) (string, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate := filepath.Join(dir, "server.crt")
	key := filepath.Join(dir, "server.key")
	keyDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certificate, key
}

func writeMTLSPKI(t *testing.T, dir string) (caPath, serverCertPath, serverKeyPath string, clientCert, clientKey []byte) {
	t.Helper()
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(10), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, name string, usage x509.ExtKeyUsage) ([]byte, []byte) {
		public, private, keyErr := ed25519.GenerateKey(rand.Reader)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, createErr := x509.CreateCertificate(rand.Reader, template, ca, public, caPrivate)
		if createErr != nil {
			t.Fatal(createErr)
		}
		keyDER, marshalErr := x509.MarshalPKCS8PrivateKey(private)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	}
	serverCert, serverKey := issue(11, "localhost", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey = issue(12, "client", x509.ExtKeyUsageClientAuth)
	caPath = filepath.Join(dir, "ca.crt")
	serverCertPath = filepath.Join(dir, "mtls-server.crt")
	serverKeyPath = filepath.Join(dir, "mtls-server.key")
	for path, data := range map[string][]byte{caPath: caPEM, serverCertPath: serverCert, serverKeyPath: serverKey} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caPath, serverCertPath, serverKeyPath, clientCert, clientKey
}

func TestTLSScopedTokenAndPairing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certificate, key := writeCertificate(t, dir)
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Listeners["network"] = config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AuthMode: "token", TLSCert: certificate, TLSKey: key}
	server, err := StartServer(cfg, &fakeRuntime{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	var endpoint string
	for _, listener := range server.listeners {
		if listener.Addr().Network() == "tcp" {
			endpoint = listener.Addr().String()
		}
	}
	store, err := NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := CreatePairingBundle(store, endpoint, certificate, "", "localhost", false, "", "", []string{"status"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialBundle(t.Context(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	if _, err := client.Status.GetStatus(t.Context(), &controlrpc.GetStatusRequest{}); err != nil {
		t.Fatal(err)
	}
	_, err = client.Control.Trigger(t.Context(), &controlrpc.TriggerRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("trigger error=%v", err)
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ImportPairingBundle(filepath.Join(dir, "credentials"), "server", encoded)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPairingBundle(imported)
	if err != nil || loaded.TokenID != bundle.TokenID {
		t.Fatalf("loaded=%v err=%v", loaded, err)
	}
	if err := store.Revoke(bundle.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status.GetStatus(t.Context(), &controlrpc.GetStatusRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revoked token error=%v", err)
	}
}

func TestTokenStoreNeverListsVerifier(t *testing.T) {
	t.Parallel()
	store, err := NewTokenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record, secret, err := store.Create([]string{"trigger", "status"}, nil)
	if err != nil || len(secret) != 64 || record.Hash != "" {
		t.Fatalf("record=%v secret length=%d err=%v", record, len(secret), err)
	}
	if !store.Authorize(record.ID, secret, "status") || store.Authorize(record.ID, secret, "admin") {
		t.Fatal("scope authorization failed")
	}
	stored, err := os.ReadFile(filepath.Join(store.dir, tokenFilename))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte(secret)) {
		t.Fatal("token secret was persisted")
	}
	listed, err := store.List()
	if err != nil || listed[0].Hash != "" {
		t.Fatalf("listed=%v err=%v", listed, err)
	}
}

func TestReplicationScopesAreDistinctFromControlScopes(t *testing.T) {
	t.Parallel()
	for method, want := range map[string]string{
		"/boomerangz.replication.v1.RemoteService/Capabilities":           "replicate",
		"/boomerangz.replication.v1.RemoteService/ListDatasets":           "replicate",
		"/boomerangz.replication.v1.RemoteService/InspectDatasetIdentity": "replicate",
		"/boomerangz.replication.v1.RemoteService/Receive":                "replicate",
		"/boomerangz.replication.v1.RemoteService/Prune":                  "prune",
		"/boomerangz.control.v1.StatusService/ListDatasets":               "status",
	} {
		if got := methodScope(method); got != want {
			t.Errorf("methodScope(%q)=%q, want %q", method, got, want)
		}
	}
	if _, err := normalizeScopes([]string{"replicate", "prune"}); err != nil {
		t.Fatalf("replication scopes rejected: %v", err)
	}
}

func TestTCPReplicationRequiresReplicateScope(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certificate, key := writeCertificate(t, dir)
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Listeners["replication"] = config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AuthMode: "token", TLSCert: certificate, TLSKey: key, ReplicationRoots: []string{"tank/backups"}}
	server, err := StartServerWithReplication(cfg, &fakeRuntime{}, remoteTestBackend{}, "/bin/true", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	var endpoint string
	for _, listener := range server.listeners {
		if listener.Addr().Network() == "tcp" {
			endpoint = listener.Addr().String()
		}
	}
	store, err := NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		scope string
		code  codes.Code
	}{{scope: "status", code: codes.PermissionDenied}, {scope: "replicate", code: codes.OK}} {
		bundle, err := CreatePairingBundle(store, endpoint, certificate, "", "localhost", false, "", "", []string{test.scope}, nil)
		if err != nil {
			t.Fatal(err)
		}
		connection, err := DialPairingConnection(bundle)
		if err != nil {
			t.Fatal(err)
		}
		_, callErr := remoterpc.NewRemoteServiceClient(connection).Capabilities(t.Context(), &remoterpc.CapabilitiesRequest{})
		_ = connection.Close()
		if status.Code(callErr) != test.code {
			t.Fatalf("scope %s replication code=%s err=%v", test.scope, status.Code(callErr), callErr)
		}
	}
}

func TestCertificateReloadRetainsLastCompletePair(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certificate, key := writeCertificate(t, dir)
	reloader := &certificateReloader{certFile: certificate, keyFile: key, logger: slog.New(slog.DiscardHandler), listener: "test"}
	first, err := reloader.get(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte("incomplete renewal"), 0o600); err != nil {
		t.Fatal(err)
	}
	retained, err := reloader.get(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Certificate, retained.Certificate) {
		t.Fatal("failed reload did not retain prior certificate")
	}
}

func TestMTLSAndTokenListenerRequiresBothCredentials(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ca, certificate, key, clientCertificate, clientKey := writeMTLSPKI(t, dir)
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Listeners["mtls"] = config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AuthMode: "mtls+token", TLSCert: certificate, TLSKey: key, ClientCA: ca}
	server, err := StartServer(cfg, &fakeRuntime{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	var endpoint string
	for _, listener := range server.listeners {
		if listener.Addr().Network() == "tcp" {
			endpoint = listener.Addr().String()
		}
	}
	store, err := NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	record, secret, err := store.Create([]string{"status"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundle := PairingBundle{Version: 1, Endpoint: endpoint, TrustMode: "ca", CAPEM: string(caPEM), ServerName: "localhost", ClientCert: string(clientCertificate), ClientKey: string(clientKey), TokenID: record.ID, Secret: secret, Scopes: record.Scopes}
	client, err := DialBundle(t.Context(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Connection.Close()
}

func TestManagedServerAndClientPKI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Listeners["managed"] = config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AdvertisedAddress: "localhost:7443", AuthMode: "mtls"}
	server, err := StartServer(cfg, &fakeRuntime{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(cfg.Paths.IdentityDir, "pki", "listeners", "managed", "ca.crt"),
		filepath.Join(cfg.Paths.IdentityDir, "pki", "listeners", "managed", "ca.key"),
		filepath.Join(cfg.Paths.IdentityDir, "pki", "listeners", "managed", "server.crt"),
		filepath.Join(cfg.Paths.IdentityDir, "pki", "listeners", "managed", "server.key"),
		filepath.Join(cfg.Paths.IdentityDir, "pki", "clients", "ca.crt"),
		filepath.Join(cfg.Paths.IdentityDir, "pki", "clients", "ca.key"),
	} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatalf("managed PKI file %s: %v", path, statErr)
		}
		if !info.Mode().IsRegular() {
			t.Fatalf("managed PKI file %s is not regular", path)
		}
		if filepath.Ext(path) == ".key" && info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("managed private key %s mode=%o", path, info.Mode().Perm())
		}
	}
}

func TestManagedMTLSPairingNeedsNoTokenOrClientFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	listenerConfig := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AdvertisedAddress: "localhost:7443", AuthMode: "mtls"}
	cfg.Listeners["managed"] = listenerConfig
	server, err := StartServer(cfg, &fakeRuntime{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	for _, listener := range server.listeners {
		if address, ok := listener.Addr().(*net.TCPAddr); ok {
			listenerConfig.AdvertisedAddress = "localhost:" + strconv.Itoa(address.Port)
		}
	}
	store, err := NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := CreateListenerPairing(store, cfg.Paths.IdentityDir, "managed", listenerConfig, "", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.TokenID != "" || bundle.Secret != "" || bundle.ClientCert == "" || bundle.ClientKey == "" || bundle.ClientID == "" {
		t.Fatalf("unexpected managed mTLS pairing: %+v", bundle)
	}
	connection, err := DialPairingConnection(bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if _, err := controlrpc.NewStatusServiceClient(connection).GetStatus(t.Context(), &controlrpc.GetStatusRequest{}); err != nil {
		t.Fatal(err)
	}
}
