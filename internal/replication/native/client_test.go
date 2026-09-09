package native

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/control"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type testBackend struct{ zfs.Executor }

func (testBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, {Name: "tank/a", Type: zfs.Filesystem}, {Name: "tank/b", Type: zfs.Filesystem}, {Name: "other/private", Type: zfs.Filesystem}}, nil
}
func (testBackend) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 10, Pool: "tank", PoolGUID: 11}, nil
}
func (testBackend) InspectState(context.Context, string, bool) (zfs.State, error) {
	return zfs.State{Received: map[string]map[string]string{}, ResumeTokens: map[string]string{}}, nil
}
func (testBackend) CheckPermissions(context.Context, string, []string) error       { return nil }
func (testBackend) SetProperties(context.Context, string, map[string]string) error { return nil }
func (testBackend) InheritProperty(context.Context, string, string) error          { return nil }
func (testBackend) AbortReceive(context.Context, string) error                     { return nil }
func (testBackend) DestroyDataset(context.Context, string, bool) error             { return nil }

func testTLSCertificate(t *testing.T) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private, Leaf: leaf}, leaf
}

func TestAuthenticatedNativeEndpointNegotiatesSharedService(t *testing.T) {
	t.Parallel()
	certificate, leaf := testTLSCertificate(t)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service, err := remoterpc.NewServerForRoots(testBackend{}, []string{"tank/a", "tank/b"}, "/bin/true")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}})))
	remoterpc.RegisterRemoteServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	digest := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	bundle := control.PairingBundle{Version: 1, Endpoint: listener.Addr().String(), TrustMode: "pin", ServerName: "localhost", SPKIPin: base64.RawStdEncoding.EncodeToString(digest[:]), TokenID: "id", Secret: "secret", Scopes: []string{"replicate"}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	endpoint, err := Open(ctx, bundle, "tank/a", "/bin/true")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	inventory, err := endpoint.Executor().ListDatasets(ctx)
	if err != nil || len(inventory) != 3 || inventory[0].Name != "tank" || inventory[2].Name != "tank/b" {
		t.Fatalf("inventory=%v err=%v", inventory, err)
	}
	if _, err := endpoint.Executor().InspectDatasetIdentity(ctx, "other/private"); err == nil {
		t.Fatal("native endpoint exposed identity outside configured roots")
	}
	if err := endpoint.Executor().CheckPermissions(ctx, "tank/a", []string{"receive:append"}); err != nil {
		t.Fatalf("permission preflight: %v", err)
	}
	reseed, ok := endpoint.Executor().(zfs.ReseedExecutor)
	if !ok {
		t.Fatal("native endpoint does not expose reseed operations")
	}
	if err := reseed.AbortReceive(ctx, "tank/a"); err != nil {
		t.Fatalf("abort receive: %v", err)
	}
	if err := reseed.DestroyDataset(ctx, "tank/a", true); err != nil {
		t.Fatalf("destroy dataset: %v", err)
	}
	if err := reseed.DestroyDataset(ctx, "other/private", true); err == nil {
		t.Fatal("native endpoint destroyed a dataset outside its configured root")
	}
	if got := endpoint.CanonicalTarget(); got != "native://127.0.0.1:"+strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)+"/tank/a" {
		t.Fatalf("canonical target=%q", got)
	}
}
