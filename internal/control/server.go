package control

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server owns all configured local-control listeners.
type Server struct {
	servers   []*grpc.Server
	listeners []net.Listener
	sockets   []socketFile
	wait      sync.WaitGroup
	logger    *slog.Logger
	closeOnce sync.Once
	closeErr  error
}

type socketFile struct {
	path   string
	device uint64
	inode  uint64
}

func identifySocket(path string) (socketFile, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return socketFile{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 {
		return socketFile{}, fmt.Errorf("control socket identity unavailable")
	}
	return socketFile{path: path, device: uint64(stat.Dev), inode: stat.Ino}, nil
}

type peerListener struct{ net.Listener }

func (l peerListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("unix peer credentials unavailable")
	}
	var credentialErr error
	raw, err := syscallConn.SyscallConn()
	if err == nil {
		err = raw.Control(func(fd uintptr) { _, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	}
	if err != nil || credentialErr != nil {
		_ = conn.Close()
		return nil, errors.Join(err, credentialErr)
	}
	return conn, nil
}

func listenUnix(path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket %s", path)
		}
		probe, dialErr := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(context.Background(), "unix", path)
		if dialErr == nil {
			_ = probe.Close()
			return nil, fmt.Errorf("control socket is already active: %s", path)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			return nil, fmt.Errorf("cannot verify existing control socket %s: %w", path, dialErr)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return peerListener{listener}, nil
}

type certificateReloader struct {
	mu          sync.Mutex
	certFile    string
	keyFile     string
	certificate *tls.Certificate
	logger      *slog.Logger
	listener    string
	lastFailure string
}

func (r *certificateReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loaded, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err == nil {
		r.certificate = &loaded
		r.lastFailure = ""
	} else if r.certificate != nil && err.Error() != r.lastFailure {
		r.logger.Error("retain previous control TLS identity after reload failure", "listener", r.listener, "error", err)
		r.lastFailure = err.Error()
	}
	if r.certificate == nil {
		return nil, err
	}
	return r.certificate, nil
}

func tlsServerConfig(name string, listener config.ListenerConfig, identityDir string, logger *slog.Logger) (*tls.Config, error) {
	if listener.TLSCert == "" {
		managed, err := ensureManagedServerIdentity(identityDir, name, listener.AdvertisedAddress)
		if err != nil {
			return nil, err
		}
		listener.TLSCert, listener.TLSKey = managed.cert, managed.key
	}
	if (listener.AuthMode == "mtls" || listener.AuthMode == "mtls+token") && listener.ClientCA == "" {
		managedCA, err := ensureManagedClientCA(identityDir)
		if err != nil {
			return nil, err
		}
		listener.ClientCA = managedCA
	}
	reloader := &certificateReloader{certFile: listener.TLSCert, keyFile: listener.TLSKey, logger: logger, listener: name}
	if _, err := reloader.get(nil); err != nil {
		return nil, err
	}
	result := &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: reloader.get}
	if listener.AuthMode == "mtls" || listener.AuthMode == "mtls+token" {
		pem, err := os.ReadFile(listener.ClientCA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client_ca contains no certificates")
		}
		result.ClientAuth = tls.RequireAndVerifyClientCert
		result.ClientCAs = pool
	}
	return result, nil
}

func methodScope(method string) string {
	if strings.Contains(method, ".replication.v1.RemoteService/") {
		if strings.HasSuffix(method, "/Prune") {
			return "prune"
		}
		return "replicate"
	}
	if strings.HasSuffix(method, "/GetStatus") || strings.HasSuffix(method, "/WatchStatus") || strings.HasSuffix(method, "/ListDatasets") {
		return "status"
	}
	if strings.HasSuffix(method, "/Trigger") || strings.HasSuffix(method, "/Reconcile") {
		return "trigger"
	}
	return "admin"
}

func authorize(ctx context.Context, store *TokenStore, method string) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return status.Error(codes.Unauthenticated, "token required")
	}
	id, secret, ok := strings.Cut(strings.TrimPrefix(values[0], "Bearer "), ".")
	if !ok || !store.Authorize(id, secret, methodScope(method)) {
		return status.Error(codes.PermissionDenied, "token is invalid or lacks scope")
	}
	return nil
}

func grpcServer(service *service, remote remoterpc.RemoteServiceServer, tokenAuth bool, store *TokenStore, tlsConfig *tls.Config) *grpc.Server {
	var options []grpc.ServerOption
	if tlsConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	if tokenAuth {
		options = append(options,
			grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if err := authorize(ctx, store, info.FullMethod); err != nil {
					return nil, err
				}
				return handler(ctx, req)
			}),
			grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if err := authorize(stream.Context(), store, info.FullMethod); err != nil {
					return err
				}
				return handler(srv, stream)
			}),
		)
	}
	server := grpc.NewServer(options...)
	controlrpc.RegisterStatusServiceServer(server, service)
	controlrpc.RegisterControlServiceServer(server, service)
	if remote != nil {
		remoterpc.RegisterRemoteServiceServer(server, remote)
	}
	return server
}

// StartServer binds the default Unix socket plus configured named listeners.
func StartServer(cfg config.Config, backend runtime, logger *slog.Logger) (*Server, error) {
	return startServer(cfg, backend, nil, "", logger)
}

// StartServerWithReplication additionally exposes scoped replication services
// on TCP listeners that configure replication roots.
func StartServerWithReplication(cfg config.Config, backend runtime, replication zfs.Executor, zfsPath string, logger *slog.Logger) (*Server, error) {
	return startServer(cfg, backend, replication, zfsPath, logger)
}

func startServer(cfg config.Config, backend runtime, replication zfs.Executor, zfsPath string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	store, err := NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		return nil, err
	}
	result := &Server{logger: logger}
	definitions := map[string]config.ListenerConfig{"@default": {Network: "unix", Address: cfg.Paths.SocketPath}}
	for name, listener := range cfg.Listeners {
		definitions[name] = listener
	}
	names := make([]string, 0, len(definitions))
	for name := range definitions {
		names = append(names, name)
	}
	slices.Sort(names)
	seen := make(map[string]bool)
	for _, name := range names {
		definition := definitions[name]
		key := definition.Network + "\x00" + definition.Address
		if seen[key] {
			continue
		}
		seen[key] = true
		var listener net.Listener
		var tlsConfig *tls.Config
		switch definition.Network {
		case "unix":
			listener, err = listenUnix(definition.Address)
			if err == nil {
				var socket socketFile
				socket, err = identifySocket(definition.Address)
				if err == nil {
					result.sockets = append(result.sockets, socket)
				}
			}
		case "tcp":
			tlsConfig, err = tlsServerConfig(name, definition, cfg.Paths.IdentityDir, logger)
			if err == nil {
				listener, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", definition.Address)
			}
		default:
			err = fmt.Errorf("unsupported listener network %s", definition.Network)
		}
		if err != nil {
			_ = result.Close()
			return nil, fmt.Errorf("listener %s: %w", name, err)
		}
		var remote remoterpc.RemoteServiceServer
		if len(definition.ReplicationRoots) != 0 {
			if replication == nil {
				_ = listener.Close()
				_ = result.Close()
				return nil, fmt.Errorf("listener %s: replication backend is required", name)
			}
			remote, err = remoterpc.NewServerForRoots(replication, definition.ReplicationRoots, zfsPath)
			if err != nil {
				_ = listener.Close()
				_ = result.Close()
				return nil, fmt.Errorf("listener %s: %w", name, err)
			}
		}
		server := grpcServer(&service{runtime: backend}, remote, definition.Network == "tcp" && strings.Contains(definition.AuthMode, "token"), store, tlsConfig)
		result.listeners = append(result.listeners, listener)
		result.servers = append(result.servers, server)
		result.wait.Add(1)
		go func() {
			defer result.wait.Done()
			if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
				logger.Error("control listener stopped", "name", name, "error", serveErr)
			}
		}()
		logger.Info("control listener started", "name", name, "network", definition.Network, "address", definition.Address)
	}
	return result, nil
}

// Close stops all RPCs and removes only sockets created by this server.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		for _, server := range s.servers {
			server.Stop()
		}
		for _, listener := range s.listeners {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.closeErr = errors.Join(s.closeErr, err)
			}
		}
		s.wait.Wait()
		for _, socket := range s.sockets {
			current, err := identifySocket(socket.path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				s.closeErr = errors.Join(s.closeErr, err)
				continue
			}
			if current.device != socket.device || current.inode != socket.inode {
				continue
			}
			if err := os.Remove(socket.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.closeErr = errors.Join(s.closeErr, err)
			}
		}
	})
	return s.closeErr
}
