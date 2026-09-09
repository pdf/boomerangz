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
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
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
	servers     []*grpc.Server
	listeners   []net.Listener
	sockets     []socketFile
	mu          sync.Mutex
	config      config.Config
	backend     runtime
	replication zfs.Executor
	zfsPath     string
	store       *TokenStore
	endpoints   map[string]*serverEndpoint
	wait        sync.WaitGroup
	logger      *slog.Logger
	reloader    *reloadHandler
	closeOnce   sync.Once
	closeErr    error
	closed      bool
}

type serverEndpoint struct {
	name       string
	definition config.ListenerConfig
	server     *grpc.Server
	listener   net.Listener
	socket     *socketFile
}

type socketFile struct {
	path   string
	device uint64
	inode  uint64
}

// Addresses returns the bound addresses for diagnostics and guarded tests.
func (s *Server) Addresses(network string) []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []string
	for _, listener := range s.listeners {
		if network == "" || listener.Addr().Network() == network {
			result = append(result, listener.Addr().String())
		}
	}
	return result
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
	managedClientAuth := false
	if (listener.AuthMode == "mtls" || listener.AuthMode == "mtls+token") && listener.ClientCA == "" {
		managedCA, err := ensureManagedClientCA(identityDir)
		if err != nil {
			return nil, err
		}
		listener.ClientCA = managedCA
		managedClientAuth = true
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
		if managedClientAuth {
			result.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("client supplied no certificate")
				}
				return authorizeManagedClient(identityDir, state.PeerCertificates[0])
			}
		}
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
	result := &Server{logger: logger, reloader: &reloadHandler{}, config: cfg.Clone(), backend: backend, replication: replication, zfsPath: zfsPath, store: store, endpoints: make(map[string]*serverEndpoint)}
	definitions := listenerDefinitions(cfg)
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
		var endpoint *serverEndpoint
		endpoint, err = result.buildEndpoint(name, definition, cfg.Paths.IdentityDir)
		if err != nil {
			_ = result.Close()
			return nil, fmt.Errorf("listener %s: %w", name, err)
		}
		result.startEndpoint(endpoint)
		result.endpoints[name] = endpoint
	}
	result.refreshViewsLocked()
	return result, nil
}

func listenerDefinitions(cfg config.Config) map[string]config.ListenerConfig {
	definitions := map[string]config.ListenerConfig{"@default": {Network: "unix", Address: cfg.Paths.SocketPath}}
	for name, listener := range cfg.Listeners {
		definitions[name] = listener
	}
	return definitions
}

func (s *Server) buildEndpoint(name string, definition config.ListenerConfig, identityDir string) (*serverEndpoint, error) {
	var listener net.Listener
	var tlsConfig *tls.Config
	var err error
	endpoint := &serverEndpoint{name: name, definition: definition}
	switch definition.Network {
	case "unix":
		listener, err = listenUnix(definition.Address)
		if err == nil {
			socket, identifyErr := identifySocket(definition.Address)
			if identifyErr != nil {
				_ = listener.Close()
				return nil, identifyErr
			}
			endpoint.socket = &socket
		}
	case "tcp":
		tlsConfig, err = tlsServerConfig(name, definition, identityDir, s.logger)
		if err == nil {
			listener, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", definition.Address)
		}
	default:
		err = fmt.Errorf("unsupported listener network %s", definition.Network)
	}
	if err != nil {
		return nil, err
	}
	endpoint.listener = listener
	var remote remoterpc.RemoteServiceServer
	if len(definition.ReplicationRoots) != 0 {
		if s.replication == nil {
			_ = closeEndpoint(endpoint)
			return nil, fmt.Errorf("replication backend is required")
		}
		remote, err = remoterpc.NewServerForRoots(s.replication, definition.ReplicationRoots, s.zfsPath)
		if err != nil {
			_ = closeEndpoint(endpoint)
			return nil, err
		}
	}
	server := grpcServer(&service{runtime: s.backend, reloader: s.reloader, reloadAllowed: name == "@default"}, remote, definition.Network == "tcp" && strings.Contains(definition.AuthMode, "token"), s.store, tlsConfig)
	endpoint.server = server
	return endpoint, nil
}

func (s *Server) startEndpoint(endpoint *serverEndpoint) {
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		if serveErr := endpoint.server.Serve(endpoint.listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			s.logger.Error("control listener stopped", "name", endpoint.name, "error", serveErr)
		}
	}()
	s.logger.Info("control listener started", "name", endpoint.name, "network", endpoint.definition.Network, "address", endpoint.definition.Address)
}

func (s *Server) refreshViewsLocked() {
	names := make([]string, 0, len(s.endpoints))
	for name := range s.endpoints {
		names = append(names, name)
	}
	slices.Sort(names)
	s.servers = s.servers[:0]
	s.listeners = s.listeners[:0]
	s.sockets = s.sockets[:0]
	for _, name := range names {
		endpoint := s.endpoints[name]
		s.servers = append(s.servers, endpoint.server)
		s.listeners = append(s.listeners, endpoint.listener)
		if endpoint.socket != nil {
			s.sockets = append(s.sockets, *endpoint.socket)
		}
	}
}

func closeEndpoint(endpoint *serverEndpoint) error {
	if endpoint == nil {
		return nil
	}
	if endpoint.server != nil {
		endpoint.server.Stop()
	}
	var result error
	if endpoint.listener != nil {
		if err := endpoint.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			result = errors.Join(result, err)
		}
	}
	if endpoint.socket != nil {
		current, err := identifySocket(endpoint.socket.path)
		if errors.Is(err, os.ErrNotExist) {
			return result
		}
		if err != nil {
			return errors.Join(result, err)
		}
		if current.device == endpoint.socket.device && current.inode == endpoint.socket.inode {
			if err := os.Remove(endpoint.socket.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
	}
	return result
}

func retireEndpoint(endpoint *serverEndpoint) error {
	if endpoint == nil || endpoint.server == nil {
		return closeEndpoint(endpoint)
	}
	done := make(chan struct{})
	go func() {
		endpoint.server.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		endpoint.server.Stop()
		<-done
	}
	return closeEndpoint(endpoint)
}

func endpointAddress(definition config.ListenerConfig) string {
	return definition.Network + "\x00" + definition.Address
}

func endpointUnchanged(previous, next config.ListenerConfig) bool {
	if !reflect.DeepEqual(previous, next) {
		return false
	}
	// The server certificate is loaded for each handshake, while client CA
	// pools are immutable once constructed and therefore need a fresh server.
	return next.Network != "tcp" || (next.AuthMode != "mtls" && next.AuthMode != "mtls+token")
}

func normalizedDefinitions(cfg config.Config) map[string]config.ListenerConfig {
	definitions := listenerDefinitions(cfg)
	names := make([]string, 0, len(definitions))
	for name := range definitions {
		names = append(names, name)
	}
	slices.Sort(names)
	seen := make(map[string]bool)
	result := make(map[string]config.ListenerConfig)
	for _, name := range names {
		definition := definitions[name]
		key := endpointAddress(definition)
		if seen[key] {
			continue
		}
		seen[key] = true
		result[name] = definition
	}
	return result
}

func (s *Server) validateEndpoint(name string, definition config.ListenerConfig, identityDir string) error {
	if definition.Network == "tcp" {
		if _, err := tlsServerConfig(name, definition, identityDir, s.logger); err != nil {
			return err
		}
	}
	if len(definition.ReplicationRoots) != 0 {
		if s.replication == nil {
			return fmt.Errorf("replication backend is required")
		}
		if _, err := remoterpc.NewServerForRoots(s.replication, definition.ReplicationRoots, s.zfsPath); err != nil {
			return err
		}
	}
	return nil
}

// Reload validates and replaces listener definitions. New addresses are bound
// before the active set changes. Same-address replacements are fully validated,
// then rebound with rollback to the previous definition on failure.
func (s *Server) Reload(cfg config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	if cfg.Paths.IdentityDir != s.config.Paths.IdentityDir {
		return fmt.Errorf("identity_dir must remain unchanged during listener reload")
	}
	nextDefinitions := normalizedDefinitions(cfg)
	staged := make(map[string]*serverEndpoint)
	for name, definition := range nextDefinitions {
		previous, exists := s.endpoints[name]
		if exists && endpointUnchanged(previous.definition, definition) {
			continue
		}
		if exists && endpointAddress(previous.definition) == endpointAddress(definition) {
			if err := s.validateEndpoint(name, definition, cfg.Paths.IdentityDir); err != nil {
				return fmt.Errorf("listener %s: %w", name, err)
			}
			continue
		}
		endpoint, err := s.buildEndpoint(name, definition, cfg.Paths.IdentityDir)
		if err != nil {
			for _, candidate := range staged {
				_ = closeEndpoint(candidate)
			}
			return fmt.Errorf("listener %s: %w", name, err)
		}
		staged[name] = endpoint
	}

	replaced := make(map[string]*serverEndpoint)
	for name, definition := range nextDefinitions {
		previous, exists := s.endpoints[name]
		if !exists || endpointUnchanged(previous.definition, definition) || endpointAddress(previous.definition) != endpointAddress(definition) {
			continue
		}
		if err := retireEndpoint(previous); err != nil {
			for _, candidate := range staged {
				_ = closeEndpoint(candidate)
			}
			return fmt.Errorf("stop listener %s: %w", name, err)
		}
		endpoint, err := s.buildEndpoint(name, definition, cfg.Paths.IdentityDir)
		if err != nil {
			rollback, rollbackErr := s.buildEndpoint(name, previous.definition, s.config.Paths.IdentityDir)
			if rollbackErr == nil {
				s.startEndpoint(rollback)
				s.endpoints[name] = rollback
			}
			for replacedName, old := range replaced {
				_ = closeEndpoint(s.endpoints[replacedName])
				restored, restoreErr := s.buildEndpoint(replacedName, old.definition, s.config.Paths.IdentityDir)
				if restoreErr == nil {
					s.startEndpoint(restored)
					s.endpoints[replacedName] = restored
				}
				rollbackErr = errors.Join(rollbackErr, restoreErr)
			}
			for _, candidate := range staged {
				_ = closeEndpoint(candidate)
			}
			s.refreshViewsLocked()
			return errors.Join(fmt.Errorf("listener %s: %w", name, err), rollbackErr)
		}
		replaced[name] = previous
		s.endpoints[name] = endpoint
	}
	for name := range replaced {
		s.startEndpoint(s.endpoints[name])
	}
	for _, endpoint := range staged {
		s.startEndpoint(endpoint)
	}

	var retired []*serverEndpoint
	for name, endpoint := range s.endpoints {
		definition, exists := nextDefinitions[name]
		if exists && reflect.DeepEqual(endpoint.definition, definition) {
			continue
		}
		if replacement := staged[name]; replacement != nil {
			s.endpoints[name] = replacement
			retired = append(retired, endpoint)
			continue
		}
		if _, wasReplaced := replaced[name]; wasReplaced {
			continue
		}
		if !exists {
			delete(s.endpoints, name)
			retired = append(retired, endpoint)
		}
	}
	for name, endpoint := range staged {
		if _, exists := s.endpoints[name]; !exists {
			s.endpoints[name] = endpoint
		}
	}
	s.config = cfg.Clone()
	s.refreshViewsLocked()
	if len(retired) != 0 {
		go func(endpoints []*serverEndpoint) {
			time.Sleep(100 * time.Millisecond)
			for _, endpoint := range endpoints {
				if err := retireEndpoint(endpoint); err != nil {
					s.logger.Error("retire reconfigured control listener", "name", endpoint.name, "error", err)
				}
			}
		}(retired)
	}
	return nil
}

// SetReloadHandler installs the transaction invoked by the Reload RPC.
func (s *Server) SetReloadHandler(handler func(context.Context) (daemonstate.ReloadResult, error)) {
	s.reloader.mu.Lock()
	s.reloader.fn = handler
	s.reloader.mu.Unlock()
}

// Close stops all RPCs and removes only sockets created by this server.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		endpoints := make([]*serverEndpoint, 0, len(s.endpoints))
		for _, endpoint := range s.endpoints {
			endpoints = append(endpoints, endpoint)
		}
		s.endpoints = make(map[string]*serverEndpoint)
		s.refreshViewsLocked()
		s.mu.Unlock()
		for _, endpoint := range endpoints {
			s.closeErr = errors.Join(s.closeErr, closeEndpoint(endpoint))
		}
		s.wait.Wait()
	})
	return s.closeErr
}
