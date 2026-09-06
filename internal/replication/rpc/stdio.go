package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type stdioAddress string

func (a stdioAddress) Network() string { return "ssh-command" }
func (a stdioAddress) String() string  { return string(a) }

type stdioConn struct {
	reader  io.Reader
	writer  io.Writer
	closed  chan struct{}
	onClose func()
	once    sync.Once
}

func (c *stdioConn) Read(data []byte) (int, error)  { return c.reader.Read(data) }
func (c *stdioConn) Write(data []byte) (int, error) { return c.writer.Write(data) }
func (c *stdioConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.onClose()
	})
	return nil
}
func (c *stdioConn) LocalAddr() net.Addr              { return stdioAddress("remote") }
func (c *stdioConn) RemoteAddr() net.Addr             { return stdioAddress("ssh-client") }
func (c *stdioConn) SetDeadline(time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(time.Time) error { return nil }

type singleListener struct {
	conn     net.Conn
	closed   chan struct{}
	mu       sync.Mutex
	accepted bool
	once     sync.Once
}

func (l *singleListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.accepted {
		l.accepted = true
		conn := l.conn
		l.mu.Unlock()
		return conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}
func (l *singleListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (l *singleListener) Addr() net.Addr { return stdioAddress("ssh-shell") }

// ServeStdio serves exactly one gRPC connection over an SSH command's standard
// input and output. No socket or forwarded port is opened.
func ServeStdio(ctx context.Context, service RemoteServiceServer, input io.Reader, output io.Writer) error {
	if service == nil || input == nil || output == nil {
		return errors.New("remote service, input, and output are required")
	}
	listener := &singleListener{closed: make(chan struct{})}
	connection := &stdioConn{reader: input, writer: output, closed: make(chan struct{}), onClose: func() { _ = listener.Close() }}
	listener.conn = connection
	server := grpc.NewServer(grpc.MaxRecvMsgSize(MaxStreamChunk+64*1024), grpc.MaxSendMsgSize(32*1024*1024))
	RegisterRemoteServiceServer(server, service)
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
			_ = listener.Close()
		case <-stopped:
		}
	}()
	err := server.Serve(listener)
	close(stopped)
	if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
