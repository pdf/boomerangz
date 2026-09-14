package control

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Transport keepalive for TCP connections, on the listeners and on every
// client that dials a pairing bundle, native replication included. A long-lived
// call - a status watch or a transfer - otherwise has no way to notice a peer
// that vanished without closing the connection: nothing is sent while nothing
// changes. Either side notices within keepaliveTime plus keepaliveTimeout of
// the last activity. The local Unix socket needs none, because a dead peer
// closes it.
//
// The client and server values are one decision. A server counts a strike for
// each ping that arrives sooner than its enforcement MinTime after the previous
// one and closes the connection after a few, so a client's Time must never be
// shorter than the server's MinTime; MinTime sits below it for margin.
const (
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
	keepaliveMinTime = 20 * time.Second
)

var (
	serverKeepalive = keepalive.ServerParameters{Time: keepaliveTime, Timeout: keepaliveTimeout}
	// Neither side pings without an active call: a watch or a transfer is the
	// connection worth keeping alive, and the other calls are short.
	serverKeepaliveEnforcement = keepalive.EnforcementPolicy{MinTime: keepaliveMinTime, PermitWithoutStream: false}
	clientKeepalive            = keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout, PermitWithoutStream: false}
)

func serverKeepaliveOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.KeepaliveParams(serverKeepalive), grpc.KeepaliveEnforcementPolicy(serverKeepaliveEnforcement)}
}

func clientKeepaliveOption() grpc.DialOption {
	return grpc.WithKeepaliveParams(clientKeepalive)
}
