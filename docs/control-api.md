# Status and remote control

The daemon exposes its control API on `/run/boomerangz/boomerangz.sock` by
default. The socket is mode `0660`; access is limited by the containing
directory, socket ownership, and Unix peer credentials. Keep administrative
users in the intended service group rather than making the socket world
accessible.

Show one current status snapshot:

```sh
boomerangz status
```

In a terminal this displays managed roots, their next snapshot times, queue
pressure, and the latest state of each job. Redirected output is one JSON
object. Watch mode redraws a terminal display, or emits newline-delimited JSON
when redirected:

```sh
boomerangz status --watch
boomerangz status --watch --interval 5s >status.jsonl
```

Queue an immediate snapshot for all active roots, or selected roots:

```sh
boomerangz trigger
boomerangz trigger pool/data pool/home
```

Requests are accepted only for active scheduling roots and use the daemon's
normal deduplication, worker limits, ownership checks, and just-in-time target
verification. A trigger does not bypass an operation already queued or running.

`boomerangz dataset clean` automatically uses the local control socket when the
daemon is running. The daemon blocks active or related scheduling scopes, waits
for running work to become quiescent, and revalidates the normal clean plan.
Preview remains read-only and `--apply` remains required. In the current command
surface, adoption and installation identity recovery refuse to run while the
configured daemon socket exists.

## Optional TCP listeners

TCP control is disabled unless a listener is configured. Every TCP listener
uses TLS 1.3 or newer and must use `token`, `mtls`, or `mtls+token`
authentication. For example:

```toml
[listeners.remote_control]
network = "tcp"
address = "192.0.2.10:7443"
auth_mode = "token"
tls_cert = "/etc/boomerangz/server.crt"
tls_key = "/etc/boomerangz/server.key"
```

Bind to a management interface or firewall-restricted address. A wildcard bind
is allowed for the listener, but token pairing then requires `--endpoint` with
the client-visible host and port. The daemon reloads the certificate and key
together on new TLS handshakes. If a renewal is temporarily incomplete, it logs
the failure and retains the last usable pair; clients still enforce certificate
validity.

Create one token per client and transfer the resulting JSON bundle through a
trusted channel. The token secret appears only in this output:

```sh
boomerangz auth token create \
  --listener remote_control \
  --endpoint backup.example.net:7443 \
  --server-name backup.example.net \
  --scope status >laptop-pairing.json
```

Without a trust flag, the bundle pins the certificate's public key and verifies
its name and validity period. Use `--ca /path/to/ca.pem` to embed an explicit CA
chain, or `--system-ca` for a publicly trusted certificate. CA trust permits
normal certificate/key renewal without re-pairing while the name and chain
remain valid. System roots are never silently substituted for the selected
trust mode.

On the client, import and use the bundle:

```sh
boomerangz auth token import backup laptop-pairing.json
boomerangz status --credential backup
```

Imported bundles are stored in `paths.credentials_dir` with mode `0600` and
contain client authorization material. Treat them as secrets. List or revoke
server-side tokens by identifier:

```sh
boomerangz auth token list
boomerangz auth token revoke TOKEN_ID
```

Available scopes are `status`, `trigger`, and `admin`. `status` permits status
snapshots, watches, and dataset listing; `trigger` permits snapshot triggers and
reconciliation hints; `admin` includes all methods, including clean.

For `mtls`, set `client_ca` to the CA that issues accepted client certificates.
For `mtls+token`, also supply `--client-cert` and `--client-key` when creating a
pairing bundle; those credentials are embedded in the protected bundle. Pure
mTLS clients can use any gRPC client configured with the accepted certificate,
private key, server trust, and the versioned API in
`proto/boomerangz/control/v1/control.proto`.
