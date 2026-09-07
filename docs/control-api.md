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
advertised_address = "backup.example.net:7443"
auth_mode = "token"
tls_cert = "/etc/boomerangz/server.crt"
tls_key = "/etc/boomerangz/server.key"
```

Bind to a management interface or firewall-restricted address. `address` is the
local bind address; `advertised_address` is the client-visible address placed in
pairing bundles and is required when Boomerangz manages the server certificate.
The daemon reloads an external certificate and key
together on new TLS handshakes. If a renewal is temporarily incomplete, it logs
the failure and retains the last usable pair; clients still enforce certificate
validity.

When `tls_cert` and `tls_key` are both omitted, Boomerangz creates a protected
private CA and renewable server certificate under `paths.identity_dir`. The
certificate covers the hostname in `advertised_address`. Existing pairings keep
working across leaf-certificate renewal because the managed CA is retained.

Create one token per client and transfer the resulting JSON bundle through a
trusted channel. The token secret appears only in this output:

```sh
boomerangz auth pairing create \
  --listener remote_control \
  --scope status >laptop-pairing.json
```

Externally managed certificates use the client's system roots when `pairing_ca`
is omitted. Set `pairing_ca` on the listener to embed an explicit private CA.
`pairing_pin_certificate = true` additionally pins the certificate public key;
CA-chain, hostname, validity, and usage verification still apply.

On the client, import and use the bundle:

```sh
boomerangz auth pairing import backup laptop-pairing.json
boomerangz status --credential backup
```

Imported bundles are stored in `paths.credentials_dir` with mode `0600` and
contain client authorization material. Treat them as secrets. List or revoke
server-side tokens by identifier:

```sh
boomerangz auth token list
boomerangz auth token revoke TOKEN_ID
```

Available scopes are `status`, `trigger`, `replicate`, `prune`, and `admin`. `status` permits status
snapshots, watches, and dataset listing; `trigger` permits snapshot triggers and
reconciliation hints; `replicate` permits scoped destination inspection,
reconciliation, and receive operations; `prune` is reserved for scoped remote
pruning; `admin` includes all methods, including clean.

For `mtls` or `mtls+token`, setting `client_ca` selects an externally managed
client CA; supply that client's `--client-cert` and `--client-key` when creating
its pairing. If `client_ca` is omitted, Boomerangz manages a client CA and issues
a distinct client certificate automatically. Pure mTLS pairings contain no
token. `mtls+token` pairings contain both credentials and the server requires
both.
