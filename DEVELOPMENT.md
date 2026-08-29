# Development

The repository uses Rust 1.97, one current PostgreSQL schema, synthetic MTProto/provider fixtures,
and no migration tooling. On the shared development Mac, every compiler-backed command below runs
through `build-gate --`; CI runs the equivalent Cargo command directly.

## Full gate

```sh
CHANNEL_DIGEST_TEST_DATABASE_URL=postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest \
CHANNEL_DIGEST_TEST_NATS_URL=nats://127.0.0.1:14224 \
  build-gate -- ./tools/full-gate.sh
```

CI invokes the same `./tools/full-gate.sh` after provisioning PostgreSQL and the four exact
JetStream consumers. The script is the command-list source of truth and also enforces the 850-line
Rust source ceiling plus a content/credential-to-outbox audit.

Start disposable PostgreSQL 17 and NATS JetStream fixtures before the gate. No test needs a Telegram
credential, network session, private channel, real user/chat identifier, or source body.

## Check configuration

Both binaries accept `check-config`. It parses strict role-specific settings and, for the worker,
authenticates the bounded encrypted session file without binding ports or contacting Telegram.
Diagnostics report only stable error classes and key names.
