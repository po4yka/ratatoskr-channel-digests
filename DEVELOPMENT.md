# Development

The repository uses Rust 1.97, one current PostgreSQL schema, synthetic MTProto/provider fixtures,
and no migration tooling. On the shared development Mac, every compiler-backed command below runs
through `build-gate --`; CI runs the equivalent Cargo command directly.

## Full gate

`.github/workflows/ci.yml` is the command-list source of truth. Its `gate` job runs, in this order, with `CHANNEL_DIGEST_TEST_DATABASE_URL=postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest` and `CHANNEL_DIGEST_TEST_NATS_URL=nats://127.0.0.1:14224` set:

```sh
cargo fmt --all --check
cargo deny --locked check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked -- --test-threads=1
cargo build --workspace --all-targets --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
cargo build --workspace --release --locked --jobs 2
```

It then enforces the 850-line Rust source ceiling and a content/credential-to-outbox audit, and runs `openspec validate --all --strict`, `openspec validate --archived` and `git diff --check`. Locally, prefix each compiler-backed command with `build-gate --`.

Start disposable PostgreSQL 17 and NATS JetStream fixtures before the gate. No test needs a Telegram
credential, network session, private channel, real user/chat identifier, or source body.

## Check configuration

Both binaries accept `check-config`. It parses strict role-specific settings and, for the worker,
authenticates the bounded encrypted session file without binding ports or contacting Telegram.
Diagnostics report only stable error classes and key names.
