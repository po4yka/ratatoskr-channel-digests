# Development

The repository uses Rust 1.97, one current PostgreSQL schema, synthetic MTProto/provider fixtures,
and no migration tooling. On the shared development Mac, every compiler-backed command below runs
through `build-gate --`; CI runs the equivalent Cargo command directly.

## Full gate

`.github/workflows/ci.yml` is the command-list source of truth. A separate `deny` job runs `cargo deny --locked check` on its own, so a new RustSec advisory cannot hide a clippy or test failure behind it. Its `gate` job runs, in this order, with `CHANNEL_DIGEST_TEST_DATABASE_URL=postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest` and `CHANNEL_DIGEST_TEST_NATS_URL=nats://127.0.0.1:14224` set:

```sh
cargo fmt --all --check
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

## What a clone needs before you plan a change

A change is planned with OpenSpec, which is a CLI a clone installs for itself. Use the version
`.github/workflows/openspec.yml` pins, so your terminal and the gate answer the same:

```bash
npm install --global @fission-ai/openspec@1.10.0
```

Cross-repository behaviour lives in a store, and registering one is per-machine state that no
repository can turn on for you — the same kind of step as `git config core.hooksPath .githooks`:

```bash
git clone git@github.com:po4yka/ratatoskr-workspace.git <path>
openspec store register <path> --id ratatoskr-workspace
```

`openspec doctor` reports whether both are in place.

## The Rust skills in this repository

`.agents/skills/` holds eighteen Rust skills vendored from `po4yka/rust-skills`, and
`.claude/skills/` symlinks to them. Unlike the steps above this needs nothing from your machine: the
files are in the tree, so a fresh clone already has them.

Update them with the catalogue and never by hand:

```bash
npx skills update
```

That rewrites `.agents/skills/` and `skills-lock.json` from the catalogue. Run it in one repository,
read the diff, then apply the same change to every Ratatoskr repository whose stack is Rust.
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy differs from the others.
