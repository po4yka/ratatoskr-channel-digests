#!/bin/sh
set -eu

: "${CHANNEL_DIGEST_TEST_DATABASE_URL:?set the disposable PostgreSQL fixture URL}"
: "${CHANNEL_DIGEST_TEST_NATS_URL:?set the disposable NATS fixture URL}"

cargo fmt --all --check
cargo deny --locked check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked -- --test-threads=1
cargo build --workspace --all-targets --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
cargo build --workspace --release --locked --jobs 2

oversized="$({ find src services tests -name '*.rs' -print0 | xargs -0 wc -l; } | awk '$2 != "total" && $1 > 850 { print $0 }')"
if [ -n "$oversized" ]; then
  echo "$oversized"
  exit 1
fi

if rg -n 'outbox_messages[^\n]*(body|session|api_hash|session_key)' src schema.sql; then
  echo 'privacy audit found source or credential material entering the outbox'
  exit 1
fi

openspec validate --all --strict
openspec validate --archived
git diff --check
