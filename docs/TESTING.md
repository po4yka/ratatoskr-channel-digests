# Channel Digests testing strategy

Required tests:

- Configuration: unknown prefixed settings fail naming only the key and safe reason, and the API reaches truthful readiness with valid storage/service-auth settings and no provider material.
- Subscriptions: redelivered enable/disable commands converge on one subscription, another owner cannot observe or disable it, and the 20-active-per-owner limit is enforced.
- Session and provider policy: a missing or unreadable session key keeps the worker unready with zero fake-provider calls, and an invite or message-link locator is rejected before any provider call.
- Acquisition: duplicate observations converge, an edited post appends another immutable content-digest revision, a bounded flood-wait survives a worker restart without an early retry, and deleted/unavailable/partial-channel outcomes checkpoint durably.
- Runs and manifests: a redelivered scheduled occurrence reuses the same run, window, and manifest identity; canonical manifest bytes and SHA-256 stay identical when the same revisions arrive in another order; on-demand and scheduled window bounds match the documented rules.
- API and command intake: a foreign manifest or result read returns the same absence as a nonexistent one, and a duplicate subscription or run command produces one domain effect and one replayable outcome.
- Knowledge exchange and schedule execution: a non-empty manifest yields exactly one typed recap request, a worker restart before publish acknowledgement republishes the same request identity without a new manifest, a foreign or out-of-order completion leaves the run unsettled, and a redelivered deployment occurrence still produces exactly one run per active owner.
- Result projection: a completed or partial result is returned only when the Knowledge analysis identity and result digest match durable linkage; a failed result stays local and never contacts Knowledge; a missing or foreign result is indistinguishable; an unavailable, malformed, oversized, unauthorized, or digest-inconsistent Knowledge response fails closed with a content-free `502`/`503` and no recap bytes.
- Retention, telemetry, and shutdown: a synthetic post carrying a unique marker never reaches ordinary telemetry or outbox payloads, and a shutdown mid-acquisition leaves a valid checkpoint that resumes without duplicate revision effects.

Fixtures use synthetic MTProto session material, a fake public-channel provider, and a fake Knowledge result-reader server; no test uses a real Telegram credential, a live provider session, or a real user/chat identifier.

## The gate

`.github/workflows/ci.yml` is the command-list source of truth; there is no separate gate script. Its `gate` job runs, against a disposable PostgreSQL 17 service and a disposable JetStream container it starts, with `CHANNEL_DIGEST_TEST_DATABASE_URL` and `CHANNEL_DIGEST_TEST_NATS_URL` set:

```sh
cargo fmt --all --check
cargo deny --locked check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked -- --test-threads=1
cargo build --workspace --all-targets --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
cargo build --workspace --release --locked --jobs 2
```

It then enforces the 850-line Rust source ceiling, audits that no source or credential material reaches the outbox, and runs `openspec validate --all --strict`, `openspec validate --archived`, and `git diff --check`. Locally, prefix each compiler-backed command with `build-gate --`, matching `DEVELOPMENT.md`.

## Test-first

A change is planned before it is built, and the plan is a task list in which behaviour arrives in pairs: one task adds a failing test, the next makes it pass. `openspec/config.yaml` carries that rule, which is what puts it into every planning and implementation request rather than only into this document.

The loop:

1. Write the test the scenario names. Run it. Confirm it fails, and read the failure — a test that fails because it does not compile has proved nothing about the behaviour.
2. Write the smallest change that makes it pass. Run it again.
3. Refactor only once it is green, adding no test and changing no behaviour.

Two checks stand behind this, and neither of them can see the order:

- `openspec validate --archived` fails when a change was archived with a task left unticked.
- A step in `ratatoskr-workspace/.github/workflows/fleet.yml` fails when this repository holds a manifest and a `ci.yml` that never runs a test.

`ratatoskr-workspace/docs/QUALITY_GATES.md` records why the order itself is not checkable, rather than leaving the gap to be discovered.
