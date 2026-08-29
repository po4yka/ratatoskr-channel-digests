## Context

The workspace OpenSpec store defines fleet ownership, public API semantics, schedule behavior,
notification integration, and single-host deployment. This repository implements only the
source-owning bounded context. Its first commit contains this plan and no runtime source.

## Goals / Non-Goals

**Goals:** preserve owner-scoped subscriptions and immutable public post revisions; isolate the
consented MTProto session; execute deterministic bounded runs; publish body-free contracts; serve
authorized loopback projections; recover after redelivery and restart; deploy as two finite roles.

**Non-Goals:** private channels, groups, dialogs, invite links, join/leave writes, continuous archive
mirroring, LLM inference, public sessions, Bot API delivery, per-user schedules, or model switching.

## Decisions

### D1: One bounded context, two processes

One shared library backs `ratatoskr-channel-digests-api` and `ratatoskr-channel-digests-worker`. The
API owns bounded owner-authorized reads and command admission on `127.0.0.1:8098`; the worker owns
JetStream consumption, MTProto acquisition, deterministic run execution, Knowledge exchange, and
schedule occurrence fan-out. Operator listeners are `9469` and `9470`.

### D2: Current schema is the only durable definition

One idempotent `schema.sql` owns provider status metadata, public channels, owner subscriptions,
immutable post revisions, run/window state, manifests/results, inbox/outbox records, and leases. No
migration tooling exists. Expected-state transitions and natural keys prevent replay or restart from
duplicating effects or regressing terminal truth.

### D3: Provider authority is worker-only and fail-closed

The worker reads a session ciphertext artifact and a separate key path with strict permissions. The
API cannot represent those settings. Startup remains unready on absence, corruption, or
reauthorization. The narrow provider interface accepts only validated public usernames and exposes
no dialog enumeration or membership mutation operation.

### D4: Immutable revisions and deterministic windows preserve evidence

Duplicate provider observations converge by channel/message/content digest; edits append a revision.
On-demand windows trail 24 hours from acceptance. Scheduled windows span the prior occurrence to the
current occurrence, capped at seven days and never before subscription activation. Runs are keyed by
owner, trigger, closed-open window, and idempotency identity.

### D5: Knowledge receives a manifest reference, never source bodies on the bus

At most 100 revisions across 20 active subscriptions enter a canonical manifest. The worker commits
its bytes and SHA-256 before publishing the typed recap request. Knowledge retrieves through a
service-authenticated owner-bound loopback route. Completion is accepted only when owner, run,
manifest, counts, and citations match durable evidence.

### D6: API and event boundaries are explicitly authorized

Every route requires a fixed service identity plus an internal owner claim. Foreign and missing
resources are indistinguishable. Mutations use typed Contracts, transactional inbox/outbox state,
and stable idempotency. JetStream topology is pre-provisioned and exact; the service never widens it.

### D7: Partial, empty, and failed runs remain distinct

An empty verified window completes without Knowledge spend. One successful channel may yield a
partial manifest with safe coverage warnings. Reauthorization or total acquisition failure cannot be
reported empty. Provider waits, unavailable Knowledge, and restart checkpoints remain durable and
finite.

### D8: Privacy and operations are bounded

Post bodies stay only in owned bounded storage and authenticated manifest responses. Events, logs,
metrics, diagnostics, and normal configuration contain references/counts/safe classes only. Expired
transient payloads may be minimized without deleting run, revision digest, or provenance evidence.

## Risks / Trade-offs

- Shared account visibility is broader than one owner's subscriptions: enforce explicit subscription
  scope and provide no general history interface.
- Flood waits may cross an occurrence: persist wait-until and resume without advancing the window.
- Edits race acquisition: pin exact immutable revisions in each manifest.
- Knowledge/source outages delay results: retain retryable state and fabricate no recap.
- Synthetic provider tests do not prove live consent or provider acceptance: record those evidence
  boundaries explicitly.

## Rollout / Rollback

Publish this service with schedule disabled after Contracts and Knowledge. Platform and Telegram are
downstream. Enable schedule only after their compatible health and TG-012 evidence. Rollback disables
schedule/traffic first, stops worker and API, and preserves subscriptions, runs, and immutable
evidence until an explicit retention action.
