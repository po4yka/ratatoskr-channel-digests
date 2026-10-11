# channel-digest-service Specification

## Purpose
Defines repository-local behavior for safe public-channel acquisition, immutable digest execution, authenticated source access, and restart-safe producer roles.

## Requirements

### Requirement: Configuration and process roles are finite and separate

The service SHALL load a strict finite configuration for database pools, API/operator listeners,
provider/session paths, request/source sizes, concurrency, retry, schedule, retention, and shutdown.
Unknown or invalid settings SHALL fail without exposing values. The API role SHALL listen on
`127.0.0.1:8098` with operator port `9469`; the worker operator port SHALL be `9470`. The API role
SHALL contain no provider credential or session access.

#### Scenario: API boots without provider material

- **WHEN** the API starts with valid storage and service-auth settings but no MTProto settings
- **THEN** it reaches truthful readiness and its effective configuration contains no provider secret

#### Scenario: Unknown setting is refused safely

- **WHEN** configuration contains an unknown prefixed key with a unique secret-looking value
- **THEN** startup fails naming only the key and safe reason, not the value

### Requirement: Owned state is idempotent and owner scoped

One current schema SHALL contain provider status metadata, public channels, owner subscriptions,
immutable post revisions, digest runs/windows, manifests/results, inbox/outbox records, and leases.
Subscription usernames SHALL normalize to lowercase, preserve first activation, converge enable or
disable replays, and enforce at most 20 active subscriptions per owner. Foreign reads SHALL behave as
absence and disabling SHALL retain historical run evidence.

#### Scenario: Subscribe redelivery converges

- **WHEN** one owner repeats an identical enable command for a public username
- **THEN** one active subscription with its original effective time remains

#### Scenario: Another owner cannot observe the subscription

- **WHEN** a different owner reads or disables that username
- **THEN** the result is indistinguishable from an absent owner-scoped subscription

### Requirement: Provider session and channel policy fail closed

Only the worker SHALL read separate session-ciphertext and key files with approved permissions.
Absence, corruption, unsafe permissions, or reauthorization SHALL keep provider work unready without
logging bytes or paths. Provider operations SHALL accept only public usernames and SHALL expose no
private/group/dialog/invite/message-link/numeric-peer or join/leave capability.

#### Scenario: Missing session key prevents provider calls

- **WHEN** ciphertext exists but the configured key is absent or unreadable
- **THEN** the worker remains unready and the fake provider records zero calls

#### Scenario: Invite locator is rejected before resolution

- **WHEN** a command supplies a Telegram invite or message link
- **THEN** it fails with the stable invalid-channel class and no provider call occurs

### Requirement: Acquisition appends immutable revisions with partial truth

Acquisition SHALL page only the eligible public-channel closed-open window under finite calls,
timeouts, and retries. Duplicate observations SHALL converge; changed normalized bytes SHALL append
an immutable content-digest revision. Deleted, unavailable, flood-wait, timeout, reconnect, and
partial-channel outcomes SHALL checkpoint durably and resume after restart without calling a failed
channel empty or erasing successful channels.

#### Scenario: Edited observation creates another revision

- **WHEN** the same channel/message is observed with changed normalized bytes
- **THEN** both content digests remain immutable and later run evidence selects the observed revision

#### Scenario: Flood wait survives restart

- **WHEN** the provider returns a bounded flood-wait and the worker restarts before it expires
- **THEN** no early provider retry occurs and execution resumes from the persisted wait/checkpoint

### Requirement: Runs and manifests are deterministic and terminally monotonic

On-demand runs SHALL use the trailing 24 hours ending at acceptance. Scheduled runs SHALL use the
previous occurrence through the current occurrence, capped at seven days and never before activation.
Natural-key replay SHALL reuse one run. A canonical manifest SHALL select at most 100 revisions across
20 subscriptions in stable order, include exact window/count/digest/linkage evidence, and be byte
identical across input order. Empty runs SHALL bypass Knowledge; terminal state SHALL not regress.

#### Scenario: Scheduled occurrence is redelivered

- **WHEN** one owner and occurrence command is delivered again after uncertain acknowledgement
- **THEN** the same run, window, manifest identity, and terminal result are reused

#### Scenario: Input order changes

- **WHEN** the same selected revisions reach manifest construction in another order
- **THEN** canonical bytes and SHA-256 remain identical

### Requirement: API and command intake enforce service and owner authority

Loopback subscription, manifest, result, and command routes SHALL require the configured service
identity and owner scope, enforce finite body/page bounds, and return explicit DTOs without provider
credentials, raw errors, or unrelated source content. Foreign and missing reads SHALL be identical.
Typed command intake SHALL validate Contracts, deduplicate transport and semantic identities, and
commit domain mutation plus outbox operation reports atomically.

#### Scenario: Foreign manifest read is hidden

- **WHEN** an authenticated service requests a manifest under the wrong owner
- **THEN** it receives the same scoped absence response as a nonexistent manifest

#### Scenario: Duplicate command has one effect

- **WHEN** the same subscription or run command is redelivered under equivalent identity
- **THEN** one domain effect and one replayable operation outcome remain

### Requirement: Knowledge exchange and schedule execution are replay safe

A non-empty committed manifest SHALL cause exactly one body-free typed Knowledge recap request.
Completion or failure SHALL settle only when owner, run, manifest digest, counts, result identity, and
citation membership match durable evidence. Duplicate, foreign, or out-of-order facts SHALL not
regress state. The service SHALL consume the typed deployment-wide schedule occurrence command
through a dedicated durable pull consumer. Occurrence intake and all active-owner natural-key runs
SHALL commit with one inbox decision; replay SHALL create no additional runs. The service SHALL
compute each owner window from subscription activation and the previous/current occurrence grid
points and SHALL not emit Telegram delivery events directly.

#### Scenario: Deployment occurrence is redelivered

- **WHEN** Platform redelivers one occurrence envelope after uncertain acknowledgement
- **THEN** the inbox replays one decision and each active owner still has exactly one run for that occurrence

#### Scenario: Worker stops after manifest commit

- **WHEN** the worker restarts before recap-request publication acknowledgement
- **THEN** it republishes the same typed request identity without another manifest or inference identity

#### Scenario: Foreign completion is received

- **WHEN** a completion names another owner or manifest digest
- **THEN** the run remains unsettled and no result is exposed

### Requirement: Retention, telemetry, and shutdown preserve privacy and recovery

Raw post bodies SHALL remain only in owned bounded storage and authenticated manifest responses.
Session bytes SHALL never enter PostgreSQL, events, logs, metrics, fixtures, or diagnostics. Expired
transient payloads SHALL be minimized without deleting immutable revision digests, run/provenance
evidence, or terminal linkage. Both processes SHALL drain, checkpoint, and join within the finite
shutdown budget while readiness fails immediately.

#### Scenario: Content marker triggers failure

- **WHEN** a synthetic post containing a unique marker reaches a bounded failure path
- **THEN** captured ordinary telemetry and outbox payloads contain none of the marker

#### Scenario: Shutdown interrupts acquisition

- **WHEN** the worker receives termination during a page boundary
- **THEN** readiness fails, the durable checkpoint remains valid, and restart resumes without duplicate revision effects

### Requirement: Completed digest results project Knowledge-owned recaps without copying them

An owner-authorized result read SHALL resolve the local terminal result before contacting Knowledge.
Completed and partial results SHALL be returned only after the Knowledge analysis identity and exact
SHA-256 result digest match the immutable linkage accepted from the completion fact. The successful
projection SHALL contain an explicit local result envelope and the closed recap returned by
Knowledge, SHALL remain bounded and non-cacheable, and SHALL not cause recap narrative to be stored
by Channel Digests. Failed results SHALL return only their safe local failure projection and SHALL
not contact Knowledge. Missing or foreign local results SHALL remain indistinguishable. An absent,
unauthorized, malformed, oversized, unavailable, or integrity-inconsistent Knowledge response SHALL
fail closed with a stable content-free error and no partial recap.

#### Scenario: Completed result is projected through verified Knowledge linkage

- **WHEN** an authorized owner reads a completed or partial result whose Knowledge analysis identity and result digest match the durable completion fact
- **THEN** the service returns the explicit local result envelope and exact Knowledge-owned recap with `Cache-Control: no-store` without persisting recap narrative locally

#### Scenario: Foreign result is rejected before Knowledge access

- **WHEN** an authenticated service reads an existing result under another owner
- **THEN** it receives the same scoped `404` as a nonexistent result and Knowledge receives no request

#### Scenario: Failed result remains a safe local projection

- **WHEN** an authorized owner reads a terminal failed result
- **THEN** the service returns only the result identity, run identity, failed outcome, and safe failure class without contacting Knowledge or exposing recap fields

#### Scenario: Knowledge projection is unusable

- **WHEN** Knowledge is unavailable or returns an absent, unauthorized, malformed, oversized, foreign, or digest-inconsistent projection
- **THEN** the result read returns a stable `502` or `503` class with no recap bytes, upstream diagnostics, secret material, or partial success

### Requirement: Scheduled and on-demand runs both execute

Every accepted run SHALL carry the operation id it was accepted under, and the executor SHALL select pending runs from the run table alone. A schedule occurrence SHALL fan out one run per active owner, each carrying the occurrence operation id, and those runs SHALL execute to a recap request exactly like on-demand runs.

#### Scenario: Scheduled run executes

- **WHEN** an authoritative schedule occurrence is accepted for an owner with an active subscription and the executor runs once with a provider that returns one post
- **THEN** the run reaches `waiting_recap`, one manifest exists and the recap request carries the occurrence operation id

### Requirement: The manifest is the canonical contract artifact served verbatim

A run that selects at least one source SHALL store the exact canonical text of a valid `ChannelDigestManifest`, whose sha256 is the stored digest, and the API SHALL answer a manifest read with exactly those bytes. A read SHALL answer 404 with an empty body unless the owner, the run id claim and the digest claim all match the stored row. A run that selects no source SHALL complete without a manifest.

#### Scenario: Stored manifest decodes and hashes to its digest

- **WHEN** a run selects two revisions of one message and one other post
- **THEN** the stored text decodes with `ChannelDigestManifest::from_canonical_bytes`, the later body has revision 2 and the sha256 of the text equals the stored digest

#### Scenario: Claim mismatch is hidden

- **WHEN** a manifest is read with a wrong run id, a wrong digest or another owner
- **THEN** the response is 404 with an empty body

### Requirement: Every terminal state reports a typed operation status

Each on-demand operation SHALL end with a valid `OperationReported` whose status is terminal, written in the same transaction as the state change it reports, and a scheduled run SHALL report nothing. A subscription refused by the active-subscription limit SHALL fail its operation with the code `channel_digest.subscription_limit_reached`, mark the inbox row failed and acknowledge the message. A failed acquisition or an unbuildable manifest SHALL fail the run and its operation instead of being retried forever. An occurrence SHALL report `succeeded` on its own operation.

#### Scenario: Twenty-first subscription fails once

- **WHEN** a subscription command would create a twenty-first active subscription
- **THEN** the command is acknowledged, the inbox row is failed and exactly one failed report with the limit code is queued

#### Scenario: Partial completion carries a warning and the result

- **WHEN** Knowledge completes a run with omitted sources
- **THEN** the report is `partially_succeeded` with the warning `channel_digest.context_omitted` and one `channel_digest.result` reference

### Requirement: The bus speaks contract envelopes and authenticates

Published messages SHALL be contract `CommandEnvelope` and `EventEnvelope` values carrying the operation as correlation and the owner as tenant. The worker SHALL connect with the nkey seed named by `RATATOSKR__BUS__NKEY_SEED_PATH` when configured, an absolute path accepted by the worker role only. A schedule occurrence SHALL take its operation from the envelope correlation id and its owner from the envelope tenant, and an occurrence missing either SHALL be terminated.

#### Scenario: Occurrence is accepted from a Platform envelope

- **WHEN** a Platform occurrence envelope with a correlation operation and a tenant is delivered
- **THEN** every fanned-out run carries that operation id and one succeeded report exists for it

### Requirement: The worker registers its own schedule

At start the worker SHALL queue one schedule registration for the configured owner, cron and enablement, once per distinct configuration, and SHALL register nothing when no owner is configured.

#### Scenario: Configuration changes re-register

- **WHEN** the registration is queued twice with the same configuration and once with another cron
- **THEN** two registration rows exist

### Requirement: The API serves readiness and typed views

The API SHALL serve `/ready` behind the bearer alone, answering 200 while the database answers and 503 otherwise, SHALL list results owner-scoped and newest first without recap content, and SHALL serialize subscriptions and results through the contract view types.

#### Scenario: Readiness requires the bearer only

- **WHEN** `/ready` is requested with the bearer and no owner header, with no bearer, and with a closed pool
- **THEN** the answers are 200, 401 and 503

### Requirement: A run that does not finish by its deadline fails

A run in `accepted`, `acquiring` or `waiting_recap` whose last update is older than `RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS` SHALL become `failed` with the safe class `deadline_exceeded`, and an on-demand run SHALL queue exactly one `failed` report with the code `channel_digest.run_deadline_exceeded`, retryable, in the same transaction. A scheduled run SHALL fail without a report. A later Knowledge fact for such a run SHALL change no state and queue no second report.

#### Scenario: Overdue on-demand run fails once

- **WHEN** an on-demand run in each non-terminal state is older than the deadline and the reaper runs twice
- **THEN** each run is `failed` with `deadline_exceeded`, exactly one failed report with the deadline code exists per operation, and a younger run is untouched

#### Scenario: Late fact changes nothing

- **WHEN** Knowledge completes or fails a run the reaper already failed
- **THEN** the run stays `failed` and no further report is queued

### Requirement: The run deadline is a finite setting

`RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS` SHALL default to 1800 and be refused outside 60 to 86400.

#### Scenario: Deadline range

- **WHEN** the setting is 59, 60, 86400 and 86401
- **THEN** the first and last are refused and the others are accepted

### Requirement: An attributable invalid command is reported

A run or subscription command whose envelope tenant is a user, whose payload names an operation and whose payload owner equals that tenant, but which fails validation or an invariant, SHALL be recorded as a failed inbox row, queue one `failed` report with the code `channel_digest.command_invalid` that is not retryable, and be acknowledged. A command that cannot be attributed SHALL be terminated without a report.

#### Scenario: Invalid but attributable run

- **WHEN** a run envelope whose payload names its operation and owner violates a contract rule
- **THEN** exactly one failed report with the invalid-command code exists, the inbox row is failed and the message is acknowledged

#### Scenario: Unattributable command

- **WHEN** a payload does not decode and names no operation, or the producer is foreign
- **THEN** the message is terminated and no report exists

### Requirement: List pages are clamped to the configured ceiling

The subscription and result lists SHALL answer a `page_size` from 1 to 100 with at most the configured `RATATOSKR__LIMITS__PAGE_SIZE` items, and SHALL answer 400 for zero, a value above 100 or a non-number.

#### Scenario: Larger request is clamped

- **WHEN** the ceiling is 25 and a list is requested with `page_size=100`
- **THEN** the answer is 200 with at most 25 items

### Requirement: Scheduled runs use the configured language

`RATATOSKR__SCHEDULE__OUTPUT_LANGUAGE` SHALL accept `ru` or `en` for the worker role only, default to `ru`, require a schedule owner like the other schedule keys, and be the `output_language` of every run an occurrence creates.

#### Scenario: English schedule

- **WHEN** the worker is configured with `en` and an occurrence fans out
- **THEN** every created run has the language `en`

### Requirement: Shipped examples load

`deploy/systemd/api.conf.example` and `deploy/systemd/worker.conf.example` SHALL load through the configuration loader of their role, with placeholders replaced by temporary files, and SHALL bind only ports of the deployment-target table.

#### Scenario: Examples do not rot

- **WHEN** each example is loaded as its role
- **THEN** the listeners, the Knowledge origin, the seed path, the schedule keys and the deadline have the documented values
