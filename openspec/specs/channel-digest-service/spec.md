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
