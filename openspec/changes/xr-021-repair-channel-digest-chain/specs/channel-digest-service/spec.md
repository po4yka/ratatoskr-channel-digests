## ADDED Requirements

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
