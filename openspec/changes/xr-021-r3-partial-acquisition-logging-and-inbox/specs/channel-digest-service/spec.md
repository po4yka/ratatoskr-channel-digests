## ADDED Requirements

### Requirement: The worker connection uses the service's own inbox prefix

The worker SHALL open its NATS connection with the inbox prefix `_INBOX_channel_digests`, and the shipped identity fragment SHALL allow `subscribe` to exactly `_INBOX_channel_digests.>`.

#### Scenario: JetStream requests complete under the narrowed ACL

- **WHEN** the worker connects through its connection function to a broker that holds only the shipped identity fragment
- **THEN** it verifies and pulls its five durables and publishes with an acknowledgement

### Requirement: Incomplete acquisition is never an empty success

A run whose acquisition left any subscribed channel unavailable or only partly read SHALL record `acquisition_incomplete`. Such a run with no selected source SHALL fail with `provider_unavailable` and a retryable `channel_digest.provider_unavailable` report and SHALL NOT report `succeeded` with stage `no_sources`. Such a run with selected sources SHALL settle with outcome `partial`, and an on-demand run SHALL report `partially_succeeded` with the warning `channel_digest.acquisition_incomplete`, plus `channel_digest.context_omitted` when sources were also omitted. A complete acquisition SHALL still settle `succeeded`.

#### Scenario: One channel unavailable and the other empty

- **WHEN** one subscribed channel fails on its first page and the other returns no posts
- **THEN** the run is failed with `provider_unavailable` and no `succeeded` report exists

#### Scenario: A channel fails mid-pagination

- **WHEN** a channel returns one page and then fails
- **THEN** the run is marked `acquisition_incomplete` and proceeds with the retained posts

#### Scenario: Incomplete acquisition with sources

- **WHEN** Knowledge completes such a run
- **THEN** the result outcome is `partial` and the report is `partially_succeeded` with `channel_digest.acquisition_incomplete`

### Requirement: An accepted run is queued until acquisition starts

The report of an accepted on-demand run SHALL be `queued` with stage `awaiting_acquisition`. The transition to `acquiring` SHALL queue exactly one `running` report with stage `acquiring` in its own transaction.

#### Scenario: Provider down

- **WHEN** an on-demand run is accepted and the executor has not started it
- **THEN** only a `queued` report exists and no `running` report exists

#### Scenario: Acquisition starts once

- **WHEN** the executor starts the run and executes it again
- **THEN** exactly one `running` report exists

### Requirement: Both binaries log through a configured subscriber

Both processes SHALL install a tracing subscriber from `RATATOSKR__TELEMETRY__LOG_FORMAT` (`json` default, or `pretty`) and `RATATOSKR__TELEMETRY__LOG_FILTER` (an `EnvFilter` directive string, default `info`), both validated at load for both roles. No event SHALL carry a post body, channel name, token or session byte.

#### Scenario: Library warning reaches the operator

- **WHEN** the worker starts with a provider session that cannot connect
- **THEN** its output holds a JSON line with the class `provider_unavailable` and no session byte

#### Scenario: Bad telemetry value

- **WHEN** the format is `xml` or the filter is not a valid directive
- **THEN** configuration fails naming the key and not the value

### Requirement: The database role is provisioned by a checked-in file

`deploy/postgres/role.sql` SHALL create the role and database grants the shipped examples name, and both examples SHALL use `ratatoskr_channel_digests` on database `ratatoskr` and port 5432.

#### Scenario: Examples match the role file

- **WHEN** the role and database of each example URL are compared with the role file
- **THEN** they are equal
