## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md sections R3-01, R3-10, R3-11 and R3-18 rule 3; where this document and that file differ, that file wins.

## Decisions

### The inbox prefix is one constant beside the connection function

`bus.rs` holds `const INBOX_PREFIX: &str = "_INBOX_channel_digests";` and `connect` applies it with `ConnectOptions::custom_inbox_prefix` whether or not a seed is given, so the one worker connection never subscribes outside the prefix. `deploy/nats/identity.conf` changes in the same commit to stay the byte-equal copy of the deployed stanza.

### Acquisition returns a four-way outcome and one flag

`RunExecutor::acquire` returns `Complete`, `Incomplete`, `Deferred` or `Unavailable`. A flood wait on any channel still defers the whole tick. When every subscribed channel was unavailable the outcome is `Unavailable` and the run fails as today. Otherwise the outcome is `Incomplete` when at least one channel was unavailable or reported `partial`, and `Complete` when none did. The executor writes the outcome into `digest_runs.acquisition_incomplete` before it selects sources, overwriting rather than or-ing so that the tick that finally proceeds decides, and passes it to `commit`. With no selected source and an incomplete acquisition `commit` fails the run with `ProviderUnavailable` (which also queues the retryable report), otherwise it completes empty as before. Persisting the flag before the transition keeps the later Knowledge completion fact self-sufficient: `settle_completion` reads the flag from the run row, so a restart between acquisition and the fact loses nothing.

### The flag joins the omitted count in the outcome and the warning

`settle_completion` makes the result outcome `partial` when `omitted_count > 0` or the run is incomplete, and the run takes the same state. `reports::report_completion` receives the flag and reports `partially_succeeded` with the warning `channel_digest.acquisition_incomplete` ("Some channels could not be read completely in this window.") in addition to `channel_digest.context_omitted` when both apply. A scheduled run keeps reporting nothing but gets the `partial` outcome. The replay comparison uses the same computed outcome, so a replayed fact stays idempotent.

### Queued until acquisition starts

Intake enqueues `queued` / `awaiting_acquisition` for an on-demand run. `begin_acquisition` becomes one transaction: the guarded `accepted` to `acquiring` update and, for an on-demand run, the `running` / `acquiring` report. The outbox semantic key `operation:<id>:running` makes a second report impossible. The deadline reaper is unchanged and fails a run that never leaves `accepted` with the existing retryable report.

### One subscriber for both binaries

`telemetry::install_subscriber(&Config)` builds an `EnvFilter` from `RATATOSKR__TELEMETRY__LOG_FILTER` (default `info`) and the JSON or pretty formatter from `RATATOSKR__TELEMETRY__LOG_FORMAT` (default `json`), writing to standard error. Both keys are validated when the configuration loads: an unparsable filter or format is a `ConfigError` naming the key, so `check-config` refuses it. `tracing-subscriber` gains the `env-filter` feature. The call is the first statement after `Config::load` in each `main`. No event carries a post body, channel name, token or session byte; the existing call sites log classes and counts, and the provider connection warning logs the provider error class.

### The role file is configuration

`deploy/postgres/role.sql` follows the extractor's file: create the role if absent without a password, no superuser, create database, create role, replication or RLS bypass, `grant connect, create on database ratatoskr`, `\connect ratatoskr`, `revoke all on schema public`. A test compares the role, database and port of both examples with the file. The file itself, the examples' header comments and the documentation have no failing behaviour test.
