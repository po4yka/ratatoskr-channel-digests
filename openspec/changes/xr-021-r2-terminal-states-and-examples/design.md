## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md section R2-05 (a, b, e, f), R2-10 and R2-01; where this document and that file differ, that file wins.

## Decisions

### The reaper is one set-based transaction per tick

`Reaper::reap_once(now)` takes the clock as a parameter so a test does not sleep. One transaction updates every overdue on-demand-or-scheduled run in `accepted`, `acquiring` or `waiting_recap` to `failed` with `safe_failure_class = 'deadline_exceeded'` (`where state in (...) and updated_at < $now - deadline`, `returning run_id, owner_id, operation_id, trigger`) and, in the same transaction, enqueues the report for each returned on-demand run through `OperationReportRow`. The outbox unique key `operation:<id>:failed` makes a second report impossible, so running twice or racing a Knowledge failure adds nothing. A scheduled run reuses the occurrence operation id for correlation only and reports nothing. The task is spawned by `run_worker` beside the bus and provider supervisors so it runs while the provider is down, it logs only the count of failed runs and the class, and it has no effect on readiness.

### A late fact is already safe

`settle_completion` refuses a run that is not `waiting_recap` and `settle_failure` replays a run that is already `failed`; neither writes a state or a report in that case. The reaper tests pin both outcomes rather than add a branch.

### Attributable versus unattributable

The bus decodes the typed payload first. A command is attributable when the envelope tenant is a user, the payload names an `operation_id`, and the payload `owner` equals that tenant. For a typed payload that decoded but fails `validate_for_publish` or an invariant, `CommandIntake` records the rejection itself. For a payload whose typed decode fails (for example a window longer than the contract allows), the bus reads `operation_id` and `owner` from the raw JSON object and asks `CommandIntake::reject_unreadable` to record it. Both paths share the transaction already used for the subscription limit: the inbox row is stored as failed with the safe class `command_invalid`, one `failed` report with `channel_digest.command_invalid` and `retryable = false` is queued and the call returns success, so the message is acknowledged. The wrong command type, a foreign producer, a missing or mismatching tenant, an undecodable envelope and a payload with no readable operation stay Termed with no report.

### Page size clamps instead of refusing

The configured page size is a ceiling on work, not a validation of the caller. `checked_page_size` accepts `1..=100` and returns the smaller of the request and the ceiling; `0`, a value above 100 and a non-number remain 400.

### The language is passed to the creation of the run

`ScheduleConfig` carries `output_language`; the worker message handler passes it to `accept_occurrence`, which sets it in the fan-out transaction after `create_digest_run`, the way `accept_run` already does. The configuration default is `ru`, so behaviour without the key is unchanged.

### Examples are tested through the real loader

The two example files are read by a test, secrets and absolute seed paths are substituted with temporary files, and the result goes through `Config::from_environment`. The test asserts the flow-enabling values and that every bind is a port of the DEPLOYMENT_TARGET table.
