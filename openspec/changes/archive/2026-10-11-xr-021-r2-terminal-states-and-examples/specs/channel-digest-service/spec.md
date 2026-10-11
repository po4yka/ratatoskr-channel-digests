## ADDED Requirements

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
