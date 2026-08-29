## Context

Platform owns one deployment schedule while this service owns subscriptions and owner windows.

## Decisions

The worker accepts `channel_digest.schedule.occurrence_requested.v1` only from Platform, verifies
that the aggregate matches the typed occurrence reference, and records the command in the existing
inbox before creating any owner runs. One transaction records the inbox completion and all natural-key
runs. Redelivery returns the existing replay outcome.

The worker opens a dedicated fleet-provisioned durable consumer so schedule traffic cannot change the
ordering or backlog of on-demand run commands.

## Rollback

Disable the Platform schedule before rolling the worker back. Existing runs and inbox evidence remain
valid and need no schema rollback.
