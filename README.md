# Ratatoskr Channel Digests

This bounded context acquires explicitly subscribed public Telegram channels through one
operator-authorized MTProto account, preserves immutable post revisions, constructs deterministic
digest manifests, and coordinates recap analysis with Knowledge.

It contains two processes:

- `ratatoskr-channel-digests-api` — owner-scoped loopback reads on `127.0.0.1:8098`, bounded
  authenticated read-through of Knowledge-owned recap results, operator port `9469`, and no
  provider authority;
- `ratatoskr-channel-digests-worker` — exact pre-provisioned JetStream intake, encrypted session
  access, public-channel provider calls, durable coordination, and operator port `9470`.

Platform remains the authenticated public facade and schedule authority. Knowledge owns inference.
Telegram owns Bot API interaction, commands, preferences, quiet hours, and notification delivery.
Private channels, groups, dialogs, invite links, join/leave writes, and continuous history mirroring
are not supported.

The API keeps only the Knowledge analysis UUID and result SHA-256 accepted from the terminal fact.
It never stores recap narrative: successful result reads resolve the local owner first and then
verify both values against Knowledge before returning the recap.

A run that does not finish within `RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS` (default 1800) is failed
and its Platform operation reported, list pages are clamped to `RATATOSKR__LIMITS__PAGE_SIZE`, and
scheduled digests use `RATATOSKR__SCHEDULE__OUTPUT_LANGUAGE` (default `ru`). The API reads Knowledge
results from its domain listener, `RATATOSKR__KNOWLEDGE__BASE_URL=http://127.0.0.1:8091`. Operator
examples are in `deploy/systemd/api.conf.example` and `deploy/systemd/worker.conf.example`.

See [DEVELOPMENT.md](DEVELOPMENT.md) for the exact gate and [docs/OPERATIONS.md](docs/OPERATIONS.md)
for authorization and recovery procedures.
