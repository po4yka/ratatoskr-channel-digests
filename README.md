# Ratatoskr Channel Digests

This bounded context acquires explicitly subscribed public Telegram channels through one
operator-authorized MTProto account, preserves immutable post revisions, constructs deterministic
digest manifests, and coordinates recap analysis with Knowledge.

It contains two processes:

- `ratatoskr-channel-digests-api` — owner-scoped loopback reads on `127.0.0.1:8098`, operator port
  `9469`, and no provider authority;
- `ratatoskr-channel-digests-worker` — exact pre-provisioned JetStream intake, encrypted session
  access, public-channel provider calls, durable coordination, and operator port `9470`.

Platform remains the authenticated public facade and schedule authority. Knowledge owns inference.
Telegram owns Bot API interaction, commands, preferences, quiet hours, and notification delivery.
Private channels, groups, dialogs, invite links, join/leave writes, and continuous history mirroring
are not supported.

See [DEVELOPMENT.md](DEVELOPMENT.md) for the exact gate and [docs/OPERATIONS.md](docs/OPERATIONS.md)
for authorization and recovery procedures.
