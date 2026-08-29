# Data Model

`schema.sql` is the only current schema definition; this development repository has no migrations.
The `channel_digests` schema owns:

- `provider_status`, public `channels`, and owner-scoped `subscriptions`;
- immutable `post_revisions`, including bounded normalized body bytes and their SHA-256 identity;
- replay-safe `digest_runs`, canonical `digest_manifests`, and terminal `digest_results`;
- transport `inbox_messages`, transactional `outbox_messages`, and restart `leases`.

The natural subscription key is owner plus canonical lowercase username. A revision is immutable by
channel, provider message ID, and content digest, so edits append. A run is unique by owner, trigger,
idempotency identity, and closed-open window. Manifest and result links are one-to-one with a run.
Expected-state transitions prevent terminal regression.

The database does not store MTProto session/key bytes, Telegram Bot API data, Platform sessions,
Knowledge analysis bodies, or provider credentials. Disabling a subscription stops future capture
without deleting immutable run evidence.
