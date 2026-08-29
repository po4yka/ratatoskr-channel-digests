# Data Model

`schema.sql` is the only current schema definition; this development repository has no migrations.
The `channel_digests` schema owns:

- `provider_status`, public `channels`, and owner-scoped `subscriptions`;
- immutable `post_revisions`, including bounded normalized body bytes and their SHA-256 identity;
- replay-safe `digest_runs`, canonical `digest_manifests`, and terminal `digest_results`;
- transport `inbox_messages`, transactional `outbox_messages`, and restart `leases`.

The natural subscription key is owner plus canonical lowercase username. A revision is immutable by
channel, provider message ID, and content digest, so edits append. A run is unique by owner, trigger,
idempotency identity, and closed-open window. Manifest and result links are one-to-one with a run. A
completed or partial `digest_results` row stores the exact Knowledge `recap_id` and canonical
lowercase 64-hex `result_digest_hex` accepted from the terminal fact; a failed row stores neither.
Replay equality includes the owner, run, manifest digest, recap identity, result digest, outcome,
and citation count. Expected-state transitions prevent terminal regression.

The database does not store MTProto session/key bytes, Telegram Bot API data, Platform sessions,
Knowledge recap or analysis bodies, or provider credentials. `result_digest_hex` is integrity
linkage, not a narrative cache. Disabling a subscription stops future capture without deleting
immutable run evidence.
