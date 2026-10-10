# Data Model

`schema.sql` is the only current schema definition; this development repository has no migrations.
The `channel_digests` schema owns:

- `provider_status`, public `channels`, and owner-scoped `subscriptions`;
- immutable `post_revisions`, including bounded normalized body bytes and their SHA-256 identity;
- replay-safe `digest_runs`, canonical `digest_manifests`, and terminal `digest_results`;
- transport `inbox_messages`, transactional `outbox_messages`, and restart `leases`.

Every run carries the Platform `operation_id` it was accepted under (`digest_runs.operation_id`), and
a scheduled run carries the occurrence operation, so the executor selects pending runs from the run
table alone. `digest_manifests.canonical_text` holds the exact canonical manifest text and
`sha256` is the SHA-256 of that text; the API returns the text unchanged. A run that selects no
source completes without a manifest row. `outbox_messages` holds bare typed payloads with a closed
`subject` check (recap request, operation report, schedule registration); `operation_id` is null for
a registration, which belongs to no operation, and `causation_ref` records the inbound command or
event. The bus wraps each row into its contract envelope at publish time, using the row identity and
creation instant so a republish is byte-identical.

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
