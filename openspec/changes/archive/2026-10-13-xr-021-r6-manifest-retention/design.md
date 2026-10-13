## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md section R6-02; where this document and that file differ, that file wins. Only repository-internal decisions are recorded here.

## Decisions

### The text goes, the linkage stays

A manifest row is the linkage between a run, an owner, a digest and a result: `digest_results.manifest_id` references it and the coordinator reads `sha256`, `source_count` and `channel_count` afterwards. Minimizing therefore nulls only `canonical_text` and sets `minimized_at`, and a CHECK allows exactly the two states (text and no instant, or instant and no text) so a half-minimized row cannot exist. The row is never deleted and its digest never changes.

### A link table, by message, without a reference to revisions

Withdrawal works by message, not by revision: `withdraw_missing` withdraws every revision of a message id. `digest_manifest_sources (manifest_id, channel_id, provider_message_id)` records the pairs a manifest holds, with an index on `(channel_id, provider_message_id)` for the withdrawal lookup. It deliberately does not reference `post_revisions`: the link is by message, and a synthetic post committed by a test helper has no revision. `ManifestBuilder::build` returns the distinct pairs because it already holds the sources, and `commit_manifest` inserts them after the manifest row inside the same transaction. A replay hits `on conflict (run_id) do nothing`, returns before the insert and writes no second set.

### The timer keys on the run window

`Maintenance::minimize_manifests_before` selects manifests joined to `digest_runs` with `state in ('completed','partial','failed')` and `window_start < before`, oldest window first, then `manifest_id`, with `limit` rows and `for update of m skip locked`. Every source has `published_at >= window_start`, so keying by the run window means a manifest never keeps a body longer than the revisions it was built from. A non-terminal run keeps its text because the recap exchange may still need it. The sweeper runs it after `minimize_before` with the same cutoff and batch, and returns a `SweepOutcome { bodies, manifests }` so both counts reach the counters and the log line without a global.

### Withdrawal minimizes the manifests in the same statement

`withdraw_missing` becomes one statement with data-modifying CTEs: the first updates the revisions and returns the withdrawn `(channel_id, provider_message_id)` pairs, the second updates `digest_manifests` where `canonical_text is not null` and the manifest has a link row matching a withdrawn pair. It returns a `Withdrawn { revisions, manifests }`. The manifest is minimized whole, whatever the run state, because it is one hashed text and one post cannot be cut out. A recap request whose manifest is withdrawn before Knowledge fetches it ends as `manifest_unavailable` on the Knowledge side and is retryable as a new run; a recap Knowledge already stored is not withdrawn.

### The route: 410 only behind the three claims

`get_manifest` keeps the owner, run and digest gate in the `where` clause and selects `canonical_text` (nullable). A row with text answers `200`, a row without answers `410` with an empty body, no row answers `404`. Because the claims still gate the row, `410` is reachable only by a caller that already proves the owner, the run and the digest, so nothing is revealed to anyone else. The "indistinguishable" sentence in `docs/INTERFACES.md` is limited to the `404` cases.
