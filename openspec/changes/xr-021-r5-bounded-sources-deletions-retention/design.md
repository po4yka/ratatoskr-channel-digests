## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md section R5-07; where this document and that file differ, that file wins. Only repository-internal decisions are recorded here.

## Decisions

### Newest first, one extra row to learn about truncation

`select_sources` orders the deduplicated latest-revision rows by `published_at desc, channel_id, provider_message_id desc, revision_id desc` and takes `source_count + 1`. If `source_count + 1` rows came back, the run is truncated: the extra row is dropped and the rest are reversed into ascending manifest order (the S08 manifest ordering is unchanged, and `ManifestBuilder` still sorts by its own rule). `execute_one` ORs the truncation into `acquisition_incomplete` before `record_acquisition`, so the value stored on the run row and read later by the Knowledge completion path already includes it. The executor does not see the real total, only that more existed, so the WARN carries `selected` and `available_at_least` counts.

### Withdrawal by absence uses the range a page proves

A page of a newest-first walk proves the state of its id range: the ids from its lowest id up to (not including) the lowest id of the previous page of the same walk. The first page of a walk has no upper limit. A page that is short (no `next_before_message_id`) also proves everything below its lowest id, down to the channel start. A page that ended a walk at the window start with a next cursor, and a walk that stops at `max_pages`, prove only the ids of the pages read. The first page of a resumed walk is bounded above by the stored cursor. `RevisionRepository::withdraw_missing(channel_id, range, present_ids)` is one `update ... where deleted_at is null and provider_message_id in range and provider_message_id <> all(present)` statement that sets `deleted_at`, `body` and `minimized_at` on every revision of the message. An id with an empty or oversized body is present: the engine passes every id of the page, not only the stored ones. A post with `deleted: true` is withdrawn directly even though it is present.

### Withdrawn content is final

`append_revision` converges on the unique `(channel, message, digest)` key, so a withdrawn revision is never resurrected by a later identical observation. A post that is deleted upstream does not come back, and a wrongly withdrawn row needs an operator. A recap that Knowledge already stored is not withdrawn: only later digests change.

### The retention sweep is a small supervisor beside the reaper

`supervise_retention` runs on its own task, ticks at the configured interval, calls `Maintenance::minimize_before(now - body_days, 5000)` and keeps running while the provider or bus is down. The sweep updates at most 5000 rows per pass (a subselect with `limit`), so a backlog drains over several ticks. The counter is an in-process `RetentionCounter`; the operator plane serves it as `channel_digests_bodies_minimized_total` in the Prometheus text format on `GET /metrics` of the worker, because nothing else in the process exports metrics.
