## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md section R4-06; where this document and that file differ, that file wins.

## Decisions

### The window start ends the walk

`provider.rs` returns pages newest first by `offset_id`, and message ids and publication times rise together, so the first post older than `window_start` proves every later post older. `AcquisitionEngine::execute` therefore remembers, while it processes a page, whether any post of the page was published before `window_start`. After the page is processed and its in-window revisions are stored, that flag clears `before`, so the `page_committed` checkpoint and the final state both read `completed` and `partial` is false. A post exactly at `window_start` is not older, so it does not end the walk. A deleted, empty or oversized post is skipped before its date is read today and stays skipped: it cannot prove the window start was reached. A post at or after `window_end` is skipped and does not end the walk either.

### Partial keeps two meanings only

`partial` stays `before.is_some()` after the loop, which is now true only when `max_pages` was reached while the last page still reached into the window, and the provider-error return with `pages > 0` is unchanged. The executor mapping (`incomplete |= report.partial`) and the empty-and-incomplete failure of R3-10 are correct and unchanged.

### Resume is unchanged

A stored `completed` checkpoint still resumes as finished, and a stored cursor still resumes from that cursor. A checkpoint written by the new rule for a walk that ended at the window start is simply `completed`.
