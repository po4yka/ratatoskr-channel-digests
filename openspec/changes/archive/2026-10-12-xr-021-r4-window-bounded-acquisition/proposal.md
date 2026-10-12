## Why

Round 4 of changeset XR-021 found that the Round 3 incomplete-acquisition fix produces false positives (XR-021 CONTRACTS.md R4-06). The acquisition loop pages newest first and never stops when the pages go older than the window, so any channel with about 2000 or more posts of total history reaches `max_pages` and ends `partial` even when the whole window was read on page 1. Every such run settles `partially_succeeded` with `channel_digest.acquisition_incomplete`, a quiet channel with deep history and no post in the window fails with `channel_digest.provider_unavailable` instead of completing as an empty success, and each run costs up to 20 MTProto page calls per channel, which raises the flood-wait risk.

## What Changes

- The acquisition engine stops after the page that contains a post published before `window_start`: the checkpoint state is `completed`, the cursor is cleared and the report has `partial = false`. A post published exactly at `window_start` is inside the window, and a straddling page is processed in full before the walk ends.
- `partial` is true only when `max_pages` is reached while every page so far still reached into the window, or when a provider error stops a walk that already has pages.
- The resume of a stored checkpoint, the flood-wait handling and the provider error classes keep their meaning.
- `docs/OPERATIONS.md` states when `acquisition_incomplete` is set and when it is not.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: the acquisition requirement gains the window-start stop rule and defines when an acquisition is partial.

## Impact

- Affects `src/acquisition.rs`, `tests/acquisition.rs`, `tests/incomplete_acquisition.rs` and `docs/OPERATIONS.md`. No schema change, no wire shape change, and the repository stays on contracts commit `ad16855c4e7f3d52cd118274faa3b8f3ab4da576`. Cross-repository behaviour is defined in XR-021 CONTRACTS.md section R4-06, not restated here.
- No deliberate break. Rollback: revert the commits; a run already marked incomplete keeps its flag.
