## 1. The walk stops at the window start (R4-06 rules 1 to 4)

- [x] 1.1 RED: add `tests/incomplete_acquisition.rs::deep_history_with_the_window_satisfied_on_page_one_is_not_partial`, `a_quiet_channel_with_deep_history_completes_as_an_empty_success`, `a_window_longer_than_max_pages_full_pages_is_still_partial` and `a_mid_walk_provider_failure_is_still_partial`, and `tests/acquisition.rs::a_page_that_straddles_the_window_start_is_stored_in_full_and_ends_the_walk` and `a_post_exactly_at_window_start_is_inside_the_window`; run them and confirm the deep-history, quiet-channel, straddling-page and window-start tests fail on the stated assertion (page call count, run state `failed` with `provider_unavailable`) and not on a compile error, and that the page-limit and mid-walk tests already pass and pin the behaviour that must stay.
- [x] 1.2 GREEN: end the walk in `AcquisitionEngine::execute` after the page that holds a post published before `window_start`, checkpointing `completed`.

## 2. Documentation (R4-06)

- [x] 2.1 Update `docs/OPERATIONS.md` so it says when `acquisition_incomplete` is set (page limit reached while pages still reach into the window, a provider failure mid-walk, an unavailable channel) and when it is not (the window start was reached). No failing test applies because this task only edits documentation.

## 3. Gate

- [x] 3.1 Run the `DEVELOPMENT.md` gate to green. No failing test applies because this task only runs checks.
