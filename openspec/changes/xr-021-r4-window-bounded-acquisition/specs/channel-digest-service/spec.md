## MODIFIED Requirements

### Requirement: Acquisition appends immutable revisions with partial truth

Acquisition SHALL page only the eligible public-channel closed-open window under finite calls,
timeouts, and retries. Duplicate observations SHALL converge; changed normalized bytes SHALL append
an immutable content-digest revision. Deleted, unavailable, flood-wait, timeout, reconnect, and
partial-channel outcomes SHALL checkpoint durably and resume after restart without calling a failed
channel empty or erasing successful channels. Pages arrive newest first, so the walk SHALL end after
the page that holds a post published before the window start, with the checkpoint `completed`; a post
published exactly at the window start is inside the window and a page that straddles it SHALL be
stored in full. An acquisition SHALL be partial only when the page limit is reached while every page
so far still reached into the window, or when a provider failure stops a walk that already has pages.

#### Scenario: Edited observation creates another revision

- **WHEN** the same channel/message is observed with changed normalized bytes
- **THEN** both content digests remain immutable and later run evidence selects the observed revision

#### Scenario: Flood wait survives restart

- **WHEN** the provider returns a bounded flood-wait and the worker restarts before it expires
- **THEN** no early provider retry occurs and execution resumes from the persisted wait/checkpoint

#### Scenario: Deep history with the window satisfied on the first page

- **WHEN** a channel holds more history than the page limit allows and its first page already holds a post older than the window start
- **THEN** exactly one provider page is requested, the checkpoint is `completed` and the acquisition is not partial

#### Scenario: Quiet channel with deep history

- **WHEN** the first page holds only posts older than the window and a next cursor
- **THEN** the run completes as an empty success with stage `no_sources` and is not failed as `provider_unavailable`

#### Scenario: Window longer than the page limit

- **WHEN** every page up to the page limit still reaches into the window and the last page has a next cursor
- **THEN** the acquisition is partial with the checkpoint `page_limit` and the run settles `partially_succeeded` with `channel_digest.acquisition_incomplete`
