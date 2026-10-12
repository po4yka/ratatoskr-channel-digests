## ADDED Requirements

### Requirement: A digest takes the newest posts of its window and says when it dropped some

Source selection SHALL take at most `RATATOSKR__LIMITS__SOURCE_COUNT` revisions (default and maximum 100) and SHALL prefer the newest by publication time. The manifest SHALL still list the selected sources in ascending manifest order. When more eligible revisions existed than were selected, the run SHALL record `acquisition_incomplete`, so the result outcome is `partial` and an on-demand operation reports `partially_succeeded` with the warning `channel_digest.acquisition_incomplete`. A content-free WARN SHALL carry the selected count and the count seen. Exactly the cap SHALL still be complete.

#### Scenario: More posts than one digest takes

- **WHEN** a window holds 250 observed revisions and the cap is 100
- **THEN** the manifest holds the newest 100 in ascending order, the run is marked incomplete, and the operation ends `partially_succeeded` with `channel_digest.acquisition_incomplete`

#### Scenario: Exactly the cap

- **WHEN** a window holds exactly 100 observed revisions
- **THEN** the run is complete and the operation ends `succeeded`

#### Scenario: A smaller configured cap

- **WHEN** `RATATOSKR__LIMITS__SOURCE_COUNT` is 10 and a window holds 25 revisions
- **THEN** the manifest holds the newest 10 and the run is marked incomplete

### Requirement: A post deleted upstream is withdrawn from later digests

After a page of a channel is stored, every stored, non-deleted revision of that channel whose message id lies inside the id range the page proves and that the page does not contain SHALL be withdrawn. The range of a page is from its lowest id up to the lowest id of the previous page of the same walk, with no upper limit for the first page of a walk; a short last page also proves everything below it, and a walk that stops at the page limit or at a provider error proves only the pages it read. A page that marks a post `deleted` SHALL withdraw it. A post without text counts as present. Withdrawing SHALL set `deleted_at`, null the `body` and set `minimized_at` on every revision of the message, and selection SHALL exclude revisions with `deleted_at`. A recap Knowledge already stored SHALL NOT be withdrawn.

#### Scenario: A post vanishes between two walks

- **WHEN** message 7 is stored, a later walk's page covers the id range containing 7, and the page does not contain 7
- **THEN** every revision of message 7 has `deleted_at`, a null body and `minimized_at`, and the next manifest excludes it

#### Scenario: A vanished id outside the proved range

- **WHEN** a stored message id lies below the lowest id of the last page read
- **THEN** it is kept

#### Scenario: A provider error mid-walk

- **WHEN** the provider fails on the second page
- **THEN** nothing below the first page's lowest id is withdrawn

#### Scenario: A media-only post

- **WHEN** a page contains a post with an empty body
- **THEN** that message id counts as present and is not withdrawn

### Requirement: Stored bodies are minimized on a timer

The worker SHALL minimize stored post bodies older than `RATATOSKR__RETENTION__BODY_DAYS` (default 30, range 8 to 3650) every `RATATOSKR__RETENTION__SWEEP_INTERVAL_SECONDS` (default 3600, range 60 to 86400), at most 5000 rows per pass, while the provider or bus is down as well. Minimizing SHALL null the body and keep `content_sha256`, provenance and `published_at`. The sweep SHALL increment `channel_digests_bodies_minimized_total` and log only a count. Both keys SHALL be accepted only by the worker role.

#### Scenario: Old bodies are nulled

- **WHEN** a sweep runs with a body older than the retention and a younger one
- **THEN** only the older body is null and its digest and publication time remain

#### Scenario: The API role refuses the keys

- **WHEN** the API role is configured with `RATATOSKR__RETENTION__BODY_DAYS`
- **THEN** configuration fails naming the key
