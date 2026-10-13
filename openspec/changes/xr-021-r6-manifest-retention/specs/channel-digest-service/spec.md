## MODIFIED Requirements

### Requirement: Retention, telemetry, and shutdown preserve privacy and recovery

Raw post bodies SHALL remain only in owned bounded storage and authenticated manifest responses.
Session bytes SHALL never enter PostgreSQL, events, logs, metrics, fixtures, or diagnostics. Expired
transient payloads SHALL be minimized without deleting immutable revision digests, run/provenance
evidence, or terminal linkage. Post revision bodies and manifest texts SHALL expire under the same
retention: a manifest of a terminal run whose window start is older than the cutoff SHALL lose its
text and keep its digest, counts, owner, run and creation time. Both processes SHALL drain,
checkpoint, and join within the finite shutdown budget while readiness fails immediately.

#### Scenario: Content marker triggers failure

- **WHEN** a synthetic post containing a unique marker reaches a bounded failure path
- **THEN** captured ordinary telemetry and outbox payloads contain none of the marker

#### Scenario: Shutdown interrupts acquisition

- **WHEN** the worker receives termination during a page boundary
- **THEN** readiness fails, the durable checkpoint remains valid, and restart resumes without duplicate revision effects

#### Scenario: An old manifest loses its text

- **WHEN** a sweep runs over a manifest of a completed run whose window is older than the retention and a manifest of a younger run
- **THEN** only the older manifest has no text and a minimization time, its digest, counts and result linkage are unchanged, and the younger manifest is untouched

#### Scenario: A run still waiting for its recap keeps its manifest

- **WHEN** a sweep runs over a manifest of a `waiting_recap` run whose window is older than the retention
- **THEN** the manifest keeps its text

### Requirement: The manifest is the canonical contract artifact served verbatim

A run that selects at least one source SHALL store the exact canonical text of a valid `ChannelDigestManifest`, whose sha256 is the stored digest, and the API SHALL answer a manifest read with exactly those bytes while the text is stored. A read SHALL answer 404 with an empty body unless the owner, the run id claim and the digest claim all match the stored row. A read whose owner, run id and digest claims match a manifest whose text was minimized SHALL answer 410 with an empty body. A run that selects no source SHALL complete without a manifest.

#### Scenario: Stored manifest decodes and hashes to its digest

- **WHEN** a run selects two revisions of one message and one other post
- **THEN** the stored text decodes with `ChannelDigestManifest::from_canonical_bytes`, the later body has revision 2 and the sha256 of the text equals the stored digest

#### Scenario: Claim mismatch is hidden

- **WHEN** a manifest is read with a wrong run id, a wrong digest or another owner
- **THEN** the response is 404 with an empty body

#### Scenario: A minimized manifest is gone

- **WHEN** a manifest whose text was minimized is read with its owner, run id and digest
- **THEN** the response is 410 with an empty body

#### Scenario: A minimized manifest hides behind its claims

- **WHEN** a manifest whose text was minimized is read with a wrong digest, a wrong run id or another owner
- **THEN** the response is 404 with an empty body

## ADDED Requirements

### Requirement: A withdrawn post minimizes the manifests that hold it

The commit of a manifest SHALL record, in the same transaction, the distinct channel and message pairs the manifest holds, and a replay SHALL record no second set. When a post is withdrawn, every manifest that holds that message SHALL lose its whole text and keep its digest, counts and linkage, whatever the state of its run, and a manifest that does not hold the message SHALL stay intact. A recap that Knowledge already stored SHALL NOT be withdrawn.

#### Scenario: A message vanishes from a later walk

- **WHEN** one manifest holds messages 7 and 8, another holds only message 9, and a later walk proves message 7 gone
- **THEN** the first manifest has no text and a minimization time and the second is intact

#### Scenario: A walk proves nothing gone

- **WHEN** a walk finds every stored message present
- **THEN** no manifest is minimized

#### Scenario: The run still waits for its recap

- **WHEN** a message of a manifest whose run is `waiting_recap` is withdrawn
- **THEN** the manifest loses its text
