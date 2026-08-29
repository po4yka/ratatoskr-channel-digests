## MODIFIED Requirements

### Requirement: Knowledge exchange and schedule execution are replay safe

A non-empty committed manifest SHALL cause exactly one body-free typed Knowledge recap request.
Completion or failure SHALL settle only when owner, run, manifest digest, counts, result identity, and
citation membership match durable evidence. Duplicate, foreign, or out-of-order facts SHALL not
regress state. The service SHALL consume the typed deployment-wide schedule occurrence command
through a dedicated durable pull consumer. Occurrence intake and all active-owner natural-key runs
SHALL commit with one inbox decision; replay SHALL create no additional runs. The service SHALL
compute each owner window from subscription activation and the previous/current occurrence grid
points and SHALL not emit Telegram delivery events directly.

#### Scenario: Deployment occurrence is redelivered

- **WHEN** Platform redelivers one occurrence envelope after uncertain acknowledgement
- **THEN** the inbox replays one decision and each active owner still has exactly one run for that occurrence

#### Scenario: Worker stops after manifest commit

- **WHEN** the worker restarts before recap-request publication acknowledgement
- **THEN** it republishes the same typed request identity without another manifest or inference identity

#### Scenario: Foreign completion is received

- **WHEN** a completion names another owner or manifest digest
- **THEN** the run remains unsettled and no result is exposed
