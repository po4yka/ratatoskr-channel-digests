## ADDED Requirements

### Requirement: Grammers upgrades preserve the provider boundary
The provider SHALL accept only broadcast channels and SHALL preserve the RFC3339 explicit UTC offset on acquired publication timestamps.

#### Scenario: Non-broadcast peer is resolved
- **WHEN** username resolution returns a peer other than a broadcast channel
- **THEN** acquisition rejects it without reading its history.

#### Scenario: A publication timestamp is serialized
- **WHEN** a public channel message carries a UTC publication instant
- **THEN** the provider emits RFC3339 with an explicit +00:00 offset.
