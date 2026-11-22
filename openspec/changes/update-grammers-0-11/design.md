## Context
Grammers changes its timestamp type and peer variants.

## Goals / Non-Goals
Keep the existing allowlist and RFC3339 UTC offset. No runtime Telegram verification or storage changes.

## Decisions
Use jiff's explicit UTC offset formatting and match only broadcast Channel.

## Risks / Trade-offs
Run synthetic checks; real provider federation remains out of scope.
