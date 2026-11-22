## Why
Move the MTProto dependency family to Grammers 0.11 without broadening provider access or changing serialized timestamps.

## What Changes
Update the three direct Grammers crates and adapt the provider interfaces.

## Capabilities
### Modified Capabilities
- `channel-digest-service`: Preserve the public broadcast-channel and publication timestamp contract.

## Impact
Provider adapter and Cargo manifests only; no new credentials, external calls, or deployment.
