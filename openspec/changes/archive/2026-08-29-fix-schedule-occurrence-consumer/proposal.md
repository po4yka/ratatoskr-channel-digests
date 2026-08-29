## Why

The implemented scheduler fan-out was not connected to the published bus boundary, so a real
Platform occurrence could not reach the service.

## What Changes

- Consume the typed deployment-wide occurrence subject through its own durable pull consumer.
- Persist inbox replay authority and owner fan-out in one transaction.
- Pin the published additive contract revision and document the subject.

## Capabilities

### Modified Capabilities

- `channel-digest-service`: connect schedule occurrence transport to durable owner fan-out.

## Impact

The worker bus composition, contract pin, schedule tests, and interface documentation change. No
provider credential, post body, or Telegram destination enters the command.
