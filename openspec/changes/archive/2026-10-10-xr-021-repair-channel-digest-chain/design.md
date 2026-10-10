## Context

See `proposal.md`. The contract text is XR-021 CONTRACTS.md; where this document and that file differ, that file wins. The shape of the chain is: Platform command, worker intake, executor, manifest, Knowledge recap request, Knowledge fact, settled result, with an operation report on each terminal state and a Platform-owned schedule feeding occurrences.

## Decisions

### The manifest is the contract type and the stored text is the served text

`ManifestBuilder::build` takes the minted `manifest_id`, the owner and the resolved rows, builds `ChannelDigestManifest`, validates it and renders `to_canonical_bytes()`. The executor mints `manifest_id` before building so the manifest carries its own reference. The stored column is `canonical_text text`; the API returns it unchanged and never re-serializes, so the digest a reader computes is the digest the producer stored. `revision` is computed in SQL by `row_number() over (partition by channel_id, provider_message_id order by observed_at, revision_id)` across all observed revisions of the message, then the latest body per message is selected. A manifest with zero sources is not a valid contract value, so a run that selects nothing completes without a manifest row, in the same transaction as its `succeeded` report.

### Runs carry their operation id

`digest_runs.operation_id uuid not null` is written by `accept_run` (the command operation) and by `accept_occurrence` (the occurrence operation, shared by every fanned-out run). `outbox_messages.operation_id` becomes nullable because a schedule registration belongs to no operation. Only on-demand runs report; a scheduled run is owned by no Platform operation and emits no report.

### One typed report builder

`src/reports.rs` owns `OperationReportRow`, a typed builder over `OperationReported` that validates the report before it is queued. The semantic key is `operation:<id>:<status>`. Every call site enqueues inside the transaction that changes the state it reports. The subscription limit is the one place the transaction is lost: the P0001 error aborts it, so a second transaction records the inbox row as failed and enqueues the `failed` report, and the message is acknowledged.

### The bus wraps payloads, classifies commands by an explicit set, and authenticates by nkey

Outbox rows keep bare payloads and a causation reference; `bus.rs` wraps them in `CommandEnvelope` or `EventEnvelope` at publish time using the row id as the envelope id, the row creation instant as the issue instant, `operation:<id>` as correlation and `user:<owner>` as tenant. `connect(endpoint, Option<&Path>)` reads the seed once, trims it and uses `ConnectOptions::with_nkey`. A failure maps to the opaque bus error and the seed is never logged. Production consumers verify, never create, as before.

### Schedule registration

At worker start the runtime enqueues one registration command whose semantic key is the SHA-256 of owner, cron and enabled, so an unchanged configuration is sent once and a changed one registers again (Platform upserts). With no owner configured nothing is registered and a safe class is logged.

### API claims

The manifest route requires the run-id and manifest-digest claim headers and answers 404 with an empty body unless both match the stored row for the owner. `/ready` sits outside `/v1`, behind a bearer-only layer.

## Risks and trade-offs

- The oversized-source bound in the manifest builder is unreachable through acquisition (which already refuses such bodies) and is exercised with a directly stored revision: it is a defence-in-depth bound, not a live path.
- The executor keeps its existing hundred-source selection ceiling, so a day with more than one hundred posts is summarized from the first hundred chronologically, as before. Changing that is a product decision outside this change.
- The authorized-broker test proves the fragment against a real nats-server with nkey authorization, not the deployed ACL; the workspace equality check owns byte equality with Platform's stanza.
