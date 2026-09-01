---
type: Public API
title: Runtime Package
description: Immutable Agents, durable Host ownership, typed thread operations, and observation.
resource: /runtime
tags: [api, runtime, agent, host]
timestamp: 2026-08-18T00:00:00Z
---

# Runtime

`runtime.Open` owns one opaque `storage.Source` and returns a composition-root
`*runtime.Host`. Hosts create an immutable `Agent`, bind an
`AgentFactory` through `Host.ThreadService`, and expose that typed service to
the product boundary. The service owns one durable thread journal and one
in-process actor for each active thread.

## Thread service

`ThreadService` is the current host-facing lifecycle contract. Its commands
include `Create`, `Fork`, `Delete`, `SetTitle`, `Send`, `Respond`, `Cancel`,
`Retry`, queue operations, and `Subscribe`; queries include
`View`, `List`, `History`, and `Context`. Every mutation carries a stable
request key. Replaying the same key and fingerprint returns the committed
result; changing the input returns `runtime.ErrRequestConflict`.

Floret allocates `ThreadID`, `TurnID`, and `RunID`. A `LogicalRequestID` is a
user-visible association only and never replaces those execution identities.
Child threads are ordinary durable threads with explicit parent metadata.

`Send` validates and commits the canonical user turn before publishing a live
view or starting provider work. `SendInput.SupplementalContext` carries
host-provided, turn-scoped material such as an explicitly selected file. It is
validated and passed to the provider for that turn, but is not written into the
canonical user message or provider history. Queue admission, interaction
resolution, and cancellation use the same canonical-first ordering. A failed
storage write returns an error without a successful live projection. The
journal is the only durable lifecycle authority; hosts must not persist a
second transcript or rebuild a Floret view from audit records.

`AgentRequest.Input` is the input for the execution being created. For an
Ask User continuation it is the accepted, secret-free interaction resolution;
`AgentRequest.CanonicalTurnInput` remains the original canonical user input of
that Turn. Agent factories use the latter when resolving stable task identity
and must not reinterpret an interaction answer as a replacement objective.
Public answers are also projected once as a canonical user message, so the
continuation and later Turns see the same durable fact. Secret values are
ephemeral; only a redacted marker remains in the journal and provider history.

After `Send` returns an accepted turn, provider execution uses a runtime-owned
background context. The request context only controls admission and response
waiting; closing an HTTP request, changing the selected thread, or reconnecting
a view cannot cancel the accepted turn. Only `Cancel`, thread deletion, and
Host shutdown are execution cancellation owners.

`Cancel` is the authoritative user Stop boundary. One transaction records the
cancel request, resolves pending interactions, closes unfinished tool rows,
seals effect attempts, and appends the terminal. It returns that terminal view
immediately instead of waiting for provider or tool goroutines. Late turn
writes are rejected.

## Effects and shutdown

Tool effects cross a durable one-shot authorization boundary. If the outcome
of a dispatched effect cannot be confirmed, Floret atomically closes every
unfinished tool and interaction, clears provider continuation, and fails the
turn with `effect_outcome_unknown`. The effect is never replayed and there is
no retry command. A terminal turn rejects late results.

`Delete` marks the whole active subtree as deleting, cancels provider and tool
work, waits for execution/effect drains, then commits the canonical tombstone.
Late provider output and late effect dispatch are rejected. `Host.Shutdown`
stops admission, joins Host-managed work, and closes storage only after the
join; a cancelled shutdown remains in progress for a later caller.

## Views and subscriptions

`ThreadService.View` returns a complete replaceable `ThreadView` containing
ordered user, thinking, assistant, tool, and interaction items. `ViewVersion`
is process-local notification ordering. Publishers reject per-thread version
regressions. `Subscribe` is observation, not durable replay; a stale or closed
subscriber must reconnect and obtain a fresh baseline.

Every `ThreadItem` and `ThreadInteraction` carries the exact `TurnID` and
`RunID` of its canonical journal fact. Historical items retain their original
run across later turns and same-turn input continuation. Missing or conflicting
execution identity fails the view read instead of producing an ambiguous item.
Domain migration permanently fills released v5 message rows that predate
durable message `RunID` when canonical lifecycle markers identify one exact
run segment. Runtime projection never infers a missing identity.

`ThreadView.RunID` is the exact current execution identity and never aliases
`TurnID`. `RunProgress` is the actor-owned, process-local phase for active
provider and tool work. It is cleared while waiting for approval or user input
and after terminal settlement; hosts must not reconstruct it from items or
persist a second progress lifecycle. `ThreadSummary` carries the same bounded
identity and progress projection for thread lists.

Every new Run enters the actor through one atomic transition. It closes the
old live segment, installs the new Turn, Run, and logical-request identities,
resets the attempt epoch, and publishes `preparing` before provider work. The
first attempt is epoch 1. Later events must match the active Run and logical
request; only a higher epoch of that same request may replace the active
attempt. Late events from a waiting or completed Run remain rejected.

Terminal presentation settles from the canonical ordered journal. The runtime
publishes a terminal current only after that projection succeeds; a completed
turn without a visible assistant or terminal tool item is reported as a
failure rather than exposed as a successful user-only transcript.
`ThreadView.Failure` exposes the canonical terminal failure code and message.
Hosts classify terminal failures from `Failure.Code`; there is no parallel
text-error field.
`TurnResult.Output` remains the run-level aggregate result and is not appended
as a new assistant item. Different stable assistant IDs remain distinct even
when their text is equal. A canonical snapshot is applied only if the
in-memory view version is unchanged since the snapshot read began; this keeps
an older read from replacing a newer terminal view.

`ThreadService.List` reads root lifecycle facts from one canonical inventory
snapshot. `ThreadSummary` carries the current turn and run identity, attention, queue
count, pending input, typed terminal failure, and bounded last-item preview
needed by a host list. It does not hydrate complete timeline, attachment,
context, or SubAgent detail for every thread; those remain selected-thread
queries.

`ThreadContextReader.Context` keeps two scopes explicit: `Usage` is the latest
context-pressure status, while `UsageTotals` folds disjoint input, output,
cache-read, and cache-write counts from canonical final provider-usage entries
across the thread. Final usage reaches both projections through the same
attempt-scoped event decoder. Projected request estimates are never included in
totals.

After a final provider-usage context status is appended successfully,
`runtime.Event.ThreadUsageTotals` publishes the updated value from that same
canonical fold. It is absent from projected requests, stream usage, rejected
attempts, and uncommitted events. Hosts may use it for immediate display, then
reload `ThreadContextReader.Context` after reconnect; they must not accumulate
or persist a parallel total.

## Agent and provider boundary

`runtime.NewAgent` snapshots the configured profile, system prompt, gateway,
tools, effect policy, capabilities, context policy, and execution limits. The
provider gateway is the only model transport boundary. Provider credentials,
editable profiles, endpoint authorization, uploads before admission, and UI
rendering remain host-owned.

The host may return an Agent with a different provider, model, system prompt,
tool surface, or reasoning policy for a new Turn. The first provider checkpoint
freezes that complete surface for the Turn. Ask User, tool loops, retries, and
restart recovery reuse it exactly. Floret keeps the canonical path intact,
starts or resumes the surface's independent render lineage, and clears
provider-native continuation state across a surface switch.

The provider-context v6 projection establishes one explicit render boundary
for earlier projectors, including v5. Canonical lineage hashes durable typed
conversation facts rather than journal entry and parent identities, which may
be assigned differently during an active tool loop and later canonical
reconstruction. The first request after a projector change uses a new render
lineage and the complete typed canonical history. It does not create a
compaction, rewrite the journal, or reuse old provider continuation state.

`ThreadContextReader` treats each committed Turn policy as the boundary for the
latest context-usage sample. Until that Turn records its first request status,
the snapshot exposes the new provider, model, and policy without carrying the
previous Turn's usage. Canonical whole-thread token totals remain cumulative
across the boundary.

A provider natural stop completes the Turn. Ordinary tool results continue the
provider loop, while `ask_user` waits and resumes the same frozen Turn. A tool
call whose definition is unavailable returns a safe ordinary error result and
continues. Historical tool calls and results remain typed and paired even when
their definition was removed; only current definitions are exposed for new
calls. Public Ask User answers appear once in the paired tool result. Secret
answers leave a redacted result and reach only the current resume request as an
ephemeral overlay.

## Durable schema

The internal session-tree domain has a permanent contiguous v2 -> v3 -> v4 ->
v5 -> v6 -> v7 -> v8 migration lineage. `runtime.Open` runs domain migration, logical schema
update, and final invariant verification in one backend transaction. Unknown,
future, corrupt, or drifted state fails closed without changing canonical
records. `runtime.Options.StartupProgress` reports only the product-neutral
`migrating` and `verifying` phases; it exposes no record counts or content.
Startup selects one exact source format, rejects mixed authority, decodes an
unchanged current-v8 store once, and performs a persisted final verification
after every startup write. Current-schema startup is byte-preserving and
idempotent. The v5 -> v6
edge first replays pending v5 recovery frames and accepts only the exact v5
tool-result Raw repair produced before UTF-8 normalization was enforced. It then
writes segmented v6 authority and removes the legacy checkpoint, full-path root
inventory, and diff journal in the same transaction. The v6 -> v7 edge
permanently fills exact run identity, removes retry authority, and terminates
active unknown effects before the Host becomes available. The v7 -> v8 edge
classifies only the exact Engine continuation user message paired with its
`context_continue` save point as a control signal, restoring one canonical user
input per Turn. Every other mismatch
still fails closed. This automatic domain convergence is separate from the explicit
legacy physical conversion
surface; normal startup never dual-reads or converts that external schema.

Startup errors from internal session-tree authority validation are exposed
through `runtime.ErrAuthorityCorrupt`; hosts do not inspect internal errors or
physical records to classify them.

## Destructive storage reset

`PreflightStorageReset` and `ResetStorage` are the only public destructive
maintenance boundary for Floret-owned records. The caller must first stop the
`Host`, validate the exact environment and target through its own ownership
manifest, and pass that environment ID, operation ID, and manifest digest to
Floret. A backend without `storage/spi.MaintenanceResetter` fails closed.

Reset never reads, exports, or migrates prior domain records. A supported
backend removes all opaque Floret records through one backend-owned
transaction; Floret then initializes and verifies the current schema. The
operation is idempotent. Failure after old records are cleared leaves no old
authority to reopen, so the host must keep readiness closed and retry reset or
initialize a new store instead of falling back to normal startup.

## Packages

Use `identity`, `config`, `runtime`, `observation`, `tools`, `provider`, and
opaque `storage.Source` values. Downstream applications must not import
`internal/*` or infer lifecycle state from physical storage records.
