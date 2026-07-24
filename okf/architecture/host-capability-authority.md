---
type: Architecture Concept
title: Host Capability Authority
description: Normative authority states, public object graph, storage transitions, and failure invariants for Floret host capabilities.
resource: /runtime/thread_capabilities.go
tags: [architecture, runtime, capability, authority]
timestamp: 2026-07-20T00:00:00Z
---

# Status

This document is the frozen normative authority contract for the next Floret
host boundary. Independent design review approved its durable states, owners,
transitions, public capabilities, and failure points before implementation
resumed. Implementation must map to this document; discovering a new state or
transition reopens design review instead of creating a local workaround.

The v0.19 amendment reopens only interrupted-turn recovery delivery. It adds no
durable state or transition: the store-wide binder now snapshots one active turn
authority and produces a factory bound to that stable recovery target before
coordinator delivery. The factory may refresh only heartbeat and expiry fields
for the same turn owner and generation.

The v0.20 amendment makes already-owned Agent facts explicit: exact turn
admission/replay and public turn-page indexes, typed terminal failure, aggregate
root/descendant approval authority, automatic-title generation authority, and
durable user-message references. These remain Floret Store facts. A downstream
host may hold unadmitted input and product policy, but it cannot persist a second
title, turn/run lifecycle, approval queue, failure, retry mapping, or admitted
message-reference record.

# Boundary

Floret owns admitted Agent state. A downstream host owns product settings,
resources, and work not yet admitted to Floret. Cross-store work is coordinated
by an explicit host coordinator, but only Floret may create, mutate, recover,
close, fork, or delete Floret canonical Agent state.

Authority is enforced by both the public object graph and the durable storage
kernel. Services and runs never receive a raw `Store`, bootstrap scope, binder,
unbound factory, raw repo, or generic mutation API. Missing, closed, deleted,
claimed, leased, expired, or mismatched authority fails before provider, tool,
journal, metadata, event, registry, or file side effects.

# Authority Axes

Thread authority has three orthogonal axes. They are not one status enum.

| Axis | States | Meaning |
| --- | --- | --- |
| Canonical lifecycle | `absent`, `open`, `closing`, `closed`, `deleted` | whether the identity never existed, accepts Agent work, is fenced for an explicit child close, is a terminal child, or is represented only by a deletion tombstone |
| Structural authority | `unclaimed`, `fork-claimed` | whether a replayable root fork exclusively owns structural mutation |
| Execution authority | `unleased`, `turn-leased`, `mutation-leased` | whether no execution owns the thread, one provider-backed turn owns it, or one long-running provider-backed mutation owns it |

Short provider-free mutations enter mutation authority and leave it within one
atomic storage transaction. They never expose a durable intermediate
`mutation-leased` state. Compaction is the only mutation that spans a provider
call and therefore retains a durable mutation lease.

## Canonical Lifecycle

`absent` means no thread row and no deletion tombstone exist. Only an exact
root-create capability, a root-fork commit, or an exact parent-bound SubAgent
publication may publish the identity.

`open` means the thread exists and may participate in operations allowed by its
claim and lease axes.

`closing` applies only to a child subtree owned by one durable
`CloseOperationID`. It rejects new inbox publication, admission, child
publication, provider/tool dispatch, title/todo mutation, fork, and delete. An
already active target-child turn may only renew long enough to commit its
cancelled terminal outcome and release its exact proof. Reads remain valid.
Closing is not silently reverted after a crash; the same exact parent-bound close
capability must resume it.

`closed` applies only to child threads. Reads remain valid. New leases, journal
writes, inbox writes, title or todo writes, child publication, direct fork, and
lifecycle mutation are rejected. A closed terminal child may be a pinned source
inside its root parent's replayable fork plan.

`deleted` is a durable tombstone, not an inferred missing row. Root-tree delete
writes tombstones for the root and all descendants in the same Floret Store
transaction that removes queryable Agent data. Deleted `ThreadID` values are not
reusable. Delete replay is idempotent against the root tombstone. Every other
operation returns an explicit deleted-authority error. Tombstones permanently
retain the canonical identity provenance required to reject identity reuse and
classify replay: a created root retains its `CreateIntentID` and create
fingerprint, while every fork destination retains its fork `OperationID`, plan
node identity, and source provenance.

Create-intent, fork-operation, SubAgent `PublicationID`, and SubAgent
`InputRequestID` idempotency records survive root-tree deletion permanently as
internal authority ledgers; they are not queryable Agent state. Root-create and
fork capabilities that explicitly support tombstoned authority compare retained
request identity/fingerprint before classifying an exact replay as deleted.
SubAgent capability construction instead requires a live parent: after root-tree
deletion `SubAgentHostFactory.NewHost` returns `ErrThreadDeleted` before any
operation can run. Retained SubAgent ledgers prevent the same request ID from
being reused under another live parent, where changed fingerprint or authority is
`ErrRequestConflict`; they do not create a tombstone replay capability.

## Fork Claim Proof

A prepared root fork is identified by:

```text
OperationID
RequestFingerprint
SourceThreadIDs
AuthorityThreadIDs
PlanHash
```

`PrepareFork` atomically pins every source path, terminal child, destination
identity, parent relation, identity rewrite, and thread-scoped artifact manifest;
persists one immutable plan; and claims every source and destination identity.
A destination is absent while the operation is prepared. No destination journal
or artifact becomes visible before commit.

A claim blocks create, lease acquisition, append, metadata, todo, leaf move,
SubAgent publication, inbox publication, admission, close, another fork, and
delete. Reads of existing sources remain valid. Missing claims, extra claims,
changed plans, or visible destinations while an operation is still prepared are
authority corruption. Recovery fails rather than recreating or inferring claims.
Returning an already-persisted exact publication/input replay is a read, not a
publication, and does not conflict with a claim.

The fork operation states are exactly:

| State | Durable shape | Allowed transition |
| --- | --- | --- |
| `prepared` | immutable plan plus complete claim set; no destinations visible | `CommitFork`, `FailFork`, or replay same request |
| `completed` | immutable plan and result stored; either all planned destinations are matching live canonical rows, or all are matching provenance tombstones after atomic root-tree deletion; no claims | idempotent read/replay only |
| `failed` | immutable plan and deterministic error stored; no live row or tombstone carries this operation/node provenance; no claims | idempotent error replay only |

`CommitFork` creates every planned destination and its artifact copies, stores
the completed result, and releases every claim in one transaction. Partial
publication is impossible.
`FailFork` is allowed only when no destination is visible and atomically stores a
deterministic failure plus claim release. Transient storage or cancellation
errors leave the operation prepared so the same source-bound `ThreadForkHost`
and `OperationID` can replay it. There is no compensation delete.

After `FailFork`, the planned destination IDs return to ordinary
`absent + unclaimed` authority and may later be used by any otherwise-authorized
canonical creation operation: exact root create, root-fork commit, or
parent-bound SubAgent publication. Failed replay always returns the stored
deterministic error; it does not inspect or claim an unrelated later occupant. A
live row or tombstone that carries the failed operation's own `OperationID` and
plan-node provenance is `ErrAuthorityCorrupt`, because `FailFork` can commit only
before publication.

A completed fork operation is permanent. Replay validates the immutable result
against every planned destination. If every destination is the matching live
canonical row, replay returns the stored completed result. If the destination
tree was later atomically deleted and every planned node has a tombstone carrying
the exact operation, plan-node, and source provenance, replay returns
`ErrThreadDeleted`. A destination with neither the matching live row nor the
matching tombstone is `ErrAuthorityCorrupt`. Every live/tombstoned mixture is
also corruption because root-tree deletion tombstones the complete planned
destination tree atomically. Replay never recreates a destination or rewrites
the completed result.

## Lease Proof And Liveness

The storage kernel assigns each durable lease an exact proof:

```text
ThreadID
Purpose          // turn or mutation
TurnID           // required for purpose=turn
MutationID       // required for purpose=mutation
MutationKind     // compaction for a durable mutation lease
OwnerID
Generation       // monotonically increasing for the ThreadID
Heartbeat        // monotonically increasing within the generation
AcquiredAt
RenewedAt
ExpiresAt
```

Exactly one of `TurnID` and `MutationID` is set. `OwnerID` names the local owner;
`Generation` is the durable fencing token. The storage kernel assigns generation
and timestamps. Equality of thread and work identity without the same generation
is not authority.

Every Store has one persisted lease policy: `LeaseTTL`, `RenewInterval`, and
`ClockSkewAllowance`. Every process opening the same durable Store must use that
policy. `RenewInterval` is at most one third of `LeaseTTL`. The Store authority
clock is UTC wall time supplied by the Store configuration; production Store
instances sharing one database must use the same host clock, and deterministic
tests use one shared fake clock.

A lease has these liveness states:

```text
fresh:               now <= ExpiresAt
expired fenced:      ExpiresAt < now <= ExpiresAt + ClockSkewAllowance
takeover eligible:   now > ExpiresAt + ClockSkewAllowance
```

`RenewLease` compares the complete proof, increments `Heartbeat`, and advances
`RenewedAt` and `ExpiresAt` without changing generation. Provider streaming,
tool execution coordination, approval waits, and other long turn waits keep the
lease fresh. If renewal fails or the lease reaches `expired fenced`, the local
owner cancels its execution context, stops new provider/tool dispatch, and can
no longer write. Every proof-bound storage write also checks freshness.

Every takeover begins with one atomic comparison of the complete old proof and
takeover eligibility, then increments generation. There is no
clear-then-acquire sequence. Turn recovery performs finalization and releases
the transaction-local new generation in that same transaction. Compaction
takeover is different: it atomically replaces the expired mutation proof and
returns a new durable generation to the same immutable `RequestID`; that owner
then renews during the provider call and finishes through `FinishCompaction`.
A run-failure marker never makes a fresh lease stale.

A local active `Thread` stores the exact proof returned by admission. Active
settlement is allowed only when that local proof equals the current durable
proof. A lease discovered through a separate Store query does not establish
local ownership. Release requires the exact proof and can never release a newer
generation.

## SubAgent Input Lifecycle

SubAgent pending work has its own durable lifecycle:

| State | Meaning | Allowed next state |
| --- | --- | --- |
| `pending` | published to the open child inbox but not admitted as a turn | `admitted` or `cancelled` |
| `admitted` | atomically bound to one canonical child user message and turn | terminal; no further input-state change |
| `cancelled` | explicitly superseded or closed before admission | terminal; no further input-state change |

There is no `consumed` state. A consumed-without-user-message shape is invalid.

Every spawn request carries a durable `PublicationID`; every later send carries
a durable `InputRequestID`. The spawn fingerprint covers parent and parent turn,
child identity, task name/description, host profile reference, child metadata,
create versus fork-copy mode, pinned fork source thread/entry/path hash, first
message, attachments, labels, and input request identity. The send fingerprint
covers parent, child, message, attachments, labels, interrupt mode, and any
structured pending-tool completion reference.

Replay of the same request ID and fingerprint returns the same
`SubAgentInputID`. Reuse with any changed fingerprint field is a public request
conflict. The Store assigns the immutable `SubAgentInputID` during publication.
It also assigns a strictly increasing sequence within the child inbox. Sequence
is durable ordering authority; timestamps and journal scans are not.

Every SubAgent operation first revalidates its bound live parent. After that
parent check, publication checks an existing `PublicationID` or `InputRequestID`
before child lifecycle rejection. Exact same-fingerprint replay is read-only and
returns the stored publication/input after that input becomes admitted or cancelled, and
regardless of a live child's claim, lease, `closing`, or `closed` state. It does
not reopen the child, publish another input, alter sequence, or start a provider.
A changed fingerprint remains `ErrRequestConflict`; a new request against a
claimed, `closing`, or `closed` child returns the authority/lifecycle error. After
root-tree deletion no SubAgent operation is reachable because parent-bound host
construction returns `ErrThreadDeleted`. Reusing the retained request ID through
another live parent returns `ErrRequestConflict`.

Interrupt publication atomically cancels all earlier pending inputs and
publishes the new pending input. Admission and close are serialized with inbox
publication so exactly one terminal input state is committed.

## Pending Tool Identity

The exact pending target is:

```text
ThreadID
TurnID
RunID
ToolCallID
ToolName
Handle
EffectAttemptID   // required for a locally dispatched effect; empty for non-local/hosted tools
```

For a child target, the parent-child relation is additional capability authority
and is revalidated before mutation. A settlement outcome fingerprint covers the
complete target, terminal status, summary, output hash, and activity payload
hash. Replay with the same fingerprint is idempotent; any different terminal
outcome is a conflict.

## Effect Dispatch Authority

Floret owns validated local tool dispatch. The host owns current product
permission policy and approval evidence. They meet at one narrow public adapter:

```text
EffectAuthorizationGate.Dispatch(
  context,
  EffectAuthorizationRequest,
  AuthorizedEffect(execution context, EffectAuthorizationProof),
) -> EffectDispatchResult
```

The callback execution context is selected by the host gate and reaches the
tool handler. Floret composes it with the active turn context before the
`prepared -> dispatching` boundary, so either scope may cancel execution and no
host-selected context can extend the handler beyond canonical turn lifetime.
Cancellation before dispatch remains execution cancellation: `FinishTurn`
atomically cancels the prepared attempt instead of recording an authorization
rejection. A nil context is a contract error and never enters the handler.

Before calling the host gate, Floret persists one canonical effect attempt. The
Store assigns `EffectAttemptID`; callers and adapters cannot choose it. A unique
constraint over `{ThreadID, TurnID, RunID, ToolCallID}` permits exactly one
attempt for the canonical invocation. Concurrent preparation with the same
fingerprint returns that attempt. Any different fingerprint is
`ErrRequestConflict`. No state permits a replacement attempt for the same
invocation; retry requires a new canonical tool call identity. The attempt states
are:

| State | Handler boundary | Replay rule |
| --- | --- | --- |
| `prepared` | not crossed | same fingerprint may resume authorization; `FinishTurn` or interrupted recovery may cancel |
| `dispatching` | crossed; handler may or may not have produced effects | never dispatch again |
| `completed` | handler returned success and canonical result committed | return committed result |
| `failed` | handler returned error and canonical result committed | return committed failure; never dispatch again |
| `rejected` | authorization/adapter failed before handler | return rejection; never dispatch that attempt |
| `unknown` | process or persistence failed after `dispatching` | terminal attention state; never dispatch again |
| `cancelled` | close/recovery cancelled while still `prepared` | terminal; handler was not called |

The host audit is append-only evidence, not effect lifecycle authority.
Authorization decisions are keyed by `EffectAttemptID`, request fingerprint,
`PolicyRevision`, and an increasing authorization decision sequence. A crash
after host audit commit but before handler entry safely resumes the same
`prepared` effect attempt. The gate always rereads current policy: when revision
is unchanged it may reuse the exact audit decision; when revision changed, the
old decision cannot authorize dispatch and the gate appends a new decision under
the same effect attempt. Only the proof for the current revision may reach the
closure.

If current policy denies the invocation, or policy read, approval validation,
audit persistence, or the host gate fails before the one-shot closure is invoked,
Floret calls `RejectEffectAttempt` with the exact fresh turn proof, attempt ID,
request fingerprint, typed public rejection code, and a rejection fingerprint.
The Store requires state `prepared` and atomically records `rejected`. Replay with
the same attempt, request, code, and rejection fingerprint returns the stored
rejection; any changed terminal rejection is `ErrRequestConflict`. A storage
failure leaves the attempt `prepared`, calls no handler, and may retry that same
rejection commit while the local call retains the result, or let turn finalization
cancel the attempt. There is no durable rejecting state. Any later replay that
observes only `prepared`, including after process loss, rereads current host
policy and may authorize or reject according to that current policy.

`EffectAuthorizationRequest` contains `EffectAttemptID`, exact `ThreadID`,
`TurnID`, `RunID`, `ToolCallID`, tool name, argument hash, declared
resources/effects, read-only/destructive/open-world classification, and the
originating lease `OwnerID`, `Generation`, and observed `Heartbeat`. It contains
no Store, handler, provider, raw policy record, or host database handle.

The host gate serializes current-policy changes and host delete/disable intent
for that authority thread. In one gate hold it reads current policy/lifecycle,
validates approval, and idempotently persists the strict host audit proof. A
delete or disable intent denies dispatch. The proof contains the complete effect
attempt/invocation identity, originating lease owner/generation, policy revision,
approval identity when present, audit reference/hash, and authorization time. It
is valid only for the synchronous `Dispatch` call.

Floret creates `AuthorizedEffect` as a sealed one-shot capability. It can be
called synchronously at most once before `Dispatch` returns. The first call
atomically consumes its local token; a second call, a deferred call, or a call
after return fails before storage or handler access. The host cannot construct a
replacement capability.

At the handler boundary, the one-shot closure:

1. validates the authorization proof against the exact request and audit identity;
2. rereads the durable lease and requires the same `OwnerID` and `Generation`, a
   fresh `ExpiresAt`, and a current `Heartbeat` not older than the observed value;
3. requires lifecycle `open`, the same local Thread proof, and effect attempt
   state `prepared`;
4. atomically transitions the attempt to `dispatching`;
5. invokes the one already resolved local handler exactly once; and
6. atomically commits `completed` or `failed` plus the canonical tool result.

Renewal may advance heartbeat while authorization waits; it does not invalidate
the proof when owner and generation are unchanged. Expiry, takeover, closing,
proof mismatch, or invalid authorization rejects before step 4 and before the
handler.

Crossing `prepared -> dispatching` is the per-invocation at-most-once boundary. A crash after
that commit is never automatically retried, even if the handler may not have
started. `InterruptedTurnRecoveryHost` converts `dispatching` to `unknown` and
`prepared` to `cancelled` before finalizing the turn. When a pre-existing
`EntryRunFailure` belongs to the interrupted turn, recovery binds its exact
canonical entry ID, message, and raw hash into the recovery fingerprint and
terminal proof. Replay rereads that entry from the current canonical ancestry
and fails closed on identity, ancestry, message, raw, or hash drift. `unknown`
is not converted
into a pending target and is never used to infer a handler result. A later user
or host action may acknowledge the uncertainty, but it cannot mutate the attempt
into a known result or re-invoke the original handler.

The closure mints an opaque sealed `EffectAuthorizationReceipt` after the
handler result is canonically committed. The host gate can only return the exact
`EffectDispatchResult` produced by the closure. Floret also retains that local
result, so a missing, forged, or changed return value cannot cause effect retry.
If the handler ran but canonical result persistence or adapter return failed,
Floret returns a committed-dispatch error and converges the attempt without
retrying the handler. Each ordered finalizer creates its own cancellation-
detached bounded persistence context only when invoked. It first tries
`FinishEffectDispatch` with the captured handler outcome; any outcome
fingerprint, canonicalization, adapter-return, or finish persistence failure
then uses a fresh context to call `MarkEffectUnknown` under the exact proof.
Every sibling finalizer is attempted even when an earlier one fails. If both
finish and mark-unknown persistence fail, the turn stays non-terminal and exact
interrupted recovery performs the same conservative transition after takeover.
No available-store path leaves a handler-dispatched effect permanently
`dispatching`, and no failure path calls the handler twice.

Policy downgrade waits for an in-flight authorized effect to return; a later
effect observes the new policy.

## Aggregate Approval Authority

Approval authority is one durable queue per canonical root, covering that root
and all of its canonical descendants. Queue order is assigned by Floret from
canonical request facts; timestamps and host event order are not authority.
Exactly one item is `Current`. Only that item may move from `requested` to
`decision_submitted`, and `ResolveApproval` compares the exact root, approval,
effect, turn/run, queue generation, item revision, and stable `DecisionID`.

The durable approval states are exactly:

| State | Meaning | Allowed next state |
| --- | --- | --- |
| `requested` | queued and not yet assigned a host decision | `decision_submitted`, `cancelled`, or `timed_out` |
| `decision_submitted` | immutable decision ID and choice committed; host gate may run | one terminal state |
| `approved` | valid proof committed and matching effect crossed dispatch | terminal |
| `rejected` | user or current product policy denied before dispatch | terminal |
| `failed` | authorization service or proof contract failed before dispatch | terminal |
| `timed_out` | no decision arrived before the canonical deadline | terminal |
| `cancelled` | turn cancellation won before dispatch | terminal |

An exact response-loss replay returns the same submitted or terminal receipt and
does not invoke the host gate again. Final approval settlement, effect state,
authorization proof hash, and promotion of the next queue item are one authority
transaction. If cancellation wins before that transaction, a late proof cannot
approve or dispatch. If the proof transaction wins, later cancellation is
post-dispatch and must preserve the known handler outcome or mark it unknown;
it cannot rewrite a completed side effect as pre-dispatch cancellation.

User rejection maps to approval/effect rejection with `user_rejected`; current
policy denial maps to `policy_denied`; an unavailable gate maps to approval
failure and effect rejection with `authorization_unavailable`; invalid proof
maps to `authorization_contract`. These mappings are unique. A downstream host
owns policy and UI, submits decisions, and maps conflicts, but it does not
materialize, order, or promote an approval queue.

Each local active `Thread` owns an in-process authority gate tied to its exact
lease generation. Provider request start and tool dispatch enter that gate. Tool
dispatch holds it through authorization, handler invocation, and canonical result
commit. `PrepareSubAgentClose` takes the exclusive side, waits for in-flight
effect dispatch, revalidates local/durable proof, commits `closing`, and blocks
new provider/tool starts for that local generation.

This produces one ordering for local irreversible effects:

```text
canonical prepared effect attempt + fresh Floret turn proof
  -> local Thread authority gate
  -> current host policy gate + durable host audit proof
  -> final durable generation/freshness check
  -> canonical dispatching boundary
  -> exact local handler dispatch
  -> canonical effect result
```

No production local handler entry point exists without both gates and the
canonical effect-attempt transition. Provider request start does not require host
permission authorization; it requires the exact fresh turn proof and local
Thread gate. Provider-native hosted tools are not dispatched through the local
handler runtime.

# Valid Durable Combinations

| Lifecycle | Structural | Execution | Valid use |
| --- | --- | --- | --- |
| `absent` | `unclaimed` | `unleased` | available for exact root create or SubAgent publication |
| `absent` | `fork-claimed` | `unleased` | reserved root-fork destination |
| `open` | `unclaimed` | `unleased` | idle canonical thread |
| `open` | `unclaimed` | `turn-leased` | one admitted provider-backed turn |
| `open` | `unclaimed` | `mutation-leased` | one provider-backed compaction |
| `open` | `fork-claimed` | `unleased` | pinned root-fork source |
| `closing` | `unclaimed` | `unleased` | close operation prepared; awaiting final atomic close |
| `closing` | `unclaimed` | `turn-leased` | exact target-child turn is cancelling/finalizing under the prepared close |
| `closed` | `unclaimed` | `unleased` | readable terminal child |
| `closed` | `fork-claimed` | `unleased` | pinned terminal child source |
| `deleted` | `unclaimed` | `unleased` | tombstoned identity; root delete replay only |

Every other combination is invalid. Claims and leases never coexist. Closed or
deleted identities never hold leases. Deleted identities are never claimed.
Closing descendants share one immutable close operation identity.

# Store Lifetime

Public Store lifetime is per opened Store instance and is `open`, `closing`, or
`closed`. Closing one SQLite Store does not invalidate another independently
opened Store; durable claim and lease fencing coordinates their operations.

`Close` linearizes `open -> closing`, rejects new bootstrap, bind, `NewHost`, and
operation starts with `ErrStoreClosed`, cancels Store-owned execution contexts,
and waits for active operations to finish their proof-bound finalization. If
finalization or physical cleanup fails, `Close` returns that error and remains
`closing`; replay retries cleanup but never admits new work. Successful cleanup
transitions to `closed`. Repeated `Close` on `closed` succeeds.

Binders, factories, and handles share the Store lifetime token. Retained values
cannot start work after `closing` and fail before side effects.

# Public Object Graph

```text
trusted composition root
  -> Store lifetime owner
    -> ConfigureHostCapabilities(Store, callback) exactly once
      -> HostBootstrap, callback lifetime only
        -> one responsibility-specific binder per required family
          -> exact ThreadID or ParentThreadID factory/handle
            -> one service or operation owner
```

`HostBootstrap` has no exported methods. It is sealed on callback success,
error, or panic. Copies share the sealed state. Binders become usable only after
callback success.

The composition root is the trusted capability issuer. Exported binders cannot
mathematically prevent a malicious Go caller from forwarding them, so the
repository must enforce the trust boundary mechanically: only the composition
package may mention binder types or call binder methods; service constructors
accept exact bound factories/handles only; architecture tests and downstream
boundary scripts reject binder or raw Store fields outside composition.

Only the Store lifetime owner may retain `Store` and binders. A service may
retain an exact identity-bound factory or handle. A run may retain only the exact
handle it executes. Provider options contain configuration and adapters only;
they never contain Store, bootstrap, binder, root identity, parent identity, or
authority selectors. `EffectAuthorizationGate` is an allowed host-policy adapter;
it receives authority only through each exact invocation request. Public `Store`
exposes only `Close`.

## Capability Matrix

| Actor | Long-lived value allowed | Bound authority | Operations |
| --- | --- | --- | --- |
| Store lifetime owner | `Store`, narrow binders | whole Store for trusted composition only | configure once, close |
| Root create coordinator | `ThreadCreateHost` | one exact root ID plus durable `CreateIntentID` | create or replay that root intent only |
| Root read owner | `ThreadReadHost` | one existing root | canonical reads, the durable root-and-descendant approval queue, and exact Floret artifact reads only |
| Title coordinator | `ThreadTitleHost` | one existing root | manual title only |
| Fork coordinator | `ThreadForkHost` | one source root | prepare/commit/replay one explicit fork request |
| Delete coordinator | `ThreadDeleteHost` | one existing or tombstoned root | delete or replay that root tree |
| Turn owner | `TurnExecutionHostFactory` or `TurnExecutionHost` | one root | run, retry, pending completion, active settlement, approval read, todo update |
| Compaction owner | `ThreadCompactionHostFactory` or `ThreadCompactionHost` | one root | compact/replay one explicit request |
| Interactive SubAgent owner | `SubAgentHostFactory` or `SubAgentHost` | one parent | publish child/input or child pending completion, explicitly wait/admit direct children, active child settlement, close one child subtree |
| SubAgent read owner | `SubAgentReadHost` | one parent | list direct children; read any current descendant canonical detail or artifact |
| Pending settlement recovery owner | `PendingToolRecoveryHost` | one root or one exact parent | provider-free exact settlement |
| Interrupted recovery target owner | `InterruptedTurnRecoveryHostFactory` | one root or exact parent-child pair plus one `TurnID`, `OwnerID`, and `Generation` | refresh the complete proof for that same target only |
| Interrupted recovery attempt owner | `InterruptedTurnRecoveryHost` | one complete current proof for the factory's target | finalize that target once it is takeover eligible |
| Host effect authorization owner | `EffectAuthorizationGate` | current host policy resolved for one exact invocation | authorize, audit, and dispatch one supplied effect closure under the host policy gate |

`ThreadCreateHostBinder.Bind(ThreadID, CreateIntentID)` binds before delivery. A
create service cannot choose another root ID or intent in `CreateThread`. Every
other binder similarly binds root, parent, or parent-child identity before a
service receives a value. In particular,
`InterruptedTurnRecoveryHostBinder.BindThread(Context, ThreadID)` and
`BindSubAgent(Context, ParentThreadID, ChildThreadID)` require one active turn
lease and return exact factories bound to `{ThreadID, ParentThreadID, TurnID,
OwnerID, Generation}`. A recovery coordinator may retain those factories because
they cannot select another thread or later turn. `NewHost` rereads the complete
proof and may refresh only `Heartbeat`, `RenewedAt`, and `ExpiresAt` while the
stable target identity remains equal. This lets a retry observe renewal without
retaining the store-wide binder or following replacement authority.

There is no `PendingToolSettlementHost`. Active settlement belongs to the active
`TurnExecutionHost` or `SubAgentHost`. Provider-free settlement and interrupted
turn recovery are separate capabilities and cannot derive active authority.

Factories bind identity before provider-backed construction. `NewHost` validates
Store lifetime and canonical root/parent authority before skills, tools, sinks,
registries, gateways, provider state, or other construction side effects.
`SubAgentHostFactory.NewHost` requires a live open parent; a tombstoned parent
returns `ErrThreadDeleted`, so deleted SubAgent request ledgers are never exposed
through an operation-capable host.

## Exact Method Sets

Public capability types have no exported methods beyond this list:

| Type | Methods |
| --- | --- |
| `Store` | `Close` |
| `HostBootstrap` | none |
| `ThreadCreateHostBinder` | `Bind` |
| `ThreadReadHostBinder` | `NewHost` |
| `ThreadTitleHostBinder` | `NewHost` |
| `ThreadForkHostBinder` | `NewHost` |
| `ThreadDeleteHostBinder` | `NewHost` |
| `TurnExecutionHostBinder` | `Bind` |
| `ThreadCompactionHostBinder` | `Bind` |
| `SubAgentHostBinder` | `Bind` |
| `SubAgentReadHostBinder` | `NewHost` |
| `PendingToolRecoveryHostBinder` | `NewThreadHost`, `NewSubAgentHost` |
| `InterruptedTurnRecoveryHostBinder` | `BindThread`, `BindSubAgent` |
| `TurnExecutionHostFactory` | `NewHost` |
| `ThreadCompactionHostFactory` | `NewHost` |
| `SubAgentHostFactory` | `NewHost` |
| `InterruptedTurnRecoveryHostFactory` | `NewHost` |
| `ThreadCreateHost` | `CreateThread` |
| `ThreadReadHost` | `ReadThread`, `ReadThreadOverview`, `ListThreadTurns`, `ReadLatestThreadTurn`, `ListThreadDetailEvents`, `ReadThreadContext`, `ReadThreadAgentTodos`, `ReadApprovalQueue`, `ReadTurnProjection`, `ReadArtifact` |
| `ThreadTitleHost` | `SetThreadTitle` |
| `ThreadForkHost` | `ForkThread` |
| `ThreadDeleteHost` | `DeleteThread` |
| `TurnExecutionHost` | `RunTurn`, `RetryTurn`, `CompletePendingTool`, `SettlePendingTool`, `ReadApprovalQueue`, `ResolveApproval`, `UpdateThreadAgentTodos` |
| `ThreadCompactionHost` | `CompactThread` |
| `SubAgentHost` | `SpawnSubAgent`, `SendSubAgentInput`, `PublishPendingToolCompletion`, `WaitSubAgents`, `SettlePendingTool`, `CloseSubAgent` |
| `SubAgentReadHost` | `ListSubAgents`, `ReadSubAgentDetail`, `ListSubAgentActivityTimeline`, `ReadArtifact` |
| `PendingToolRecoveryHost` | `SettlePendingTool` |
| `InterruptedTurnRecoveryHost` | `RecoverInterruptedTurn` |
| `EffectAuthorizationGate` | `Dispatch` |

No aggregate capability bundle, conversion method, raw Store accessor, generic
thread host, unbound create handle, or method alias is public.

# Storage Authority Kernel

These are semantic operations, not optional helper compositions. Each backend
implements validation and commit in one critical section or transaction.

| Operation | Atomic responsibility |
| --- | --- |
| `CreateRoot` | reject claimed/tombstoned/wrong-shape identity; publish or replay one exact root plus create-intent fingerprint |
| `AdmitTurn` | acquire a fresh turn generation and append turn-start plus canonical user input |
| `AdmitRetry` | pin/move retry target, acquire turn generation, and append turn-start atomically |
| `ListCanonicalTurns` | read exact started-turn entry indexes with entry-identity before/since cursors; reject stale path cursors without falling back to a scan |
| `RenewLease` | compare exact fresh proof and advance heartbeat/expiry |
| `AppendWithProof` | compare exact fresh proof before journal, provider-state, or todo write |
| `PrepareApprovalBatch` | validate the complete local-call batch, create/replay every effect and approval record, append requested audit entries, and update the aggregate root queue atomically before any handler dispatch |
| `ResolveApproval` | compare exact current queue/item CAS and persist one immutable decision receipt before host-gate execution |
| `CommitApprovalDispatch` | atomically validate the authorization proof and move the matching approval/effect into approved/dispatching authority |
| `FinalizeApproval` | atomically settle approval and effect outcome, proof hash, audit entry, and next-item promotion |
| `CancelApprovalBatch` | atomically cancel every still-pre-dispatch item for an exact turn and promote the aggregate queue |
| `PrepareEffectAttempt` | Store-assign the single attempt for one canonical invocation, or replay/conflict its exact fingerprint |
| `RejectEffectAttempt` | compare exact fresh turn proof and one `prepared` attempt, then persist one typed terminal rejection or replay/conflict it |
| `BeginEffectDispatch` | final-check one-shot authorization, exact current fresh generation, lifecycle, and `prepared` state; transition to `dispatching` |
| `FinishEffectDispatch` | persist handler result, optional immutable full-output artifact, and canonical tool result as one `completed` or `failed` transition without re-invoking handler |
| `MarkEffectUnknown` | under the exact local turn proof, terminalize a `dispatching` attempt whose result cannot be durably completed, without handler retry |
| `RecoverEffectAttempt` | turn `prepared` into `cancelled` and `dispatching` into `unknown` during interrupted-turn recovery |
| `FinishTurn` | cancel remaining `prepared` effects, reject any `dispatching` effect, require all attempts terminal-safe, append terminal outcome/provider state, release proof, and preserve matching `closing` |
| `ValidateInterruptedTurnResolution` | in one read transaction validate the current authority snapshot plus the bound target's admission, normal/recovery finish, and terminal ledgers before a factory permanently resolves it |
| `AdmitPendingToolCompletion` | validate/settle exact pending target and admit one new host-authored continuation turn |
| `BeginCompaction` | persist request identity and acquire fresh compaction mutation proof |
| `TakeOverCompaction` | compare one takeover-eligible compaction proof and replace it with a durable new generation for the same request |
| `FinishCompaction` | persist exact result/failure and release exact mutation proof |
| `PrepareFork` | pin immutable complete journal/artifact plan and claim every source/destination identity |
| `CommitFork` | publish all destinations and thread-scoped artifact copies, store completed result, and release all claims |
| `FailFork` | store deterministic pre-publication failure and release all claims |
| `SetTitle` | enter and leave manual-title mutation authority within one transaction |
| `BeginAutomaticThreadTitle` | claim one empty-title generation and immutable token after canonical user admission |
| `CompleteAutomaticThreadTitle` / `FailAutomaticThreadTitle` | compare exact generation/token and settle one ready or failed automatic-title state; a manual title wins |
| `SettlePendingToolRecovery` | enter and leave settlement mutation authority within one transaction |
| `RecoverInterruptedTurn` | take over exact eligible lease, reread path, finalize exact turn, and release within one transaction |
| `PublishSubAgent` | create one child, or fork-copy its pinned journal/artifact closure, and publish its first pending input atomically |
| `PublishSubAgentInput` | publish/replay one input and apply interrupt cancellations atomically |
| `PublishSubAgentPendingToolCompletion` | settle one exact idle-child pending target and publish one structured pending input atomically |
| `AdmitSubAgentInput` | select one pending input, acquire child turn proof, append turn-start/user message, and mark admitted |
| `PrepareSubAgentClose` | validate exact parent/child and close request, fence one idle descendant subtree plus an optionally locally owned active target turn as `closing` |
| `FinishSubAgentClose` | require no remaining leases, cancel pending inputs, append lifecycle entries, and mark the prepared subtree closed atomically |
| `DeleteRootTree` | rederive exact tree, reject claim/lease, remove queryable state, and write tombstones |
| `ReadArtifact` | in one snapshot validate exact root or complete descendant ancestry, lifecycle, and `(ThreadID, ArtifactID)` ownership, then return reference and text |

Generic append, metadata, leaf, todo, and lease primitives may exist inside a
backend, but production agentharness paths use the semantic owner operation.
Unsupported interfaces fail during composition or `NewHost`, never after work
starts.

## Artifact Read Authority

Floret-owned full tool output is durable Agent output, not a host file or URL.
The public `ArtifactRef` contains an opaque `ArtifactID`, safe label, kind, MIME,
size, and content hash. It never contains or implies an HTTP route, filesystem
path, sibling-store key, or host product identity. A host may project a returned
reference into its own authenticated transport URL without persisting another
artifact-to-thread mapping.

`ArtifactID` is unique only within its canonical `ThreadID`; the durable lookup
key is `(ThreadID, ArtifactID)`. The public operation is exactly:

```text
ReadArtifactRequest { ThreadID, ArtifactID }
ArtifactContent { Ref, Text }
```

Full-output creation is not a generic artifact-store write. For one local tool
invocation, `FinishEffectDispatch` receives the already-captured full text and
output policy, assigns the deterministic thread-scoped `ArtifactID`, writes the
immutable artifact payload, and appends the canonical tool-result entry carrying
its complete `ArtifactRef` in the same proof-bound Memory critical section or
SQLite transaction. Either both become visible or neither does. Replay verifies
the same effect outcome, payload hash, and complete reference and never writes a
second artifact. An artifact row without its exact canonical result entry, or a
result reference without its immutable payload, is authority corruption and is
never readable. Fork claims block new effect dispatch and therefore block this
only production artifact-admission transition.

`ThreadReadHost.ReadArtifact` accepts only its exact bound root.
`SubAgentReadHost.ReadArtifact` accepts one exact requested descendant and
atomically proves the complete current ancestry from that thread to its bound
parent. The bound parent itself, an ancestor, sibling, cousin, or thread under
another tree is not a descendant. A SubAgent read binder may bind an existing
`open`, `closing`, or `closed` parent because reads remain valid across child
closure; `deleted` is never readable. The requested descendant may likewise be
`open`, `closing`, or `closed`.

The Store performs canonical lifecycle/shape validation, ancestry validation,
composite artifact lookup, ownership validation, and reference/content loading
inside one Memory critical section or SQLite read transaction. A concurrent
root-tree delete therefore yields exactly one linear result: the complete read
finishes before deletion, or deletion wins and the read returns a zero-value
result with no content or reference metadata. Physically retained cleanup bytes
after a committed delete are unreachable.

Missing `(ThreadID, ArtifactID)` and an ID that exists only under another thread
both return `ErrArtifactNotFound`, preventing an artifact-existence oracle.
Foreign SubAgent ancestry returns `ErrSubAgentNotFound`; a child passed to a root
read host returns `ErrSubAgentParentRequired`; deleted authority returns
`ErrThreadDeleted`; malformed ancestry or artifact ownership returns
`ErrAuthorityCorrupt`; Store closure returns `ErrStoreClosed`; and a backend
without the atomic operation returns `ErrUnsupportedStoreCapability`. Every
error returns a completely zero `ArtifactContent`. There is no public artifact
listing, write, delete, global-ID lookup, raw Store accessor, or URL resolver.

Error precedence validates the bound authority before artifact identity. A
deleted bound parent/root returns `ErrThreadDeleted`. With a live bound parent,
an absent target or any live/tombstoned target whose complete ancestry does not
reach that parent returns `ErrSubAgentNotFound` without revealing whether the
foreign identity exists. A tombstoned target whose retained ancestry does reach
the bound parent returns `ErrThreadDeleted`. Only after live authority succeeds
does the composite artifact lookup return `ErrArtifactNotFound`.

Fork copying treats artifact payload as part of the pinned canonical source.
For each source/destination pair, the immutable fork plan stores exactly the
deduplicated artifact-reference closure reachable from the pinned entries that
will be copied. Each manifest item pins source and destination thread,
thread-scoped `ArtifactID`, complete canonical reference fingerprint (safe label,
kind, MIME, size, and content hash), payload content hash, and source canonical
result entry. Duplicate references to the same composite identity must be
byte-for-byte identical; orphan artifacts and artifacts referenced only by an
off-path branch are not copied.

`CommitFork` revalidates that exact source closure and copies each immutable
payload to the corresponding destination under the same thread-scoped
`ArtifactID` in the same transaction as journal publication, so copied journal
references need no rewrite. Missing/changed source payload or reference,
inconsistent duplicate reference, extra planned item, or destination identity
collision is authority corruption and publishes no destination or artifact.
Deleting the source later does not affect the destination copy; destination
deletion removes its copy. Completed live replay validates the exact destination
entry/reference/payload closure, while an all-tombstoned completed replay returns
`ErrThreadDeleted` without inspecting deleted payload bytes.

# State Transitions

## Root Create

Owner: exact-ID `ThreadCreateHost`.

```text
absent + unclaimed + unleased
  -> CreateRoot
  -> open + unclaimed + unleased
```

`ThreadID` and `CreateIntentID` are bound before handle delivery. The create
fingerprint contains exactly `ThreadID`, `CreateIntentID`, and the public create
contract version. Title, provider profile, host settings, timestamps, and other
mutable or host-owned data are not create fingerprint fields. Replay returns the
same canonical root only if its stored create fingerprint matches and it is an
open top-level root with no parent, fork source, or fork operation identity.
Reuse of either identity with a different fingerprint is `ErrRequestConflict`.
Existing child, claimed destination, or malformed live root is a conflict. If the
same create-intent record and fingerprint resolve to its matching created-root
tombstone, replay returns `ErrThreadDeleted`. Reusing that `CreateIntentID` for a
different thread/fingerprint, or reusing that created-root `ThreadID` with changed
create provenance, is `ErrRequestConflict`. A new intent targeting any other
tombstoned identity, including one produced by fork or SubAgent publication,
returns `ErrThreadDeleted`. Failure emits no event and creates no cached object.

## Turn Admission, Renewal, And Finish

Owner: exact root-bound `TurnExecutionHost`, or a direct child runtime owned by
an exact parent-bound `SubAgentHost`.

```text
open + unclaimed + unleased
  -> AdmitTurn or AdmitRetry
  -> fresh turn-leased(generation N)
  -> RenewLease and proof-bound provider/tool/journal work
  -> FinishTurn
  -> original lifecycle (`open`, or matching `closing`) + unclaimed + unleased
```

Admission never creates a thread. Turn-start and canonical user input, including
attachments and ordered durable references, are one commit. Text, attachments,
reference order/fields, exact `ThreadID`, `TurnID`, and `RunID` form immutable
admission and replay authority. Provider and tool effects start after admission.
New Memory and SQLite append writes apply the same strict attachment admission
limits; durable reads and exact historical replay retain the schema-v16
compatibility validator so accepted legacy descriptors are never reset or
silently discarded. The replay boundary applies consistently to root turn
admission, SubAgent publication and input, and root or SubAgent pending-tool
completion; only a matched durable request fingerprint may use it, while every
new authority request remains subject to current limits.
Retry target selection is recorded as exact source turn/entry identity and is
part of atomic admission; retry does not copy the source user message. Missing
retry target has zero side effects.

Attachment descriptors include only opaque host resource identity and bounded
display facts. Optional text statistics are host-attested snapshots and are
deep-copied into every authority, retry, fork, cache, and public projection.
Floret never reads attachment bytes. If a `ModelGateway` expands those bytes,
its prepared request is non-durable linear execution state: every successful
prepare terminates exactly once through stream/release or discard, and any
pre-stream authority, budget, compaction, storage, cancellation, or shutdown
failure closes it before returning.
Descriptor-only direct requests with attachments use the complete serialized
`ModelRequest` byte length as a conservative generic-payload upper bound for
projected pressure. Its additive components sum to that full bound so compatible
native anchors preserve attachment growth; this estimate is explicitly not
exact token usage.

Host-provided `SupplementalContext` is normalized before a new admission but is
not part of the durable fingerprint. It is visible only to the current provider
execution, cannot enter cache, raw plan, ledger, summary, checkpoint, or opaque
continuation state, and is ignored by exact running/terminal replay. A
reference-only root admission requires renderable supplemental context for that
current turn; entry points that cannot supply it reject reference-only input.
That turn is not a retry target because no durable provider input can reproduce
the ephemeral material. Retry target selection skips its direct user and
save-point candidates before acquiring authority; public `CanRetry` is false.
Freshly resolving the host resource is a new turn, not replay of the old one.
Public reads return only canonical references from the journal and exact turn
entry index. They never reconstruct references from supplemental context or a
host store.

Only the exact fresh turn proof may write the turn, provider state, approvals,
todos, or active settlement. Automatic title settlement uses its separate
durable generation/token claim and Store-owned worker lifetime. Renewal failure
fences the local turn owner before another irreversible dispatch.

`FinishTurn` never changes canonical lifecycle from `closing` back to `open`.
When the exact turn is being cancelled by a matching close operation, it writes
the cancelled terminal outcome, releases the lease, and leaves the subtree
`closing` for `FinishSubAgentClose`.

Before releasing the lease, `FinishTurn` atomically changes every remaining
`prepared` effect attempt to `cancelled`. It requires every attempt to be one of
`completed`, `failed`, `rejected`, `cancelled`, or `unknown`. A `dispatching`
attempt blocks finish until the local owner commits its captured result through
`FinishEffectDispatch` or explicitly marks it `unknown`; it is never ignored.
Normal provider failure, context cancellation, SubAgent close, and Store close
all use this same terminalization rule. Consequently no terminal unleased turn
retains a replayable `prepared` or ownerless `dispatching` attempt.

## Pending Tool Settlement And Completion

Active settlement owner: the exact active root or direct child turn owner.

```text
fresh turn-leased(generation N)
  -> local proof == durable proof == target turn identity
  -> append exact settlement under generation N
  -> remain fresh turn-leased(generation N)
```

Recovery settlement owner: exact `PendingToolRecoveryHost`.
`SettlePendingToolRecovery` is one transaction that requires an open idle thread,
revalidates the complete target, appends the settlement, and leaves the thread
idle. It never starts a provider.

`CompletePendingTool` is a different provider-backed transition. Its request
contains a durable `CompletionRequestID`, `Target`, `ContinuationTurnID`,
`ContinuationRunID`, and the terminal outcome. Its immutable completion
fingerprint covers all of those fields plus the structured host-authored
continuation payload. `AdmitPendingToolCompletion` atomically persists the
completion request record, validates the exact target, accepts an existing
identical settlement or writes it, acquires the continuation turn proof, and
appends turn-start plus the structured user message that references the target.
A conflicting settlement, changed completion fingerprint, or continuation ID
already owned by another request fails before provider invocation.

Replay of the same `CompletionRequestID` and fingerprint returns the stored
admission result whether the continuation is leased, terminal, or later recovered;
it never starts another provider owner. Reuse with any changed field is
`ErrRequestConflict`. The normal turn renewal and finish contract applies only to
the owner that received the original exact proof.

Root completion is owned by `TurnExecutionHost`. Child completion is owned by
the exact parent-bound `SubAgentHost` through
`PublishPendingToolCompletion`. A child is executed only through explicit
`WaitSubAgents`, so child completion does not start a provider immediately.
`PublishSubAgentPendingToolCompletion` requires the child to be open and idle,
atomically settles the exact target, and publishes one structured pending input
under a durable `InputRequestID`. `WaitSubAgents` later admits that exact input.
This preserves the single SubAgent admission owner while avoiding a
settled-without-continuation crash gap.
For child completion, `InputRequestID` is also the completion request identity;
its fingerprint includes the exact target, outcome, child, and structured input.
Replay returns the same input even after it becomes admitted or cancelled.

## Compaction And Manual Title

Compaction owner: exact `ThreadCompactionHost`. `CompactThread` carries a durable
`RequestID`, which is the `MutationID`. Its immutable request fingerprint covers
`ThreadID`, `RequestID`, source, pinned source leaf entry, pinned active-path hash,
summary schema version, prompt identity, and the canonical compaction request
payload hash. `BeginCompaction` revalidates the pinned path and atomically stores
that request record in `prepared` state plus a fresh mutation lease before
provider invocation.

The owner renews during the provider call. Replay of the same request and
fingerprint returns its stored terminal record, reports a fresh or expired-fenced
owner as busy, or takes over an eligible lease with a new generation and retries
the same immutable request. A changed fingerprint is `ErrRequestConflict`; a
different request cannot recover or release it.

`FinishCompaction` accepts the exact current proof, request fingerprint, and one
terminal outcome fingerprint. Success stores the canonical compaction entry and
completed result; failure stores one typed terminal error. Both terminal shapes
record the finishing owner and generation and release the proof atomically.
Replay with that exact finishing proof and outcome fingerprint returns the stored
terminal record. A changed outcome is `ErrRequestConflict`; an older generation
after takeover is `ErrStaleAuthority`. Terminal failed compaction is not retried;
a new provider attempt requires a new `RequestID`.

The durable compaction states are exactly:

| State | Durable shape | Allowed transition |
| --- | --- | --- |
| `prepared` | immutable request fingerprint plus current compaction lease proof; no terminal outcome | renew, eligible exact takeover, exact finish, or same-request replay |
| `completed` | immutable request, canonical result, outcome fingerprint, and finishing owner/generation; no lease | exact terminal replay only |
| `failed` | immutable request, typed error, outcome fingerprint, and finishing owner/generation; no lease | exact terminal replay only |

Manual title is provider-free. `SetTitle` checks idle, enters mutation authority,
writes the title event/state, and leaves authority in one transaction. It has no
durable half-state. In provider-title mode, canonical user admission immediately
claims `TitleStatus=pending` with a monotonically increasing generation and
immutable token, before main provider completion. The Store-owned title worker
runs concurrently with the main turn and settles only that claim to `ready` or
`failed`; a manual host title invalidates the pending claim and wins every late
completion. Store close cancels and joins title workers before backend close.
On reopen, an orphaned pending generation is deterministically failed rather
than restarted with another provider call. Reference-only title prompts contain
canonical labels only, never display text or opaque resource identity.

## Replayable Root Fork

Owner: exact source-root `ThreadForkHost` and explicit `OperationID`.

```text
all sources idle/unclaimed; all destinations absent/unclaimed
  -> PrepareFork: immutable plan + complete claims
  -> CommitFork: all destinations + completed result + no claims
```

A deterministic conflict before publication may instead transition prepared to
failed through `FailFork`. Transient errors remain prepared. Replays read only
the immutable plan; they do not regenerate IDs, discover new children, rebuild
mappings, recreate missing completed targets, or compensate by deletion.
Completed replay returns the stored result only while every planned destination
is the matching live row. Exact matching fork-provenance tombstones return
`ErrThreadDeleted`; missing or inconsistent live/tombstone authority returns
`ErrAuthorityCorrupt`.
Failed replay returns its stored error even if an unrelated later operation uses
one of the formerly planned destination IDs. Only a row or tombstone carrying the
failed operation's own provenance is corruption.

## Root Tree Delete

Owner: exact root-bound `ThreadDeleteHost`.

```text
open root; descendants open or closed; every identity unclaimed/unleased
  -> DeleteRootTree
  -> deleted tombstones + no queryable Agent state
```

The Store rederives the tree inside the transaction. Active root/child lease,
claim, malformed graph, or concurrent child publication rejects the whole
delete. Replay against the root tombstone succeeds. A never-existing root is not
a successful delete.

If a Floret-managed physical artifact cannot be deleted transactionally, the
canonical transaction removes all references and returns a committed result.
Cleanup failure is a typed committed-cleanup error; replay retries cleanup only
and never repeats canonical deletion.

## Interrupted Turn Recovery

Owner: `InterruptedTurnRecoveryHost` bound either to one root or one exact
parent-child pair.

```text
takeover-eligible turn lease generation N
  -> RecoverInterruptedTurn(expected complete proof)
  -> transaction-local turn_recovery mutation generation N+1
  -> reread active path inside the same transaction
  -> cancel prepared effects and mark dispatching effects unknown
  -> finalize exact unfinished turn and release
  -> original lifecycle (`open`, or matching `closing`) + unclaimed + unleased
```

The pre-takeover path is diagnostic only. Fresh and expired-fenced leases are
busy. Terminal turn, mismatched unfinished turn, missing thread, invalid parent,
or wrong purpose is explicit. Recovery never creates, scans host records,
reconstructs a journal, or infers owner death from a failure marker.
Recovery first changes every remaining `prepared` effect to `cancelled` and
every `dispatching` effect to `unknown`. If any effect outcome is unknown, the
canonical terminal failure is `effect_outcome_unknown`; only a turn with no
unknown effect may settle as generic `interrupted`. The public typed failure,
recoverability, and retry eligibility are Floret facts and cannot be rewritten
by host startup presentation.
Recovery of a closing target preserves the immutable `CloseOperationID`; it
cannot reopen the subtree or finish close.

The exact `InterruptedTurnRecoveryHostFactory` survives retries and contains the
already-bound root or parent-child identity plus the initial `TurnID`, `OwnerID`,
and `Generation`. Each `NewHost` call reads the current complete lease proof. It
returns a proof-bound handle only when those stable fields still match, while
accepting a monotonically newer heartbeat and its timestamps. A lower heartbeat
within the same generation is authority corruption.

`BindThread` or `BindSubAgent` first validates the live exact root or
parent-child authority, then returns `ErrInterruptedTurnNotFound` when that
target has no active turn lease. Composition creates no target and the failure
has zero side effects. Once a factory exists, `NewHost` validates the current
proof as a canonical successor before classifying it. Lease disappearance or a
well-formed strictly higher generation is resolved only after
`ValidateInterruptedTurnResolution` atomically rechecks the current authority
snapshot and the exact target's admission, finish, and terminal ledgers. The
coordinator then permanently completes that target and never follows later
authority. Lease disappearance is canonical only when the durable generation
ledger has not rolled back. A normal owner finish at the same generation and a
completed recovery at the next generation are both validated from their exact
admission proof. A lower generation or heartbeat, malformed finish ledger, or
changed purpose, `TurnID`, or `OwnerID` within the same generation is authority
corruption rather than a resolved target. If root-tree deletion commits between
the preliminary proof read and atomic resolution validation, the tombstone
returns `ErrThreadDeleted`; legitimate deletion is never reported as corruption.

A fresh or expired-fenced matching proof produces a handle whose
`RecoverInterruptedTurn` returns `ErrThreadBusy` with zero mutation. The same
handle may be retried as wall time advances when the proof is unchanged. If
renewal advances heartbeat, the old handle returns `ErrStaleAuthority`; the
coordinator obtains a refreshed handle from the same factory. Neither the
factory nor the handle can bind another identity or stable turn authority.

## SubAgent Publication

Owner: exact parent-bound `SubAgentHost`.

Every spawn carries `PublicationID`, child `ThreadID`, the complete first input
request, and either create mode or fork-copy mode. The immutable publication
fingerprint includes every canonical source and child metadata field defined in
the SubAgent input identity contract. During an active parent turn the operation
uses that exact fresh proof. Outside a turn it enters/leaves a short
`subagent_publish` mutation inside the publication transaction.

Create mode publishes child metadata plus first pending input. Fork-copy mode
copies the pinned parent path and exactly its deduplicated on-path artifact
entry/reference/payload closure, applies child metadata, copies each payload
under the same child-scoped `ArtifactID`, and publishes the first pending input
in the same transaction. The durable `PublicationID` fingerprint includes the
complete pinned path hash and artifact closure fingerprint. Create mode requires
an empty artifact closure.

SubAgent fork-copy is not a replayable root fork and does not use a fork claim,
but it applies the same immutable reference and payload validation as root fork
commit. Source drift, orphan/off-path inclusion, inconsistent duplicate refs,
or child composite collision returns zero child, input, journal, and artifact
publication. Exact replay validates the already-published child journal and
artifact closure and never recopies payloads. A claim on the parent,
closed/deleted parent, wrong parent identity, non-absent child, or proof mismatch
rejects publication. No controller, child, input, or success event exists before
commit.

Spawn and send only publish pending work. They never start provider execution.

## SubAgent Inbox And Admission

Inbox owner: exact parent-bound `SubAgentHost`.

`PublishSubAgentInput` validates parent-child authority and child lifecycle.
It may coexist with a fresh child turn lease, but not with a claim, mutation
lease, close, or delete. It is the only such concurrent write and is not a generic
journal append.

Admission owner: the same exact parent-bound `SubAgentHost`, only through an
explicit `WaitSubAgents` call. Read/list/detail never admit. One host may own
multiple direct child turns, but each local child `Thread` stores only its own
exact proof. Nested children require a different `SubAgentHost` bound to that
child as parent.

`AdmitSubAgentInput` does not accept a caller-selected `SubAgentInputID`. Under
the child authority transaction, the Store selects the pending input with the
lowest durable sequence, using `SubAgentInputID` only as a deterministic tie
breaker, then acquires the child turn generation, appends turn-start and the
canonical user message carrying that exact ID, and marks it admitted. No pending
input returns the internal no-work result with zero lease or journal mutation;
`WaitSubAgents` does not expose it as a public lifecycle error.
Replay of the same child `TurnID`/`RunID` returns the input already admitted to
that turn; reuse of those continuation identities for another input is a request
conflict. Two processes cannot admit the same input. Active child settlement
revalidates the parent-child relation and requires that child's local proof to
equal its durable proof.

## SubAgent Close

Owner: exact parent-bound `SubAgentHost` and explicit `CloseOperationID`.

`CloseSubAgent` closes the target child subtree, not just one node. The request
fingerprint fixes parent, target child, descendants derived at preparation, and
reason. Replay with changed input is a conflict.

`PrepareSubAgentClose` rederives the subtree in one transaction. Every descendant
must be idle, unclaimed, and open/closed. The target child alone may have one
fresh turn lease, and only when the calling `SubAgentHost` supplies the exact
local proof. A foreign lease, an active descendant, an expired-fenced lease, or
a takeover-eligible lease returns busy with zero close or cancellation side
effects. The target itself must be open; a new close identity against an already
closed target returns `ErrSubAgentClosed`. On success, the Store persists the
immutable subtree membership and each node's prepared lifecycle. Every open node
is marked `closing` under the same operation ID; already-closed descendants remain
closed and receive no new lifecycle write. New inbox, admission, spawn, fork, and
delete transitions are then fenced.

If the target turn is active, the host first takes the exclusive side of that
local `Thread` authority gate. This waits for any in-flight authorized local
effect, blocks new provider/tool starts, and revalidates the local/durable proof.
While that exclusive gate is still held, `PrepareSubAgentClose` commits
`closing`. Only after the durable fence exists does the host cancel the execution
context. Thus a local handler either began and returned before prepare, or is
never invoked; it cannot start after prepare commit.

While closing, that exact turn owner may only renew, append its cancelled
terminal outcome, and release. It may not dispatch another provider request or
tool effect. If the owner crashes, the exact parent-child
`InterruptedTurnRecoveryHost` finalizes the takeover-eligible turn; it does not
complete close itself.

`FinishSubAgentClose` requires the same request fingerprint and no remaining
lease. It atomically cancels pending inputs only for nodes prepared from `open`,
appends lifecycle entries for those nodes in postorder, and marks those nodes
closed. Descendants recorded as already closed are verified unchanged and are
not appended to. Crash after preparation leaves a visible, fail-closed `closing`
subtree; replay of the same `CloseOperationID` resumes it, and replay after
completion returns the stored result. There is no reopen or fallback path.
Completed close replay is a read of the immutable close record. It remains
available through an already-bound live-parent host while the closed child is a
pinned fork source; the fork claim blocks new close mutation, not exact replay.

There is no public batch close capability. A host parent-stop coordinator lists
canonical children and records its own progress while invoking exact
`CloseSubAgent` operations. This makes partial progress explicit at the
cross-operation coordinator instead of hiding it inside one broad Floret handle.

Send-versus-close has two outcomes: input commits before prepare and is
explicitly cancelled at finish, or prepare commits first and input is rejected.
Admission-versus-close similarly yields an admitted active turn that must
terminally cancel, or a closing rejection. No closed parent has an open
descendant, no pending input remains behind a closed child, and there is no
post-close cancellation append.

# Mutual Exclusion

## Single Identity

| Current state | Read | Create | Turn admit | Exact proof write | Short mutation | Inbox publish | Root fork prepare/commit | Delete |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| absent, unclaimed | not found | exact create | reject | reject | reject | reject | source reject; destination only through commit | not found |
| absent, fork-claimed | not found | reject | reject | reject | reject | reject | matching prepared operation only | reject |
| open, idle | allow | conflict | allow | reject | allow | allow for child | allow prepare as source | allow if whole tree idle |
| open, fresh turn lease | allow | conflict | reject | exact fresh proof only | reject | allow dedicated child inbox only | reject | reject busy |
| open, expired-fenced turn lease | allow | conflict | reject | reject | recovery only after skew window | exact existing publication/input replay only | reject | reject busy |
| open, takeover-eligible turn lease | allow | conflict | reject | reject | interrupted recovery only | exact existing publication/input replay only | reject | reject busy |
| open, fresh compaction lease | allow | conflict | reject | exact fresh compaction proof only | reject | exact existing publication/input replay only | reject | reject busy |
| open, expired-fenced compaction lease | allow | conflict | reject | reject | reject busy, including same RequestID | exact existing publication/input replay only | reject | reject busy |
| open, takeover-eligible compaction lease | allow | conflict | reject | reject | same RequestID `TakeOverCompaction` only | exact existing publication/input replay only | reject | reject busy |
| open, fork-claimed | allow | conflict | reject | reject | reject | exact existing publication/input replay only | matching prepared operation only | reject busy |
| closing child, idle | allow | conflict | reject | reject | matching close finish only | exact existing publication/input replay only | reject | reject busy |
| closing target child, fresh turn lease | allow | conflict | reject | terminal-cancel/renew only under exact proof | matching close waits | exact existing publication/input replay only | reject | reject busy |
| closing target child, expired-fenced turn lease | allow | conflict | reject | reject | reject busy | exact existing publication/input replay only | reject | reject busy |
| closing target child, takeover-eligible turn lease | allow | conflict | reject | reject | interrupted recovery, then matching close replay | exact existing publication/input replay only | reject | reject busy |
| closed child, unclaimed | allow | conflict | reject | reject | matching completed close replay only; new close rejects closed | exact existing publication/input replay only | allow as pinned terminal source | root-tree delete only |
| closed child, fork-claimed | allow | conflict | reject | reject | exact completed close replay only; new close rejects | exact existing publication/input replay only | matching prepared operation only | reject busy |
| deleted tombstone | deleted error | deleted conflict | reject | reject | reject | reject | reject | root delete replay only |

Provider request start is allowed only for `open + fresh turn lease` under the
local Thread authority gate. Local tool handler dispatch additionally requires
`EffectAuthorizationGate`. Closing and every expired state reject new dispatch.

## Multi-Identity Operations

| Operation | Parent/root requirements | Child/source requirements | Destination requirements |
| --- | --- | --- | --- |
| SubAgent create publication | open parent, unclaimed; idle short mutation or exact fresh parent turn proof | none | absent, unclaimed, no tombstone |
| SubAgent fork-copy publication | same as create publication | pinned parent path plus exact on-path artifact closure in the same transaction | absent, unclaimed, no tombstone; child journal, artifact copies, and first input publish atomically |
| SubAgent inbox publication | open parent, unclaimed | open direct child, unclaimed, no mutation lease; idle or fresh turn lease | not applicable |
| SubAgent pending completion publication | open parent, unclaimed | open direct child, idle, exact pending tool target | one structured pending input plus target settlement in one commit |
| SubAgent admission | open parent, unclaimed | open direct child, idle, exact pending input | not applicable |
| SubAgent close prepare | open parent, unclaimed | target subtree unclaimed; descendants idle; target idle or exact locally owned fresh turn | all open nodes become closing under one operation ID |
| SubAgent close finish | open parent, unclaimed | exact prepared closing subtree, all unleased | all nodes become closed atomically |
| Root fork prepare | open root plus pinned open/closed terminal sources, all unclaimed/unleased | all sources readable and immutable in transaction | all absent/unclaimed/no tombstone |
| Root delete | open root | all descendants open/closed, unclaimed/unleased | tombstones written atomically |

# Failure And Side Effects

Ordering is mandatory:

1. Validate request shape, Store lifetime, and bound identity.
2. Validate lifecycle, parent authority, claim, lease, freshness, and request
   idempotency preconditions.
3. Commit one semantic storage transition.
4. Update local caches from the committed result.
5. Emit observation events.
6. Invoke provider or irreversible tool/file effects only when the committed
   transition authorizes them.

Failures in steps 1 or 2 have zero journal, metadata, provider-state, event,
registry, provider, tool, and file side effects. A semantic operation never
returns an ordinary failure after a partial commit. Observation failure does not
turn a committed canonical result into an uncommitted error. Provider/tool
failure after admission becomes that turn's terminal outcome and is not retried
through another owner.

Public callers branch through `errors.Is`/`errors.As`, never strings:

| Public result | Meaning and retry rule |
| --- | --- |
| `ErrThreadNotFound` | identity never existed; not success and not locally retryable |
| `ErrInterruptedTurnNotFound` | a validated live exact root or parent-child target has no active turn lease; bind creates no recovery target, has zero side effects, and the current recovery scan does not retry it |
| `ErrRecoveryTargetResolved` | an exact interrupted-turn factory's bound lease disappeared or was replaced by a well-formed strictly higher generation; the coordinator completes that target and never follows later authority |
| `ErrThreadDeleted` | tombstoned identity, including exact replay of a completed fork whose destination tree was later deleted; fatal except exact delete replay |
| `ErrSubAgentNotFound` | requested descendant is absent from or foreign to the bound parent; artifact reads return a zero `ArtifactContent` and do not reveal foreign identity existence |
| `ErrThreadBusy` plus `AuthorityBusyError.Kind` | claim, fresh turn, expired-fenced turn, or mutation owner; retry only after observed authority change |
| `ErrSubAgentClosing` | exact close operation owns the subtree; only same close replay or required turn recovery may proceed |
| `ErrSubAgentClosed` | child lifecycle is terminal; fatal for writes |
| `ErrThreadAuthorityInvariant` or `ErrSubAgentParentRequired` | durable authority is malformed, or a root capability targeted a parent-owned child; fatal |
| `ErrStaleAuthority` | local generation/heartbeat is no longer current; owner must stop effects |
| `ErrNoRetryTarget` | canonical retry target absent; request must change |
| `ErrPendingToolNotFound` | exact target identity absent; request must change |
| `ErrPendingToolNotActive` | target exists but is not an active pending result; request must change |
| `ErrPendingToolSettlementConflict` | terminal fingerprint differs; fatal conflict |
| `ErrArtifactNotFound` | requested `(ThreadID, ArtifactID)` is absent, including an ID owned only by another thread; result contains no content or reference metadata |
| `ErrRequestConflict` plus `RequestConflictError` | one create/fork/compaction/publication/input/completion/close identity or canonical effect invocation was reused with a different fingerprint; fatal for that request identity |
| `ErrEffectUnauthorized` | current host policy or approval denies the exact invocation; handler was not called |
| `ErrAuthorizationUnavailable` | current policy read, approval check, audit persistence, or host gate failed; handler was not called; retry after host state changes requires a new canonical tool invocation, never a replacement attempt for the same ToolCallID |
| `ErrInvalidAuthorizationProof` | gate passed a proof that does not match the exact invocation/audit identity; closure rejects it before handler dispatch |
| `ErrEffectDispatchConsumed` | one-shot closure was called twice, retained, or called after `Dispatch` returned; the rejected callback invocation does not call the handler |
| `ErrEffectOutcomeUnknown` | canonical attempt crossed `dispatching` but no committed result exists; handler is never automatically retried |
| `ErrAuthorizationContract` | gate returned success without the closure result or changed/forged its opaque receipt; retry classification depends on whether the local closure crossed dispatch |
| `ErrAuthorityCorrupt` | impossible durable shape, claim set, graph, or operation state; fatal and startup-blocking when discovered during verification |
| `ErrUnsupportedStoreCapability` | backend cannot provide required semantic atomicity; composition failure |
| `UnsupportedStoreSchemaError` via `errors.As` | Store version or fingerprint is not an exact supported schema or migration source; opening is non-destructive and no compatibility data is synthesized |
| `StoreLeasePolicyMismatchError` via `errors.As` | configured lease policy differs from the persisted Store policy; opening is non-destructive and may be retried only with the exact persisted policy |
| `ErrStoreClosed` | Store is closing/closed; no new work may start |
| `CommittedCleanupError` via `errors.As` | canonical delete/close committed; retry cleanup only, never repeat canonical mutation |
| `CommittedEffectError` via `errors.As` | exact authorized handler was invoked; never retry the effect, only record/repair observation state |

`RequestConflictError` exposes only operation kind and request identity; it does
not expose stored payloads or policy data. `AuthorityBusyError` similarly exposes
the busy authority kind without leaking owner secrets.

`ErrAuthorizationContract` before closure invocation is an ordinary no-effect
error. After the local closure crossed `dispatching`, it is wrapped by
`CommittedEffectError`, which is authoritative for no-retry handling. Repeated
public dispatch of the same `EffectAttemptID` returns the canonical state:
`prepared` may resume the same fingerprint, `completed`/`failed` return the
stored result, `rejected` returns its stored public rejection, `cancelled` returns
its stored terminal cancellation, and `dispatching`/`unknown` return
`ErrEffectOutcomeUnknown`.
Supplying a different attempt ID for the same canonical invocation is impossible
through the public contract and rejected by the Store uniqueness constraint in
internal tests.

No path treats missing rows, stale snapshots, unsupported shapes, aliases, or
old versions as success.

# Backend Obligations

## Memory

Memory is the deterministic reference backend. It implements the full semantic
kernel under one process-local mutex and matches SQLite outcomes, liveness rules,
and public error classification. It does not claim cross-process durability.

## SQLite

SQLite is the normative durable and cross-process backend. Lifecycle,
tombstones, fork operation/claims, lease generation/heartbeat/expiry, input
and publication request identity, effect-attempt lifecycle, and semantic transitions are
persisted. Multi-row transitions
use one transaction with an early write lock. Lease policy is persisted and
validated on open. Invalid authority combinations are constrained or explicitly
rejected. Two independently opened Store instances observe one owner and result.

Store instances in one process reserve a single writer slot keyed by the
canonical physical database path before requesting the early SQLite write lock.
That reservation is context-cancellable and is shared by initialization and
normal mutations; read transactions do not enter it. SQLite busy waiting is
disabled: a writer from another process or outside this admission contract
returns the SQLite lock error immediately, without polling, retry, or a hidden
timeout. A failed or cancelled admission publishes no transaction and the next
writer can use the same connection normally. The process-local slot is only a
liveness mechanism; durable authority and cross-process exclusion remain in the
SQLite transaction.

### Schema Compatibility

The current authority-kernel schema is exact version `16`. It persists canonical
turn-entry indexes, path depth, typed turn failure metadata, retry-source
identity, automatic-title generation/token state, and aggregate approval
authority. Exact version `14` migrates to v15 and then v16; exact version `15`
migrates directly to v16. Both migrations validate the complete predecessor
fingerprint and authority data before mutation and run under the Store's one
early write transaction. Failure rolls the entire migration back.

The exact version `13` fingerprint remains supported only when the Store is
empty. Empty means every version `13` data table has zero rows; only schema
metadata and SQLite-internal bookkeeping may exist. Under the same early write
lock, Floret verifies emptiness, replaces v13 with the exact v16 schema, and
persists the configured lease policy. A non-empty v13 Store is rejected because
its missing authority cannot be guessed.

The v14 migration constructs and validates exact canonical turn indexes without
changing journal raw bytes. The v15 migration requires one valid journal root
per thread, computes positive path depth without quadratic ancestry strings,
validates started `(ThreadID, TurnID, RunID)` identities, converts retry source
metadata to exact source turn/entry identity, normalizes typed terminal failure,
and installs title and approval authority. Cross-thread reuse of a `RunID` is
valid; only the exact thread/turn/run tuple is execution authority.
Because v15 did not record failure origin, a valid status-matching explicit code
is preserved, structured durable authority may prove a narrower code, and every
other failure migrates to `legacy_unclassified`. Error text and diagnostic copy
are never classification authority. A code-less legacy aborted marker is
rewritten to failed/`legacy_unclassified` while retaining its old status as
diagnostic metadata, because cancellation versus interruption cannot be proved.

Versions older than `13`, unknown versions, missing metadata, and unknown
fingerprints return `UnsupportedStoreSchemaError` with the observed version and
fingerprint plus the exact current/predecessor shapes. Opening v14, v15, or v16
also requires the configured lease policy to equal the persisted policy. If
concurrent openers race a migration with different policies, the transaction
winner's policy remains authoritative and every loser either opens with that
exact policy or returns `StoreLeasePolicyMismatchError`; no path overwrites the
winner, synthesizes identity, backs up, or repairs the database.

## File

File storage may advertise only capability families for which it provides the
same crash consistency, exact generation/freshness fencing, and atomic semantic
transition. It never substitutes process-local ownership. Until it implements
the full kernel, it is not a valid backend for the public Store capability graph
or cross-process host runtime. Unsupported capability construction fails before
execution.

## Host Authorization Adapter

The host adapter persists current policy and append-only audit in its own
authority store. It provides one authority-thread gate covering policy read,
approval validation, audit persistence, and synchronous callback invocation. It
does not persist Floret effect lifecycle, cannot retain the callback, and cannot
use audit rows as current permission. Adapter contract failures use the public
authorization and committed-effect errors above.

For a locally owned execution, an exact valid terminal `TurnResult` from the
original `RunTurn` call is the authority-release barrier: terminal outcome and
durable lease release are complete. If execution returns without that result,
the host must first confirm the exact terminal turn through the public canonical
read API. A host may signal its own pending processes earlier, but it uses the
provider-free, exact root- or parent-bound pending recovery capability only
after one of those proofs. `ErrThreadBusy` before the barrier is a contract
fact, not a retry loop.

# Required Negative Verification

The implementation plan must map each rule to Memory and SQLite, and to explicit
File rejection where unsupported.

| Area | Required negative proof |
| --- | --- |
| Object graph | Store exposes only `Close`; bootstrap copies/error/panic cannot issue; each binder issues one family; create binds one ThreadID/CreateIntentID; interrupted recovery snapshots one exact root/parent-child plus TurnID/OwnerID/Generation factory before coordinator delivery; downstream services cannot contain binder/raw Store fields |
| Store lifetime | `Close` races with bind/NewHost/run/recovery without post-closing side effects; retained capabilities fail `ErrStoreClosed`; cleanup replay admits no work |
| Construction | missing root, child passed as root, missing parent, deleted identity, and wrong bound ID fail before skills/tools/events/registry/gateway/provider state |
| Root create | same intent/different ThreadID, same created-root ThreadID/different intent, changed contract version, and wrong live shape conflict; matching deleted create replay and new intent against another tombstone return deleted; concurrent exact live replay publishes one root/result |
| State shapes | every invalid lifecycle/claim/lease/input/fork combination is rejected by Memory, SQLite, verification, and migration |
| Schema compatibility | one early-write-locked transaction migrates exact v14 through v15 to v16, exact v15 to v16, or empty exact v13 directly to v16; non-empty v13, older/unknown versions, missing metadata, invalid authority data, and alternate fingerprints roll back without schema or data changes; no identity is synthesized; concurrent different lease policies produce one winner and typed mismatch losers |
| Lease liveness | renewal keeps long turn and approval wait fresh; failed renewal fences dispatch/write; expired-fenced cannot write or take over; only takeover-eligible exact proof can recover |
| Generation | stale/released/wrong-owner/wrong-purpose/wrong-turn/replaced-generation proof cannot write, renew, finish, or release |
| Recovery factory | bind validates live exact authority and returns `ErrInterruptedTurnNotFound` with no target for no lease; exact root and parent-child factories cannot select another identity or later turn; same-target heartbeat renewal makes the old handle stale and a refreshed handle busy/recoverable; lease disappearance or a valid higher generation resolves only after atomic admission/finish/terminal validation; generation/heartbeat rollback, missing or malformed resolution linkage, malformed replacement, or same-generation stable-field drift is corruption; concurrent tombstoning returns deleted; wrong root-child/parent-child relation, Store close, fresh/expired-fenced/stale attempts, and concurrent bind/NewHost/renew/release/replacement/recovery all have zero mutation unless exact takeover commits |
| Admission | exact thread/turn/run start plus user text/attachments/references are atomic and fingerprinted; attachment/reference/supplemental field/count/size/payload/UTF-8 limits fail before journal/leaf/ledger mutation; attachment text-stat pointers are deep-copied; expanded gateway preparation supplies complete conservative estimate plus stable fingerprint and every handle is streamed once or discarded/closed on all early exits; prepared handles never enter durable/cache/replay identity; reference-only requires current-turn supplemental only where that contract exists; supplemental is excluded from replay fingerprint and every durable/cache/ledger/compaction/continuation surface; provider is not called on failure or exact replay; retry source turn/entry is atomic without copied user input; finish plus release is atomic |
| Canonical turn page | Memory/File/SQLite exact started-turn index; bounded tail/before/since reads; stale entry cursor fails without active-path fallback; retry source and typed failure survive reopen/fork; malformed index/raw/path depth fails closed; query count and plan do not grow with references or full journal size |
| Active settlement | complete target identity; local proof equals durable proof; wrong turn or generation has zero append/provider/tool side effect |
| Recovery settlement | fresh/expired-fenced lease blocks; identical concurrent settlement writes once; conflict is explicit; no durable mutation half-state |
| Completion | root `CompletionRequestID` replay/conflict plus target settlement and continuation admission are atomic; child `InputRequestID` replay/conflict plus target settlement and pending input publication are atomic; target/continuation/input IDs are distinct; replay never starts a second provider owner |
| Effect authorization | Store assigns one attempt per canonical invocation across concurrent/different-ID preparation; complete batch preflight precedes every handler; denied/unavailable authorization commits one exact `rejected` result without handler entry; failed rejection persistence leaves ordinary `prepared` so later replay rereads current policy; attempt lifecycle covers crash before/after dispatch boundary; final handler boundary rereads same fresh owner/generation while allowing newer heartbeat; one-shot rejects double/deferred calls; each ordered finalizer gets a fresh detached context, later finalizers still run after an earlier error, and finish/fingerprint/adapter failure marks unknown with a second context; FinishTurn cancels prepared and blocks dispatching; unknown is never auto-retried |
| Approval queue | complete batch creates approval/effect/audit/queue authority atomically; root and descendants share deterministic sequence and one current item; exact generation/revision/current/DecisionID CAS; response-loss replay calls host gate once; decision, proof dispatch, terminal mapping, and promotion are atomic at their defined boundaries; late proof versus cancellation has one winner; restart orphan and gate/proof failures have unique approval/effect terminal mapping |
| Automatic title | canonical admission begins one durable pending generation before main provider completion; title/main provider use causal barriers proving concurrency; manual title wins late settlement; provider failure, timeout, Store close, reopen, delete, and SubAgent close settle or fence the exact generation/token; worker cancellation and join race cleanly; reference-only prompt contains labels but no text/resource identity |
| Interrupted recovery | public exact root/parent-child and TurnID/OwnerID/Generation factory; validated no-lease target returns `ErrInterruptedTurnNotFound` and is not created; same-target renewal refreshes without following a later turn; only canonically finished targets or valid higher-generation replacements resolve, while rollback, missing/drifted admission, malformed finish, or same-generation stable drift is corruption; fresh failure-marked and expired-fenced leases are preserved; stale/resolved targets have zero journal/effect/event/result mutation; takeover validates admission plus started path before mutation and rechecks path in transaction; prepared effects become cancelled, dispatching effects become unknown, unknown outcome takes typed failure precedence over interrupted, and dual SQLite yields one finalization |
| Compaction | immutable pinned-path fingerprint; same RequestID replay only; renewal and expired takeover fence generations; exact terminal owner/outcome replay; changed outcome conflicts; different request cannot recover; one terminal result |
| Fork | prepare pins complete plan/claims plus exact on-path artifact-reference closure; orphan/off-path artifacts excluded; duplicate refs must agree; source reference/payload metadata and hash changes reject; no destination visible while prepared; destination collision or commit failure rolls back every journal/artifact copy; failure publishes none and releases IDs for later root create, root fork, or SubAgent publication; failed-op provenance can never appear; completed all-live replay validates journal/artifact closure and returns stored result; completed all-tombstoned matching provenance returns `ErrThreadDeleted`; every completed mixture or missing/inconsistent authority is corruption, never recreation |
| Delete | active descendant, claim, concurrent spawn, malformed graph, or never-existing root causes zero deletion; tombstone replay; deleted ID cannot recreate; publication/input idempotency ledgers survive for cross-parent request-ID nonreuse, while deleted parent host construction fails before SubAgent operations; cleanup error is committed |
| Artifact admission/read | `FinishEffectDispatch` atomically creates immutable payload plus exact canonical result ref and replays one artifact only; failed append leaves no orphan and dangling/inconsistent rows are corruption; exact root or complete descendant read authority; Store-scoped `(ThreadID, ArtifactID)` collision tests; open/closing/closed child reads; parent/self/ancestor/sibling/cousin/foreign/deleted rejection; missing and wrong-owner both `ErrArtifactNotFound`; every failure has a zero result; read/delete and read/Store-close linearization; Memory/SQLite parity; public refs contain no host URL or path |
| SubAgent publication | create mode requires empty artifact closure; fork-copy pins exact path plus on-path entry/ref/payload closure into PublicationID fingerprint; orphan/off-path and inconsistent duplicate refs reject; source drift or child artifact collision rolls back child/journal/input/artifacts; exact replay validates without recopy; no success event/cache before commit; spawn never executes provider |
| SubAgent input | PublicationID/InputRequestID replay/conflict; exact replay is read-only across every live claim/lease/closing/closed state while new requests reject; ledgers survive delete for cross-parent ID nonreuse but deleted parent construction exposes no operation replay; pending/admitted/cancelled transitions only; interrupt atomic; no consumed state |
| SubAgent admission | only Wait admits; reads never activate; Store selects lowest durable inbox sequence; dual SQLite admits exact input once; no-pending has zero mutation; start/user/admitted state atomic; user carries exact input ID |
| SubAgent close | request identity replay/conflict; exact completed replay remains readable under fork claim while new close rejects; new close against closed target rejects; foreign/descendant lease gives zero prepare/cancel; preclosed descendants remain unchanged; local authority gate orders provider/tool dispatch before closing; FinishTurn/recovery preserve closing; send/admit race outcomes exact; no closed parent with open descendant or pending input |
| Production entry | provider start requires Store authority plus fresh proof/local gate; local handler dispatch additionally requires canonical effect attempt and host authorization gate; settlement/recovery/SubAgent execution has no no-Store path |

# Freeze Gate

The completed design freeze used this gate, which remains mandatory for later
authority-contract changes:

1. Two independent reviewers review this document only and do not inspect or
   patch implementation.
2. Each explicitly reports whether any state, identity, owner, transition,
   mutual exclusion, failure point, backend obligation, or capability is missing.
3. Findings are resolved here and both reviewers re-review the revision.
4. Only after both report no design blocker is the existing diff mapped into
   storage authority kernel, agentharness state machine, and public runtime
   capability phases.
5. Implementation review verifies conformance. A new state or transition reopens
   design review before code changes.

# Related

* [Boundaries](boundaries.md)
* [Runtime Layers](runtime-layers.md)
* [Public API Boundary](../decisions/public-api-boundary.md)
