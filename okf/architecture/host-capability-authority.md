---
type: Architecture Concept
title: Host Capability Authority
description: Normative authority states, public object graph, storage transitions, and failure invariants for Floret host capabilities.
resource: /runtime/thread_capabilities.go
tags: [architecture, runtime, capability, authority]
timestamp: 2026-07-18T00:00:00Z
---

# Status

This document is the frozen normative authority contract for the next Floret
host boundary. Independent design review approved its durable states, owners,
transitions, public capabilities, and failure points before implementation
resumed. Implementation must map to this document; discovering a new state or
transition reopens design review instead of creating a local workaround.

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
  AuthorizedEffect(EffectAuthorizationProof),
) -> EffectDispatchResult
```

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
`prepared` to `cancelled` before finalizing the turn. `unknown` is not converted
into a pending target and is never used to infer a handler result. A later user
or host action may acknowledge the uncertainty, but it cannot mutate the attempt
into a known result or re-invoke the original handler.

The closure mints an opaque sealed `EffectAuthorizationReceipt` after the
handler result is canonically committed. The host gate can only return the exact
`EffectDispatchResult` produced by the closure. Floret also retains that local
result, so a missing, forged, or changed return value cannot cause effect retry.
If the handler ran but canonical result persistence or adapter return failed,
Floret returns `CommittedEffectError`; the attempt remains `dispatching` until
pure persistence succeeds. The same local turn owner may retry only
`FinishEffectDispatch` with the already captured handler result; it never retries
the handler. If that result cannot be durably completed before turn shutdown,
the owner uses `MarkEffectUnknown` under the exact fresh proof. A process crash
leaves recovery to perform the same conservative transition.

Policy downgrade waits for an in-flight authorized effect to return; a later
effect observes the new policy.

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
| Root read owner | `ThreadReadHost` | one existing root | canonical reads and exact Floret artifact reads only |
| Title coordinator | `ThreadTitleHost` | one existing root | manual title only |
| Fork coordinator | `ThreadForkHost` | one source root | prepare/commit/replay one explicit fork request |
| Delete coordinator | `ThreadDeleteHost` | one existing or tombstoned root | delete or replay that root tree |
| Turn owner | `TurnExecutionHostFactory` or `TurnExecutionHost` | one root | run, retry, pending completion, active settlement, approval read, todo update |
| Compaction owner | `ThreadCompactionHostFactory` or `ThreadCompactionHost` | one root | compact/replay one explicit request |
| Interactive SubAgent owner | `SubAgentHostFactory` or `SubAgentHost` | one parent | publish child/input or child pending completion, explicitly wait/admit direct children, active child settlement, close one child subtree |
| SubAgent read owner | `SubAgentReadHost` | one parent | list direct children; read any current descendant canonical detail or artifact |
| Pending settlement recovery owner | `PendingToolRecoveryHost` | one root or one exact parent | provider-free exact settlement |
| Interrupted turn recovery owner | `InterruptedTurnRecoveryHost` | one root or one exact parent-child pair | finalize one takeover-eligible interrupted turn |
| Host effect authorization owner | `EffectAuthorizationGate` | current host policy resolved for one exact invocation | authorize, audit, and dispatch one supplied effect closure under the host policy gate |

`ThreadCreateHostBinder.Bind(ThreadID, CreateIntentID)` binds before delivery. A
create service cannot choose another root ID or intent in `CreateThread`. Every other binder similarly
binds root, parent, or parent-child identity before a service receives a value.

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
| `InterruptedTurnRecoveryHostBinder` | `NewThreadHost`, `NewSubAgentHost` |
| `TurnExecutionHostFactory` | `NewHost` |
| `ThreadCompactionHostFactory` | `NewHost` |
| `SubAgentHostFactory` | `NewHost` |
| `ThreadCreateHost` | `CreateThread` |
| `ThreadReadHost` | `ReadThread`, `ReadThreadOverview`, `ListThreadTurns`, `ReadLatestThreadTurn`, `ListThreadDetailEvents`, `ReadThreadContext`, `ReadThreadAgentTodos`, `ReadTurnProjection`, `ReadArtifact` |
| `ThreadTitleHost` | `SetThreadTitle` |
| `ThreadForkHost` | `ForkThread` |
| `ThreadDeleteHost` | `DeleteThread` |
| `TurnExecutionHost` | `RunTurn`, `RetryTurn`, `CompletePendingTool`, `SettlePendingTool`, `ListPendingApprovals`, `UpdateThreadAgentTodos` |
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
| `RenewLease` | compare exact fresh proof and advance heartbeat/expiry |
| `AppendWithProof` | compare exact fresh proof before journal, provider-state, approval, todo, or automatic-title write |
| `PrepareEffectAttempt` | Store-assign the single attempt for one canonical invocation, or replay/conflict its exact fingerprint |
| `RejectEffectAttempt` | compare exact fresh turn proof and one `prepared` attempt, then persist one typed terminal rejection or replay/conflict it |
| `BeginEffectDispatch` | final-check one-shot authorization, exact current fresh generation, lifecycle, and `prepared` state; transition to `dispatching` |
| `FinishEffectDispatch` | persist handler result, optional immutable full-output artifact, and canonical tool result as one `completed` or `failed` transition without re-invoking handler |
| `MarkEffectUnknown` | under the exact local turn proof, terminalize a `dispatching` attempt whose result cannot be durably completed, without handler retry |
| `RecoverEffectAttempt` | turn `prepared` into `cancelled` and `dispatching` into `unknown` during interrupted-turn recovery |
| `FinishTurn` | cancel remaining `prepared` effects, reject any `dispatching` effect, require all attempts terminal-safe, append terminal outcome/provider state, release proof, and preserve matching `closing` |
| `AdmitPendingToolCompletion` | validate/settle exact pending target and admit one new host-authored continuation turn |
| `BeginCompaction` | persist request identity and acquire fresh compaction mutation proof |
| `TakeOverCompaction` | compare one takeover-eligible compaction proof and replace it with a durable new generation for the same request |
| `FinishCompaction` | persist exact result/failure and release exact mutation proof |
| `PrepareFork` | pin immutable complete journal/artifact plan and claim every source/destination identity |
| `CommitFork` | publish all destinations and thread-scoped artifact copies, store completed result, and release all claims |
| `FailFork` | store deterministic pre-publication failure and release all claims |
| `SetTitle` | enter and leave manual-title mutation authority within one transaction |
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
attachments, are one commit. Provider and tool effects start after admission.
Retry target move is part of atomic admission. Missing retry target has zero
side effects.

Only the exact fresh turn proof may write the turn, provider state, approvals,
todos, automatic title, or active settlement. Renewal failure fences the local
owner before another irreversible dispatch.

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
durable half-state. Automatic title inside a turn uses the exact turn proof.

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
Recovery of a closing target preserves the immutable `CloseOperationID`; it
cannot reopen the subtree or finish close.

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

### Schema Compatibility

The authority-kernel schema remains version `14` until this unpublished design is
implemented. Version `14` is accepted only with the exact frozen canonical
fingerprint. A different version `14` fingerprint, including an earlier
experimental fingerprint, is not a migration source and is rejected
non-destructively. No version `15` is introduced to preserve an unpublished
shape.

The only supported predecessor is the exact version `13` fingerprint, and it may
migrate only when the Store is empty. Empty means every version `13` data table
has zero rows; only schema metadata and SQLite-internal bookkeeping may exist.
The Store acquires an early SQLite write lock and, in that same transaction,
verifies emptiness before replacing any schema object. A non-empty version `13`
Store is rejected non-destructively because it cannot supply `CreateIntentID`,
request identities, authority generation and expiry, effect attempts, close
identities, or other frozen authority proofs without guessing.

Under that same early-write-locked transaction, an empty version `13` Store is
replaced by the exact version `14` schema and configured lease policy. The
emptiness decision and replacement therefore have one linearization point.
Failure leaves the version `13` schema and fingerprint unchanged. The migration
never synthesizes identities, resets data, renames or backs up a database, or
creates an alternate Store.
Versions older than `13`, unknown versions, missing metadata, and unknown
fingerprints return `UnsupportedStoreSchemaError` with the observed version and
fingerprint plus the exact accepted current and predecessor shapes.

Opening an exact version `14` Store requires the configured lease policy to equal
the persisted policy. A mismatch returns `StoreLeasePolicyMismatchError` before
issuing capabilities or changing durable state. If concurrent openers attempt to
migrate the same empty version `13` Store with different policies, the opener
holding the migration transaction persists one policy; every loser rereads the
resulting version `14` schema and either opens with that exact policy or returns
the same mismatch error. It never overwrites the winner's policy.

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

# Required Negative Verification

The implementation plan must map each rule to Memory and SQLite, and to explicit
File rejection where unsupported.

| Area | Required negative proof |
| --- | --- |
| Object graph | Store exposes only `Close`; bootstrap copies/error/panic cannot issue; each binder issues one family; create binds one ThreadID/CreateIntentID; downstream services cannot contain binder/raw Store fields |
| Store lifetime | `Close` races with bind/NewHost/run/recovery without post-closing side effects; retained capabilities fail `ErrStoreClosed`; cleanup replay admits no work |
| Construction | missing root, child passed as root, missing parent, deleted identity, and wrong bound ID fail before skills/tools/events/registry/gateway/provider state |
| Root create | same intent/different ThreadID, same created-root ThreadID/different intent, changed contract version, and wrong live shape conflict; matching deleted create replay and new intent against another tombstone return deleted; concurrent exact live replay publishes one root/result |
| State shapes | every invalid lifecycle/claim/lease/input/fork combination is rejected by Memory, SQLite, verification, and migration |
| Schema compatibility | one early-write-locked transaction checks empty exact v13 and migrates it to exact frozen v14; concurrent different lease policies produce one winner and typed mismatch losers; non-empty v13, older/unknown versions, missing metadata, and alternate v14 fingerprints are rejected without schema or data changes; no identity is synthesized |
| Lease liveness | renewal keeps long turn and approval wait fresh; failed renewal fences dispatch/write; expired-fenced cannot write or take over; only takeover-eligible exact proof can recover |
| Generation | stale/released/wrong-owner/wrong-purpose/wrong-turn/replaced-generation proof cannot write, renew, finish, or release |
| Admission | start plus user message are atomic; provider is not called on failure; retry move is atomic; finish plus release is atomic |
| Active settlement | complete target identity; local proof equals durable proof; wrong turn or generation has zero append/provider/tool side effect |
| Recovery settlement | fresh/expired-fenced lease blocks; identical concurrent settlement writes once; conflict is explicit; no durable mutation half-state |
| Completion | root `CompletionRequestID` replay/conflict plus target settlement and continuation admission are atomic; child `InputRequestID` replay/conflict plus target settlement and pending input publication are atomic; target/continuation/input IDs are distinct; replay never starts a second provider owner |
| Effect authorization | Store assigns one attempt per canonical invocation across concurrent/different-ID preparation; denied/unavailable authorization commits one exact `rejected` result without handler entry; failed rejection persistence leaves ordinary `prepared` so later replay rereads current policy; attempt lifecycle covers crash before/after dispatch boundary; final handler boundary rereads same fresh owner/generation while allowing newer heartbeat; one-shot rejects double/deferred calls; FinishTurn cancels prepared and blocks dispatching; receipt is Floret-sealed; policy revision changes append a new current decision; unknown is never auto-retried |
| Interrupted recovery | public exact root/parent-child capability; fresh failure-marked lease preserved; takeover rechecks path in transaction; dual SQLite yields one finalization |
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
