---
type: Public API
title: Native Thread Updates
description: Incremental actor observations, immutable baselines, and bounded recovery.
resource: /runtime/thread_updates.go
tags: [api, runtime, observation]
timestamp: 2026-10-20T00:00:00Z
---

# Summary

Canonical body metadata resolves exact field hashes and byte lengths without
loading source blocks. Metadata and chunk reads share one active-path locator
check; a locator never grants access or permits an inactive sibling.

The thread actor owns execution state, view versions and publication. Hosts use
optional `ThreadUpdateSource` to observe explicit mutations without copying
history on ordinary growth. Updates are transient observations, never durable
replay or a second lifecycle authority. Lost continuity invalidates the
subscription and requires a new authoritative baseline. Canonical storage,
attempt fences and side-effect settlement remain unchanged.

# Baseline and continuity

Subscribe first, then obtain `ThreadService.View`. Discard updates whose
`ViewVersion` is at or below that snapshot. Each subsequent update must name the
thread and match the current version through `BaseVersion`; apply its entire
batch atomically and advance to `ViewVersion`. A batch may span several actor
versions. Snapshot reads flush pending growth so no batch straddles its baseline.

On a gap, `ErrThreadUpdateOverflow`, `ErrThreadUpdateInvalidated`, or interrupted
transport, close the old subscription, subscribe again and refresh authoritative
views. No cursor, replay, polling or inferred missing mutation is supported. A
canonical presentation failure closes affected workspace observation rather than
silently continuing with a corrupt baseline; explicit reads retry the canonical
projection. Hosts separately refresh their workspace summaries.

# Mutation contract

Item add and replace carry the complete affected item. Remove names its stable
ID. Append carries only new text and an exact UTF-8 byte offset, which must equal
the existing text's UTF-8 length; JavaScript string offsets are not byte offsets.
An addition's ordinal determines insertion order. `ItemOrder`, when present, is
the complete current ordered ID list and also determines one-based ordinals.
Run and turn identities are authoritative actor facts, not host inferences.

A nil component wrapper means unchanged. A present wrapper installs its value,
including nil or empty values that clear the component. Facts are the small
execution, attention, failure, cancellation, progress and queue-count surface
needed by lifecycle consumers independently of display history. Public reads
are detached from actor and other subscriber ownership.

# Publication and compatibility

Ordinary append growth is coalesced at the actor's publication boundary for at
most 50ms or 64KiB. First output, segment changes, structural mutations and other
semantic boundaries flush preceding growth before immediate delivery. Each
subscription has 64 queued batches. Overflow closes it explicitly and never
blocks the provider or overwrites a semantic mutation. Unobserved execution
creates no publication timer or copied display batch.

`View`, legacy `Subscribe` and native updates share the same publication kernel
and version. The v7 legacy interface remains supported; only actual legacy
subscribers request full views. Internal waits use lightweight wakeups and
fresh small facts. There is no alternate diff algorithm or automatic fallback.

Activity live reduction and batch construction use one `ActivityReducer`. Only
affected tool facts are projected during execution; canonical and terminal
boundaries may materialize full state. Engine fragment and actor text buffers
append without repeatedly copying accumulated text, while public snapshots
remain immutable.

# Evidence

- [Native contract and publisher](../../runtime/thread_updates.go)
- [Actor mutation owner](../../runtime/thread_actor.go)
- [Independent public reconstruction test](../../runtime/thread_update_reconstruction_test.go)
- [Continuity, isolation and recovery tests](../../runtime/thread_updates_test.go)
- [Text growth complexity tests](../../runtime/thread_text_growth_test.go)
- [Engine fragment buffers](../../internal/engine/output.go)
- [Activity reducer](../../observation/activity.go)

# Lightweight command results

`ThreadCommandService` is an optional capability on the same service. Create,
Fork, title, Send, Respond, Cancel, Retry and queue operations have `*Ack` return
forms using the existing input types and command implementation. The Ack confirms
successful admission, with the canonical trimmed request key and available
execution identities. It is neither a view baseline nor a durable receipt.
An omitted `ViewVersion` means no actor baseline was observed. A later read failure
must not turn success into a repeated command. Existing complete-view APIs remain
available for consumers explicitly requesting the complete result.

Native observation queues enforce both 64 batches and 1MiB of encoded payload.
Overflow, cancellation, invalidation and Close release pending payloads. A consumer
reestablishes a baseline after overflow; canonical execution does not wait for it.
This queue limit alone does not make full-item replacement or full-view reads
bounded: the bounded-window capability is separate follow-up work.

# Canonical body reads and search

Optional ThreadBodyReader and ThreadBodySearcher read the canonical owner using
fresh threadquery.Scope on each request. Neither a source locator nor a hash
grants access. Hosts also apply private-fact policy to reasoning and arguments.
Reads preserve exact UTF-8 bytes and CRLF, with 16 KiB source and 64 KiB encoded
limits. Search scans no more than 64 KiB per call, supports exact overlapping
matches and carries cross-page candidates through a resumable byte offset.
Consumers pin the returned SHA256 while resuming and continue empty result pages
until HasMore is false. Queries are at most 4 KiB; results at most 100 matches.
Cancelling a read never cancels Agent execution. No display preview is used as
provider context. See [reader](../../runtime/thread_body.go),
[search](../../runtime/thread_body_search.go) and
[differential tests](../../runtime/thread_body_search_test.go).

# Chronological presentation reducer

Full item reads now advance one chronological reducer for control ownership,
Goal state and confirmed review evidence, steering settlement, whole-batch tool
input readiness, approval attachment and hosted-tool completion. Appending a
canonical batch visits that batch and its affected identities; it does not
rescan prior items to attach approvals or finish hosted tools. Pending tool
inputs reserve their source position privately until the waiting boundary and
never escape as actionable controls early. Full reads materialize an explicit
snapshot. This reducer still retains its complete projection and is not yet the
bounded actor/window implementation.

The fixed mixed-history fixture records the original implementation's SHA256
and checks every prefix and chunk size. It covers delayed input visibility,
resolution before a tool appears, shared Unicode source segments, exact Goal
review evidence, steering cancellation and interrupted hosted activity. See
[tests](../../runtime/thread_item_projection_test.go).

# Incremental canonical installation

Canonical provider commits, committed journal observations and interaction requests advance the actor
from its last canonical leaf through bounded exact-entry intervals. Full reads,
turn facts, context facts and actor updates use the same chronological reducers.
Only affected item and turn snapshots are replaced. Delayed tool-input
visibility preserves canonical insertion order and keeps the existing observation
version authority. Failed advancement invalidates its disposable reducer and
requires an explicit canonical reconstruction before accepting more updates.
Reducer references never escape in public views. Canonical snapshots detach mutable
activity and interaction objects before actor installation. Reads and fallible
item, turn, interaction and context checks complete before accepted presentation
changes. A failed later page or late turn validation preserves the accepted view;
explicit recovery rebuilds the invalidated disposable reducer. Precommit approval
and control observations do not manufacture canonical presentation changes.

Historical initialization releases oversized change-map capacity before steady
updates; clearing a large Go map alone leaves iteration proportional to its old
capacity. The actor scaling benchmark checks the complete append-and-install
kernel at 1,096, 10,960 and 109,600 historical turns. It does not measure storage,
observation encoding, browser performance or bounded retained actor memory.
The complete projection remains resident; full refresh boundaries, bounded
windows and downstream production qualification remain outstanding.

Latest request attribution reads the current scope's exact append ordinal from
the canonical prompt records, including pending observations, rather than loading
all request attempts. The memory backend borrows indexed request positions.
Restart, cancellation, deletion and corrupt source identity remain explicit.

See [actor differential and storage recovery tests](../../runtime/thread_canonical_projection_test.go),
[historical turn digest](../../runtime/thread_turn_projection_test.go),
[context differential tests](../../internal/agentharness/context_incremental_test.go),
and [bounded prompt reads](../../internal/storage/prompt_request_reader_test.go).
