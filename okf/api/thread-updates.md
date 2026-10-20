---
type: Public API
title: Native Thread Updates
description: Incremental actor observations, immutable baselines, and bounded recovery.
resource: /runtime/thread_updates.go
tags: [api, runtime, observation]
timestamp: 2026-10-20T00:00:00Z
---

# Summary

Execution completion reads the appended canonical interval outside the actor
mutex. The accepted view remains active and readable until installation; a
changed reducer or starting leaf requires a fresh interval. The shared item,
turn, context and SessionLifecycle reducers remain authoritative. Terminal
installation discards stream-only drafts and replaces observed tool overlays
with exact canonical facts, without republishing untouched historical items.
Terminal presentation presence is updated only for affected canonical items.
Complete recovery reads and custom stores lacking exact source readers still
use the complete adapter. This does not complete bounded cold initialization,
metadata residency, streamed canonical installation or downstream performance qualification.

The real SQLite fixed-completion benchmark (Apple M5 Pro, Go 1.27.2, 200
iterations) measured 2,104 / 2,315 / 2,447 ns per installation with 1,096 /
10,960 / 109,600 synthetic historical items, respectively, and 996 bytes / 20
allocations at every size. The final journal commit is already accepted for
this measurement; it does not time fixture creation, cold loading, model-context
changes, browser delivery or UI rendering. Reproduce with:

    GOWORK=off go test ./runtime -run '^$' -bench '^BenchmarkTerminalCanonicalSQLiteFixedBoundary$' -benchtime=200x -count=1

Large initialization batches use one transient ordered mutation index rather
than repeated linear deduplication. Ordinary turn-kind validation uses existing
marker positions; typed-turn admission still checks all historical members.

Ordinary send and queued promotion install appended canonical intervals. Actor
turn membership borrows reducer-owned append-only slices; public snapshots still
copy them, so growing one long turn does not copy its unchanged membership.

Repeated output-source admission compares canonical turn/run/channel identity
and exact reasoning hashes without reading old body blocks. Inactive sibling
locators do not contribute source authority to the current path.

Optional ThreadBodyManifestReader resolves canonical output IDs to at most 150
source parts and 64 KiB per page, with a 256-locator scan budget. Empty pages
advance across inactive siblings. A frozen active leaf preserves append validity;
body reads still authorize each field, and reasoning repeats only once. This is
source assembly for explicit reading, not a second view or provider projection.
Control output is an exact separate body field; manifests preserve its raw bytes,
while the established presentation owns whitespace trimming and the empty label.

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

Only bounded window subscriptions emit `ThreadItemBodyUpdated` (`body`). It
replaces the item's `Body` locator and exact frozen extent, preserving the
already accepted preview and every other item field. It carries neither `Item`
nor `Append`. Once a preview is truncated, later growth therefore performs no
prefix read or retransmission. Complete `ThreadUpdateSource` consumers retain
the existing add/replace/remove/append contract. A new window baseline refreshes
the bounded prefix explicitly; canonical installation uses an ordinary replace.

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
boundaries may materialize full state. The Engine and actor share one
attempt-owned source. Actor reads freeze the exact byte extent; public complete
snapshots remain immutable.

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
Interaction admission and resolution use actor-owned position and item indexes,
with attention counts updated by the changed control. Explicit full replacement
rebuilds those disposable indexes. Fixed-history benchmarks include append and
resolution; they do not certify full observation component encoding.
Ordinary accepted text is released from the actor and reducers after canonical
installation, including terminal refresh. Frozen source extents preserve explicit
complete reads; bounded windows read only exact prefixes. Complete legacy queues
freeze text before enqueueing, so subsequent deletion cannot break an accepted
snapshot. Byte/rune facts and bounded summaries preserve append offsets, citation
positions, whitespace and terminal validation. Transformed control labels remain
resident; historical metadata, full refresh boundaries, bounded initialization
and downstream production qualification remain outstanding. See
[text residency tests](../../runtime/thread_text_residency_test.go).

Latest request attribution reads the current scope's exact append ordinal from
the canonical prompt records, including pending observations, rather than loading
all request attempts. The memory backend borrows indexed request positions.
Restart, cancellation, deletion and corrupt source identity remain explicit.

See [actor differential and storage recovery tests](../../runtime/thread_canonical_projection_test.go),
[historical turn digest](../../runtime/thread_turn_projection_test.go),
[context differential tests](../../internal/agentharness/context_incremental_test.go),
and [bounded prompt reads](../../internal/storage/prompt_request_reader_test.go).


# Scoped item windows

Optional ThreadWindowSource registers authorized workspace observation through
one ThreadUpdateSubscription. SetWindow freshly authorizes each page and queues
an exact actor baseline before installing interest. Baselines replace Window,
selected Turns and applicable Interactions at the same ViewVersion authority;
accept explicit baselines even if their version is already known. Normal deltas
continue by BaseVersion. Thread and canonical leaf identity remain explicit.

Pages contain at most 150 items and 512 KiB of encoded item data. UTF-8 text
prefixes are at most 64 KiB and detached before cloning private source locators.
ThreadItem.Body identifies omitted text; partial Markdown is raw preview. Active
text uses a frozen ReadLiveBody locator until commit. Turn membership tests selected IDs
against canonical membership rather than scanning an enormous turn. Pending
interaction IDs belong to the existing interaction index, so controls remain
reachable independently of historical page selection.

Append preserves stable completed-history anchors. Reordering and tail interest
beyond three pages install bounded geometry in place. Missing accepted anchors
report AnchorUnavailable and the available tail. Failed explicit target reads
preserve the previous interest. ClearWindow releases queued items and turns;
queue reads, clears and writes share the subscription lock. Waiters release the
lock before waiting, preventing concurrent receive/clear deadlocks. Scope grants
no persistent access; hosts close the stream on permission revocation.

This is an observation substrate, not completed extreme-history qualification.
Cold actor hydration visits frozen canonical pages of at most 256 entries and
releases reconstructible text and completed ordinary activity between pages.
Retry, managed context, compaction, steering, control and interaction dependencies
retain complete canonical context validation. Queue and restored-input recovery
share one frozen body-free physical inventory, including inactive branches.
Request-key hydration reads metadata for the same last 256 canonical records.
No partial projection is installed after cancellation, a failed page or a
changed active path. [Differential and public-window regressions](../../runtime/thread_hydration_test.go)
compare exact full results and exercise append, cancellation and branch changes.
Cold hydration still visits all metadata and retained reducers keep its indexes;
this is not bounded initialization or bounded actor metadata.
Queue, context and restored-input components retain complete semantics and can
exceed the observation budget. Oversized components fail explicitly, never drop
accepted controls. Bounded actor residency, large structured metadata access,
streamed canonical installation and downstream production acceptance remain unfinished.

# Active output residency

The Engine attempt owns raw draft bytes and any path-redacted display tail under
one aggregate 4 MiB source-memory budget. The display reader shares the unchanged
prefix; only a transformed suffix is stored separately. Larger bytes spill to
private disposable files. Failed writes do not advance the accepted extent, and
every exit closes spill files. This is temporary source storage, not a journal,
replay mechanism or second execution owner. Diagnostic sanitizers and recorders
drop internal body references. The actor keeps an exact extent and bounded summary.

Optional ThreadLiveBodyReader freshly authorizes private facts per chunk and
pins ThreadID, RunID, SourceID and ThroughBytes. It returns at most 16 KiB source
and 64 KiB encoded, preserving UTF-8 and CRLF. Late append cannot extend a frozen
prefix. Retry, canonical installation, deletion and restart expire the locator;
the caller refreshes the item and uses its canonical body locator. Canceling the
read never cancels the run. Full adapters detach the exact frozen live extent.

Optional ThreadLiveBodySearcher searches that frozen display prefix through the
same authorized reader. Pages scan at most 64 KiB source, preserve UTF-8 and CRLF,
and return at most 100 overlapping exact matches. The caller resumes at
NextOffsetBytes without changing ThroughBytes. Live source expiry stays explicit;
search never silently switches to canonical bytes. Canonical and live searches
share one bounded kernel, including cross-page overlap and cancellation.

Completion still materializes complete output for the existing transactional
provider-context commit. This peak is outside the draft budget and remains a
separate implementation requirement. SQLite tests cover 44 MiB output, bounded
live previews, exact chunk/commit/restart hashes, graceful stop, frozen adapters,
revocation and cancellation. They do not establish UI or production-chain
performance acceptance. See [live body tests](../../runtime/thread_live_body_test.go)
and [spill failure and cleanup tests](../../internal/outputbody/body_test.go).

See [window contract](../../runtime/thread_window_observation.go),
[SQLite, restart, authorization and race regressions](../../runtime/thread_window_observation_test.go),
and [window geometry selector](../../runtime/thread_item_window.go).
