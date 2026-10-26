---
type: Public API
title: Native Thread Updates
description: Incremental actor observations, immutable baselines, and bounded recovery.
resource: /runtime/thread_updates.go
tags: [api, runtime, observation]
timestamp: 2026-10-20T00:00:00Z
---

# Summary

Selected activity string sources may carry optional `TextExtent` metadata: the
non-whitespace decoded UTF-8 interval, relative to the unchanged field slice.
Canonical indexing computes it in the source transaction; resident acquisition
uses the same frozen string as the source ID. Older prepared metadata omits it.
Absence is unknown, not whitespace-only, and consumers retain raw source reads.
This enables bounded display normalization where appropriate; it does not
authorize trimming Markdown indentation or silently omitting visible text.

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
most 25ms or 64KiB. First output, segment changes, structural mutations and other
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

Cold SendAck admits through current canonical facts and a one-item tail. The
existing actor retains browsing interests and appends the request-key user row
without replacing accepted history. Complete execution preparation runs before
AgentFactory/provider dispatch; replay and queued acknowledgment do not join it.
Cancellation during preparation settles through a bounded canonical tail. Legacy
Subscribe waits for complete actor promotion before publishing a full snapshot.
Shutdown settles admitted execution cancellation before aborting shared history
preparation, so preparation failure cannot race the canonical terminal claim.
It then joins both execution and preparation before closing the actor.
No alternate actor, durable receipt, schema, or index is introduced. Cold query
continuation reconstruction still reads historical metadata, but uses at most
128 source entries per verified read snapshot and checks the pinned authority
again before mutation, allowing other reads between pages. This is not constant
time admission. See [admission regressions](../../runtime/thread_admission_window_test.go)
and [cold query tests](../../internal/sessiontree/backend_query_residency_test.go).

Native observation queues enforce both 64 batches and 1MiB of encoded payload.
Overflow, cancellation, invalidation and Close release pending payloads. A consumer
reestablishes a baseline after overflow; canonical execution does not wait for it.
This queue limit alone does not make full-item replacement or full-view reads
bounded. Use the released `ThreadWindowSource` for bounded presentation reads
and observation through the same actor and canonical version.

# Canonical body reads and search

Optional `ThreadItemBodyReader.ItemBody` resolves the active-path item association
at the existing actor boundary without loading text or registering window
interest. A page-limited preview can become complete in a one-item window, so
full-text readers must not use preview truncation as source authority. The
returned locator has zero `PreviewBytes`; all subsequent body, manifest and live
reads still require fresh authorization. Missing or unsupported text sources
return `threadquery.ErrRecordUnavailable`. See the
[locator](../../runtime/thread_item_body.go) and
[page-size/restart/live regressions](../../runtime/thread_item_body_test.go).

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

Optional `ThreadFactsSource.SubscribeFacts` uses the same publication owner for
in-process host resource cleanup. It carries identities, actor versions and
runtime facts only; no bodies, historical order or presentation components are
hydrated or retained. Facts observers cannot acquire window interest. Hosts
authorize every fact before exposing it outside the process. Queue overflow
and canonical invalidation retain the existing subscription failure rules.

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

Control output retains its exact raw canonical extent for body reads, including
whitespace-only sources. The display item keeps trim offsets and its bounded
summary; full adapters reconstruct the existing trimmed presentation. A large
raw source still exposes a body locator when its trimmed preview is short.
See [SQLite control regression](../../runtime/thread_text_residency_test.go).

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


# Shared cold preparation

The storage owner performs one frozen metadata scan for context selection and
reuses those validated immutable source locators while materializing body pages.
Each page rechecks active ancestry and verifies requested bodies in its own
transaction. A final fence rejects cancellation, deletion or branch movement
during the last reducer callback. Metadata is temporary and history-sized; bodies
remain page-local. This removes a duplicate scan, not the remaining cold-history
complexity, and introduces no persisted presentation or lifecycle snapshot.

Ordinary local tool activity bodies are omitted from cold source pages and
reconstructed through their exact call/result locators when selected or requested
by a complete adapter. Running calls retain those locators so a later result
merges with the original presentation. Hosted tools, Goal presentation, pending
controls and complete provider-context validation retain their existing reducers.
Selective source pages remain explicitly partial metadata; they do not advertise
complete canonical entries or become executable provider history.

The existing thread actor owns one cold preparation. A caller's cancellation
releases its wait without restarting the source work. The Host admits at most two
active preparations; deletion and shutdown cancel and join both active and queued
preparations. Failure publishes no partial view and a subsequent explicit read may
retry. No worker runs a provider merely to render an idle historical conversation.
Sidebar summaries join this preparation, including when it starts between two
summary pages. Unrelated summary pages yield to foreground projection scans
without holding a transaction. The prepared repository owns that scheduling
boundary; a delegating migration wrapper cannot create a separate priority gate.
Current source-only summaries retain this same priority at their read boundary;
switching from paged history to bounded sources does not bypass a selected read.

The storage projection scan marks foreground read interest in the existing
migration scheduler. It cancels a background attempt at its next transaction
boundary and prevents immediate restart between read pages. It holds neither an
exclusive migration lock nor a storage transaction; explicit preparation and
deletion can still proceed. The final reader releases priority on every return
or panic so background preparation resumes. SetWindow retains the same priority
through authorization, cold preparation and selected body reads; inventory cannot
resume in the gap between projection completion and window publication.

Cold initialization first selects Retry identities using body-free metadata, then
feeds the same complete source pages into the canonical timeline and checked
context reducers. Compaction releases old context bodies. Exact managed-context
request attribution reads a source ordinal; released stores backfill a proven
suffix in transactions of at most sixteen requests. Queue and parent-event
locators store immutable source IDs and journal coverage only. Their sources
remain authoritative, including queue facts on inactive branches.

Prepared built-in stores now initialize SetWindow from exact item membership and
the selected canonical dependency closure at one leaf. Public ranks include
late-visible input reservations. Source groups retain exact ordinal, hash,
category and ancestry; turn boundaries and Retry references remain dependencies
outside the displayed interval. Context references, numeric usage totals, managed
facts, queues and parent events preserve their complete semantics. No transcript
or independently advancing lifecycle is persisted.

The same actor promotes to a complete reducer for execution and explicit full
reads. It constructs and catches up a private candidate before replacing accepted
state. Failure preserves the window and version. Active recovery failures also
close affected observers explicitly and reach the Host error channel. An all-tool
suffix carries the latest nonempty canonical user/assistant source preview at
the same leaf, so a subsequent Summary does not rescan history. Canonical writes
maintain only its source identity, hash and depth in the existing transaction.
Preview text is read on demand through existing bounded text facts; source-only
consumers do not request it. Custom repositories retain their complete reader.

Historical sources without these indexes still need validated preparation.
All readers share that owner, with private short writes and atomic publication
of readiness anchors. Cancellation, owner changes and failed commits cannot
publish a partial window. Missing-index preparation remains history-sized, as
do complete execution reducers and potentially cross-window semantic components.
Prepared cold-process measurements are not first-upgrade or complete product
acceptance.

Ordinary local tool arguments and results outside the exact checked-context
selection are not timeline text and need no cold body read. Their canonical
call/result identities, status, attachments and presentation sources remain
available. Selected context, Goal inputs and control-owned physical results
retain complete bodies for their existing semantic validation.

A canonical page shares fully validated duplicate body bytes within its one
read transaction, retaining at most 1 MiB. Larger bodies are still read and
validated completely without entering this page cache. Later transactions
revalidate source records; the cache never survives a transaction or becomes
canonical authority. Actor admission checks cancellation again after obtaining
the mutex so expired waiters cannot install a stale reading interest.
Within that same page budget, SQLite reads at most 256 exact metadata/block keys
per batch, preserving caller order, missing-record errors and detached bytes.
The normal extent and digest checks still validate every retained body. This is
an internal physical optimization; other storage implementations retain Get
semantics and the public storage SPI does not change.

- [Preparation ownership and cancellation tests](../../runtime/thread_preparation_test.go)
- [Paged Retry parity](../../runtime/thread_hydration_test.go)
- [Independent context oracle](../../internal/sessiontree/context_reducer_test.go)
- [Exact request lookup and rollback](../../internal/storage/prompt_context_request_test.go)
- [Page body budget and transaction isolation](../../internal/sessiontree/body_page_test.go)

## Current Queue Sources

Persistent window and complete hydration resolve pending queue additions and
restored-input source pairs from one rebuildable journal-membership index.
Canonical append maintains it in the same transaction. Consumed queue history
is not replayed on every open. Reordering retains the original admitted addition;
restore references the latest addition, including repeated historical restore
markers. Queue membership spans the journal and is independent of active-path
switches. Missing released metadata is prepared by the canonical storage owner;
publication verifies the source snapshot and cancellation leaves no ready anchor.
The index contains identities and counts only, not queued message text.

## Decoded Activity Fields

API decision: add optional ThreadActivityFieldReader without expanding
ThreadService or changing complete Activity JSON semantics. The identified
consumer is Flower's visible-field loader. A request uses an authorized source
entry and JSON pointer, decoded UTF-8 offset, bounded limit, and source/field
hash fences. Arbitrary JSON queries or executable paths are not accepted.

Canonical append indexes string escape boundaries and typed object/array source
locations in the same transaction. Metadata contains no field text. Each read
validates active ancestry, the source body identity, and decoded block hashes.
An ordinary append does not invalidate an immutable source. Missing legacy
metadata is prepared by the storage owner outside a writer transaction and
published after source verification. Per-field reads alone do not certify the
product's merged/default presentation, disclosure, live, or grouping behavior.

## Effective activity field reads

API decision: optional ThreadActivityStructureReader resolves an authorized
canonical item into its effective typed ActivityPresentation structure. Paths
use JSON pointer syntax, arrays page by child ordinal, and the source digest
pins ordered call/result inputs. ThreadActivityFieldReader returns decoded
UTF-8 ranges from the exact immutable string source. Per-call limits are 64
children and 64 KiB encoded structure, or 16 KiB decoded text and 64 KiB encoded
field data. Prepared structure reads do not materialize string bodies. Legacy
sources have an explicit owner-controlled preparation cost.

Complete typed merge and source-only structure use the same pure
internal/activitymerge field selector. This is a deliberate narrow dependency
from tools on tool-domain value semantics; the selector must not import runtime,
storage, observation, or host packages. The architecture gate enforces that
separation. Runtime alone synthesizes historical default presentations, and the
host does not reconstruct defaults or replay activity sources.

This capability is additive and keeps existing complete activity and window
contracts unchanged. Resident field handoff has focused regression coverage; continuous product
scrolling and end-to-end one-second opening still require product qualification.


### Resident activity field sources

The optional activity structure reader exposes the effective canonical activity
without joining all historical bodies. For an actor-resident activity, collection
pages expose only immediate field shape and lengths. Reading an individual string
path acquires a frozen, opaque source identity for that field. Subsequent field
reads authorize the thread and current item/run association again. The existing
actor owns a bounded ephemeral cache (256 selected strings, 8 MiB resident and 256 MiB temporary spill per thread),
without retaining sibling bodies. Eviction and restart require reacquiring the
selected field; neither changes execution. An accepted string remains immutable
through append, replacement and canonical completion, so chunks cannot mix old
and new content. Resident structure versions describe the requested immediate
shape, independent of unrelated token growth. These APIs do not certify product
latency or complete the continuous-scroll host integration.


### Preparing a growing history

Missing window metadata is prepared at a validated ancestor outside a writer
transaction. Staging keeps every requested readiness anchor private. Ordinary
append preserves that ancestor, so preparation catches up only the new entries
through the existing append reducers, one entry per transaction, then publishes
matching anchors atomically. It does not rescan the prepared prefix for each new
entry. Branch changes or deleted owners reject publication. Interrupted staging
has no readable readiness anchor and is rebuilt by the same canonical owner.
This removes append-triggered restarts, not the one-time historical scan cost.

### Explicit field-only activity windows

The optional ActivityFields on ThreadWindowInput requests an activity summary
instead of presentation bodies. The default remains the published complete/
preview behavior. The same option is retained by the subscription during item
replacement and bounded-window replacement. ThreadItem.ActivitySummary contains
label, description, renderer, action, error presence and effect uncertainty.
For derived Goal activities, Values["/payload/goal"] contains operation, applied,
version and optional outcome from the canonical reducer; text and review
evidence remain deferred fields, including after restart.
Its Fields contains exact root and immediate payload shapes, collection counts,
scalar metadata and canonical source identity selected in the same transaction.
These descriptors never contain collection members or hidden text. A string
shape is not an acquired body source: resolve its exact path with
ActivityStructure before ReadActivityField. Historical shape-only metadata does
not certify full detail readiness, and cannot be used to bypass preparation.
Activity identity, status, severity, attention and approval remain in the
existing activity and interaction facts. The summary is not a product grouping
decision. Hosts fetch actual visible fields through ActivityStructure and
ReadActivityField.

Canonical summary discriminators come from transactionally maintained activity
field metadata. Reading them never materializes hidden rows or error text;
visible labels and descriptions are separately bounded. Resident summaries
inspect typed fields without serializing the full presentation. A running
tool's resident presentation takes precedence over its older canonical call.
Requested resident strings remain readable across canonical completion while
the same authorized item/run exists.

This additive contract supports downstream field renderers. It does not by
itself implement continuous browsing, canonical group boundaries or the product
click-to-interactive performance gate.

### Window reads and concurrent execution

Once a display leaf is accepted, navigation reads another finite interval at
that leaf outside the actor. It does not join complete execution recovery or
replace current facts. Publication rechecks the accepted source owner before
installing the interest. A changed source discards the obsolete interval;
complete promotion still belongs to the same actor. Only a current-facts source
refresh freezes interest installation. The deterministic recovery regression
holds the full-history read open while repeatedly navigating distinct windows.

The first window prepares its exact requested interval, instead of preparing
150 items before applying a smaller limit. The exact latest
canonical preview source accompanies the interval independently of its selected
items. Missing summary source metadata joins the canonical preparation owner;
that initial preparation still counts toward first-open latency. Full-view consumers still promote the same actor. The actor owns independent
bounded-display and complete-execution reads, each using a bounded service
queue. A blocked execution recovery never occupies the display queue. Navigation
at an accepted leaf reads only its interval; a concurrent append refreshes the
same actor and accepted interests before installing that interval. Complete
promotion catches up canonical append-only growth without incrementing an
already accepted version twice. Shutdown and deletion cancel and join both
owned reads; an individual reader still cancels only its own wait.

SetWindow reserves request ownership before cold preparation. It captures
requested item sources under the actor, reads omitted bodies outside the actor,
and then validates selected identities before atomically enqueueing the current
baseline and installing its interest. Replacement, clearing and closing cancel
in-flight reading without cancelling execution. An append-only live source can
reuse an accepted prefix and advertise the current unread extent instead of
restarting on every output append. Failed reads retain the prior interest.
ActivityFields publication captures bounded immutable sources and hydrates them
in Next outside actor, service and subscription locks. The existing queue owns
ordering and charges queued plus in-flight data; caller cancellation retains the
undelivered mutation. Replacing or clearing an interest cancels its body read and
removes obsolete detail without dropping versioned semantic facts. Live source
previews are captured at append time so source completion cannot invalidate a
queued first preview. Legacy complete-presentation subscriptions retain their
published materialization behavior and still require separate qualification.

Canonical window summaries are read in the same storage snapshot as membership
and tool sources. A concurrent result cannot combine a new summary with an old
status. Resident completed presentations retain their small summary before body
eviction; publication never reconstructs hidden payloads to recover it.

For ActivityFields windows, the existing canonical source index also retains
first call, first dispatch and newest result references for ordinary invocations.
It updates these references in the same append transaction and verifies source
hashes and current ancestry on read. Replaced ordinary results need not be
replayed. Control, interaction, ambiguous-run and richer observation groups keep
their original complete reducer dependencies; this is not a claim that every
window dependency is bounded. Default full-presentation consumers retain the
complete source list needed for typed merge behavior.


### Finite item selection decision

ThreadWindowInput.ItemIDs is an additive, opt-in finite selection for hosts whose
visible content is sparse. Its consumer is the continuous reader, which must
observe visible prose on both sides of a folded sequence without subscribing to
its hidden members. Select one to 150 unique, nonempty item identities. Do not
combine it with AnchorID, Direction or Limit. Floret resolves canonical ordering,
rejects unavailable identities, and keeps the prior interest when selection fails.

The same subscription and ViewVersion own baseline and later mutations. Pending
controls remain visible outside the selection; resolved controls remain scoped
to selected items. ItemSelection explicitly marks gaps as intentional. HasBefore
and HasAfter refer to the outermost selected items, not completeness between them.
Selection does not implicitly follow the tail. A path replacement retains only
still-valid selected identities and publishes the replacement on the same queue.

Default interval selection, complete activity presentation, public View and
execution preparation retain their existing behavior. This contract does not
supply product grouping rules or summaries for hidden intervals. Hosts must not
claim exact folded counts or live range coverage from sparse endpoints alone.


## Canonical range read decision

Decoded activity field reads accept optional AlignStart: the canonical owner
may move a split-rune start backward by at most three UTF-8 bytes, reports the
actual start, and retains all source/extent and response bounds. Strict reads
remain the default. This additive decision supports finite reverse-scrolling
caches without retaining unbounded preceding chunk maps. Canonical, in-memory
resident and spilled resident paths share these semantics; source-version mixing
is still forbidden.

The optional public `ThreadItemRangeReader` resolves one maximal same-turn/run
range around an authorized canonical item. Callers specify allowed item kinds and
excluded fixed domain facts; Floret does not accept UI predicates or persist
product groups. The membership owner maintains kind/status counts and safety
facts in the canonical append transaction. Prepared persistent reads use the
existing membership range tree without reading member arrays or hidden bodies.

The result is bound to its canonical `LeafID`. It excludes resident drafts and
has no independently advancing revision. A host must not attach an actor
`ViewVersion` sampled by another read, infer a live subscription, or assume
these facts alone implement its complete presentation rules. Hosted/unknown
activity remains explicitly unknown. Existing `ThreadService` contracts and
complete views are unchanged. Missing historical indexes require canonical
owner preparation; this API makes no first-open latency guarantee.

The focused public regression verifies fresh authorization, invalid selections,
late tool-result count updates at the original position, restart, cancellation,
and that reads never execute an agent. Product grouping parity, live range
observation, and end-to-end performance are separate acceptance requirements.

### Explicit baseline correlation

Public API decision: ThreadWindowInput.RequestID and ThreadWindowSnapshot.RequestID
add opt-in request correlation to the existing ordered observation channel.
A host can keep consuming the accepted interest while SetWindow hydrates its
replacement, then recognize that baseline before forwarding its successors.
Tokens are valid UTF-8 up to 128 bytes. Floret echoes them without assigning
semantic identity or monotonicity; the actor version owns baseline/delta order.
Automatic replacements omit the token. Tokens are not persisted, indexed,
replayed or accepted as authorization. Existing callers omit them unchanged.

### Atomic selected activity values

ThreadActivityValueReader.ReadActivityValue returns one requested subtree from
one canonical read transaction or one immutable resident presentation. The
caller supplies byte (at most 256 KiB) and node (at most 256) budgets. Optional
Fields selects unique direct children of an object before traversal. Missing
children remain absent. Every request authorizes the thread afresh; field paths,
summary versions and response identities never grant access.

When Fields is explicit, TextFields may select up to eight other direct string
children. The response returns their frozen field locators beside Value from
the same canonical transaction or resident presentation. Missing children remain
absent; duplicate, overlapping and non-string selections are invalid. Locator
metadata has a separate 64 KiB encoded bound. Subsequent authorized field-range
reads retain this snapshot even while the activity changes. This supports rich
display rows whose legal code would exceed an atomic JSON budget after escaping,
without reading sibling rows or mixing newer metadata with older text.

The metadata pass bounds traversal and decoded string bytes before string reads;
the encoded JSON limit is checked before returning. Escaping can make encoded
output larger than the decoded budget, so an oversized result fails without
returning a partial value. Strings larger than one value budget use the existing
frozen field-range API. Collections use structure pages and selected members;
selecting a collection never bypasses either budget.

ThreadActivitySummary.Version is an opaque activity-local refresh hint. A live
replacement changes it independently of unrelated thread growth. Canonical
versions bind the selected latest source. Versions may change across live to
canonical handoff without changing visible content. They are not query cursors.
Values carries explicitly selected product-neutral string attributes of at most
2 KiB each and resource/chip collections of at most 16 KiB each, read from canonical
source offsets. Hidden commands, row bodies and tool errors remain field sources.
An absent value does not mean an absent canonical field: Fields retains its
exact kind and extent for separately authorized reads. Copies own their values.

These additions preserve default window behavior and execution context. They do
not certify host renderer coverage or the product one-second acceptance gate.

### Source-relative around seeks

AnchorOffset is an additive ThreadWindowInput field for an around request with
AnchorID. It counts public visible item positions relative to that stable item,
not journal depth, elapsed time, or byte offset. The canonical membership rank
index resolves the destination without scanning hidden intervening history.
Zero preserves existing behavior. Missing/out-of-range destinations fail and
leave the previous observer interest intact; every request reauthorizes scope.

The accepted baseline binds its actual item identities as a finite selection.
Later appends or reservation reveals cannot repeatedly move that selection by
reapplying the numeric offset. Product hosts may use this capability to refill
an evicted reading interval after a scrollbar jump; geometry and scroll policy
remain host concerns. The bounded seek is not evidence of one-second product
opening or complete cross-window grouping.

A window record missing only its preview locator is prepared independently.
The owner searches backward to the latest eligible nonempty message and adds
only the source locator, without rewriting current membership or boundaries.
A long historical tail can still require a metadata scan; this upgrade path
is not a constant-time or subsecond historical first-open guarantee.

## Explicit baseline summaries

`ThreadWindowInput.IncludeSummary` requests a summary on that explicit
`ThreadWindowSnapshot` only. Activity state, preview and the window version
share actor ownership. Hosts use this summary as the baseline read watermark
instead of preparing a one-item window and independently sampling `Summary`.
Automatic replacements do not repeat it. Thread identity, title and creation
metadata are obtained before freezing the actor snapshot; later title changes
remain ordered by the existing title generation. No durable state is added.


## Bidirectional Parent Receipts

The public API decision extends the existing parent-event input/page types,
rather than introducing another durable history or observer. Optional Latest
and BeforeEventID select earlier pages; results remain in canonical order.
PreviousBeforeEventID identifies the exclusive cursor before the first returned
receipt. AfterEventID cannot combine with backward selection. Fixed interval
reads retain their original first/last bounds; ListParentEvents may discover
new receipts without rebuilding the entire history. Existing defaults remain
forward reads. Hosts reauthorize each request and use ReadParentEvents with one
EventID only when the report becomes visible. These options supply bounded
navigation, not product-specific grouping or a click-to-interactive guarantee.


ListParentEventsInput.AfterOrdinal and BeforeOrdinal optionally seek an
exclusive interval on the current canonical path. Zero omits a bound. A before
bound selects its latest receipts; otherwise selection starts at the after bound.
Both directions return chronological events. These fields exclude event-relative
cursors and Latest; nonzero bounds must increase. Returned event cursors and
first/last identities describe the full path, so subsequent scrolling can leave
the initial ordinal interval. Positions are canonical entry ordinals, not item
ranks or timestamps; callers must reselect after a path change. Exact report
reads retain the existing event-ID and fixed-range contracts. An optional LeafID
selects an ancestor still on the current path; append preserves that selection,
while removal from the active path fails with ErrJournalInvariant. Every page
returns its actual LeafID. Interaction resolution is current at the read
transaction and does not claim to be the historical resolution at that leaf.

## Host-selected preview and presentation ranges

ThreadWindowInput.PreviewBytes bounds per-item source reads before hydration;
zero preserves the published default. The optional ThreadItemTextReader resolves
one active item and returns exact presentation text, including normalized control
output, with 16 KiB decoded and 64 KiB encoded limits. Every request freshly
authorizes scope and validates the current source association. A frozen live
extent cannot grow while being read. Canonical installation requires refreshing
a live locator. Recent source manifest pages occupy at most 32 in-memory pages
of 150 locators each, bound to the frozen leaf; there is no durable text cache.
