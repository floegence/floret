---
type: Maintainer Workflow
title: History Performance Qualification
description: Reproduce fixed-change SQLite scaling and distinguish upstream foundations from complete product acceptance.
resource: /internal/sessiontree/history_performance_test.go
tags: [workflow, performance, history]
timestamp: 2026-10-20T00:00:00Z
---

# Fixed-history SQLite mutation

## Send request identity lookup

    GOWORK=off go test ./internal/sessiontree -run TestTurnRequest -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench '^BenchmarkSQLiteFixedHistoryFindTurnRequest$' -benchtime=100x -count=1

The same SQLite fixture measures a missing request key, the ordinary new-send
path. The canonical control index retains started-entry ordinals only, rebuilds
from the journal, and participates in the existing transaction rollback owner.
It is not another durable command ledger. Randomized tests compare append,
rollback and rebuild results with the original reverse-scan oracle; corrupt
locators fail without a fallback scan.

On the same machine and toolchain, one diagnostic comparison measured:

| Segments | Before, ns/op (30x) | After, ns/op (100x) | After, bytes/op | After, allocations/op |
| ---: | ---: | ---: | ---: | ---: |
| 1,096 | 31,038 | 2,092 | 2,192 | 18 |
| 10,960 | 225,139 | 2,068 | 2,192 | 18 |
| 109,600 | 2,237,882 | 2,813 | 2,192 | 18 |

This qualifies one request lookup only, not complete send admission or Flower
performance. Canonical metadata and actor residency still need separate work.

## Append mutation

Run with the toolchain pinned by go.mod and workspace discovery disabled:

    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkSQLiteFixedHistoryAppend -benchtime=20x -count=1

The deterministic fixture has one running turn and independent assistant output
sources. Counts are 1,096, 10,960 and 109,600 immutable historical segments. It
uses the actual SQLite backend and native exact-document capability. Each sample
prepares and persists one new segment, then deliberately rolls back at the
backend commit boundary so history size stays fixed. It includes SQL writes and
rollback, not a committed WAL flush or provider request.

On an Apple M5 Pro, Go 1.27.2, October 20, 2026, one diagnostic run measured:

| Segments | Time/op | Allocated bytes/op | Allocations/op |
| ---: | ---: | ---: | ---: |
| 1,096 | 199,433 ns | 50,544 | 353 |
| 10,960 | 217,500 ns | 50,979 | 354 |
| 109,600 | 185,556 ns | 50,642 | 353 |

Before source ancestry indexing, the first two sizes measured 328,258 ns and
1,179,744 ns; the 100x setup was stopped after identifying its quadratic ancestor
walk. Do not interpret the interrupted run as a complete baseline measurement.
The index replaces that walk with branch-aware canonical source locators and
ancestry queries; differential tests preserve conflicting-source rejection.

These results qualify one mutation path only. They do not qualify full provider
context preparation, historical body residency, giant merged output indexing,
window baselines, product transport, Markdown, browser latency or memory plateaus.
Complete Flower acceptance requires actual published upstream consumption and the
production-stack matrix described by the implementation task. Prototype browser
numbers must not be substituted for that release gate.

## Provider commit control locators

    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkSQLiteFixedHistoryProviderCommit -benchtime=20x -count=1

This uses the same fixed SQLite history and deliberate transaction rollback.
It includes provider-context commit admission and SQL writes, not a successful
WAL flush or a complete provider request. Canonical control entry locators replace
the three complete-history scans for active turn, run identity and cancellation.
Only control positions and the active set are indexed; startup rebuilds them
from canonical entries. A randomized differential test compares them with the
original migration/validation scans through append and rollback.

One diagnostic run on the same machine and toolchain measured:

| Segments | Before, ns/op | After, ns/op | After, bytes/op | After, allocations/op |
| ---: | ---: | ---: | ---: | ---: |
| 1,096 | 259,885 | 401,358 | 50,408 | 352 |
| 10,960 | 856,802 | 323,929 | 50,404 | 351 |
| 109,600 | 4,781,348 | 178,877 | 50,284 | 353 |

These short runs have timing/cache noise; they demonstrate removal of the
history-scan trend, not a stable total speedup or production acceptance.

## Large-field initialization diagnostics

    GOWORK=off go test ./runtime -run TestCanonicalSharedContent -bench BenchmarkCanonicalSharedContentProjection -benchtime=1x -benchmem

Full-text initialization has been removed. There is no term collector or index
benchmark to optimize. Metadata preparation must not read historical body bytes;
explicit text queries pay the cost of their requested canonical content.

The diagnostic projects 10,960 canonical segments with one shared source
into the explicitly requested complete view. One append buffer and a streaming
character count preserve citation offsets without copying or recounting the
growing prefix. A single sample changed from 3.97 s / 7,735,860,400 allocated
bytes to 4.76 ms / 12,313,792 bytes. Full materialization remains proportional to
the requested result and is not a bounded-window implementation.

Startup now decodes one storage page at a time instead of retaining the complete
encoded namespace beside decoded state. Cancellation, later-page corruption,
non-advancing cursors and borrowed page buffers have direct regressions. The decoded canonical repository now retains immutable body locators after
validation. Complete reads hydrate only requested entries into detached results.
The process-wide hash-verified source cache is bounded to 128 MiB. This source
byte budget does not measure Go heap or RSS; runtime actor views and provider
context preparation still require separate qualification.

## Cold summary source reads

    GOWORK=off go test ./runtime -run 'TestThreadSummary(Streaming|Cold|Preview|Control|ToolInput)' -count=1

Built-in cold summary reads use the canonical body-free window reader through a
frozen leaf. A chronological reducer shares lifecycle, queue, attention and
interaction semantics with the explicit complete adapter. Tool input results
reserve their source order but remain invisible and unvalidated until the
canonical waiting boundary. Only the newest nonempty message preview and
required failure/input fields read body chunks. Late appends do not extend the
snapshot, empty history stays empty, and path changes, cancellation or later
page failures return no partial summary. The regression suite freezes the old
complete algorithm as an independent oracle and checks each fixture prefix,
control ordering, queue transitions, UTF-8 and trim boundaries.

This removes full historical body hydration, not all cold-summary scaling.
Metadata is still scanned, lifecycle and interaction maps grow with history,
and required control/error semantics may themselves be large. It does not
qualify bounded cold initialization or the production Flower stack.

## Aggregate window hydration

    GOWORK=off go test ./runtime -run 'TestThreadWindow(Hydrates|Escaped|Around|Preview)' -count=1

The bounded window divides one aggregate source allowance before hydrating
text or completed activity, accounting for worst-case JSON escaping and UTF-8
read slack. Each item still exposes its exact complete extent and source
locator; oversized typed activity stays an explicit complete-body reference.
The encoded page check remains authoritative for metadata. Synthetic 40 MiB
live extents test 1, 5 and 150 selected items, count actual source reads, and
verify exact mixed UTF-8 prefixes, owner preservation and escaped JSON bounds.
This bounds preview source hydration, not arbitrary inline control metadata,
actor metadata residency or complete production-stack latency.

## Current-schema startup residency

    GOWORK=off go test ./internal/sessiontree -run 'TestCurrentStartup(Reuses|Repairs|Preserves|Recovers)' -count=1 -v
    GOWORK=off go test ./runtime -run TestOpenCurrentStoreCompletesWithoutHistoricalBodyReads -count=1 -v

Normal current-store startup now checks durable readiness anchors. In the
compacted root plus child regression, 16 and 512 tail messages both require
20 record gets, one child metadata row, no body reads and no writes. Before
the change, those fixtures required 658/9,642 gets and 88/2,072 body reads.
A complete SQLite Host restart, including its background worker, reads 16
records (about 4.2 KiB) for both 8 and 128 roughly 52 KiB messages. These are
source-access regression budgets, not a production wall-clock or RSS claim.

Missing/stale indexes and migration still require complete validation. Parent
event recovery still scans child metadata; publishing a missing receipt checks
the complete source closure. Explicit inspection and selected-thread preparation
remain proportional to the selected history, including compaction checks. No
whole-store constant-time or bounded active-actor claim follows from this change.

The following older benchmark exercises the complete inspection path:

    GOWORK=off go test ./internal/sessiontree -run 'Test(CurrentStartup|StartupMetadataQuery|Historical|CanonicalBodies|Inspection|PersistentQuery)' -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkCurrentStartupCanonicalLoad -benchtime=1x -benchmem

Full domain inspection verifies each complete immutable entry before retaining
only canonical field locators. Metadata-only Goal and parent-event readers share
the exact ancestry traversal with complete reads. Reasoning source comparisons
use verified source hashes. Query correspondence shares the canonical projector
with a metadata mode, and only an index rebuild loads explicit document bodies.
The differential checks complete serialized facts, source order, segmented
assistant messages, steering, hosted tools, cancellation and unchanged storage.

The benchmark uses actual SQLite, separate 16 and 160-entry stores, and roughly
64 KiB mixed UTF-8 bodies. One sample measured 43.9 ms / 85,010,192 allocated
bytes for 16 entries and 362.5 ms / 806,965,616 allocated bytes for 160 entries.
These are total allocations, not retained heap or RSS. It measures complete domain validation, including
source reads; it is not a constant-time startup test. Every source is still
validated. Metadata, migration inputs, complete checkpoint validation and the verified
source cache have separate residency costs. Production heap/RSS and complete Flower qualification remain required.

## Metadata-only query storage

    GOWORK=off go test ./internal/sessiontree -run 'Test(QueryMetadataUpgrade|MetadataBrowse|MergedQuery|ThreadQueryUnicode)' -count=1
    GOWORK=off go test ./storage -run 'Test(SQLiteHasNoFullTextIndex|PhysicalSearchRemoval)' -count=1

These tests assert that startup removes released SQLite/KV text indexes
atomically, metadata preparation reads no historical bodies, current reopen
preserves canonical bytes, and metadata pages retain only frozen locators.
Unicode matching remains an explicit on-demand query operation. No background
indexing, complete-source token arrays or alternate search backend is allowed.

## Merged-source append diagnostic

    GOWORK=off go test ./internal/sessiontree -run 'TestMergedQuery' -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkSQLiteMergedHistoryAppend -benchtime=10x -benchmem

A real SQLite write callback, including source-locator metadata, rolls back after work
to keep the same measured history. The following measurements predate full-text removal and are retained as historical evidence, not current acceptance results. Initial single-sample results on Apple M5 Pro:

| Segments | Before ms | Before allocated bytes | After ms | After allocated bytes |
| ---: | ---: | ---: | ---: | ---: |
| 1,096 | 4.09 | 7,416,520 | 0.69 | 88,152 |
| 10,960 | 36.03 | 73,599,176 | 0.47 | 97,600 |
| 109,600 | 352.43 | 740,875,728 | 1.62 | 95,376 |

A subsequent ten-iteration run of the completed change measured:

| Segments | Time/op | Allocated bytes/op | Allocations/op |
| ---: | ---: | ---: | ---: |
| 1,096 | 313,629 ns | 76,839 | 655 |
| 10,960 | 506,112 ns | 76,860 | 656 |
| 109,600 | 236,767 ns | 76,799 | 655 |

These short latency samples remain noisy. The fixed-change allocation trend is
bounded; they do not certify production CPU or end-to-end latency. Source reads
and metadata no longer grow with the merged prefix. Correctness covers old and appended content,
cross-segment Unicode and CRLF, NUL, restart, transactional rollback, batch
append, fixed metadata size and bounded source reads. Physical migration covers
format 3 source records, rollback, cancellation, panic, restart idempotency and
unchanged opaque bytes. Production Flower latency, heap/RSS and qualification
remain outstanding.

## Retained merged-item installation

    GOWORK=off go test ./runtime -run 'TestRetainedActorDropsPrivateSourceMembership|TestThreadCanonicalTextResidency' -bench BenchmarkRetainedActorMergedAppend -benchtime=10x -benchmem

One displayed assistant item merges 1,096, 10,960 or 109,600 canonical sources.
This isolates runtime reducer/actor installation, without storage or transport.
Before removing duplicate retained source lists, ten-iteration samples allocated
19,372 / 181,164 / 1,767,680 bytes per append. Afterward all sizes allocated
940 bytes in five allocations; timing samples were 1.26 / 1.45 / 1.89 microseconds.
Exact full-source adapters, final turn references, citations, restart and nested
Fork behavior remain covered separately. Retained canonical metadata still grows
with history; these results are not bounded initialization or product acceptance.

## Exact admission and terminal locators

    GOWORK=off go test ./internal/sessiontree -run TestRuntimeAdmissionRejectsCorruptExactLocators -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench '^BenchmarkSQLiteFixedHistoryReadAcceptedTurn$' -benchtime=100x -count=1

Admission replay, accepted-turn reads, finish/cancel replay, unknown-effect
settlement and context-only admission checks share the existing exact canonical
entry index. A metadata lookup validates position, entry/thread identity and
path depth before accessing a record. It does not create another cache or load
unrelated bodies. Complete public reads retain their existing materialization
and integrity validation. Corrupt locators fail closed rather than scanning the
journal for a substitute.

On the same machine and toolchain, the SQLite read diagnostic measured:

| Segments | Before, ns/op | After, ns/op | After, bytes/op | After, allocations/op |
| ---: | ---: | ---: | ---: | ---: |
| 1,096 | 56,325 | 12,975 | 19,174 | 59 |
| 10,960 | 461,254 | 15,467 | 19,168 | 59 |
| 109,600 | 4,833,540 | 10,344 | 19,168 | 59 |

The baseline used 30 reads per size and the indexed run used 100. This removes
one measured linear scan; short-run timings are diagnostic. Other lifecycle
validation, request-key lookup, actor metadata residency and production Flower
qualification remain separate outstanding work.

## Turn terminal settlement

    GOWORK=off go test ./internal/sessiontree -run 'TestFinishTurnWithoutGoalGuardDoesNotReadHistoricalBodies|TestGoalReviewInvalidationAndTerminalCommit|TestCancelTurn' -count=1

Runtime cancellation resolves the existing turn cancellation locator before
settlement and diagnostics. It validates the canonical request and preserves its
run identity, request key, fingerprint and provenance without reading message
bodies. Regression coverage rejects whole-journal reads during Stop, shutdown
and execution-context cancellation; storage coverage bounds control-record reads
and rejects corrupt locators. The cancellation deadlines remain unchanged.

Ordinary `FinishTurn`, cancellation and unknown-effect settlement now select
their records through the existing ordered per-turn canonical index. Selection
checks the started marker, expected record count, ordinal order, bounds, thread
identity and turn identity before using the records. It does not copy the full
journal or hydrate unrelated history. The canonical path remains fully read
when a completed turn carries `goal_guard`, because Goal review validity depends
on the complete path. Other canonical bodies are still validated on explicit
complete reads and existing storage/startup boundaries.

The regression installs a body reader that fails on historical access: an
ordinary finish succeeds without reading it, while an explicit full-path read
returns the original error. Existing Goal review, cancellation, interaction,
effect and transaction tests protect terminal semantics. This removes one
history-proportional completion path; it does not qualify actor residency,
provider context construction, full projections or end-to-end Flower
performance.

## Canonical dense-tool qualification fixtures

The giant Unicode output race profile identified full-body path sanitization
as a major completion cost. `event.SafePathRefsText` now first checks for the
slash or Windows separator required by every accepted regexp alternative.
Text containing either still follows the same sanitizer. This necessary-input
filter preserves path redaction and avoids Unicode regexp work for path-free
bodies; it does not bypass canonical integrity validation or qualify startup.

`runtime/long_thread_fixture_test.go` writes deterministic synthetic history
through the real SQLite repository, canonical turn admission and settlement.
It is test tooling, not a host-facing write API. From the exact published
module, create an empty output directory and run:

    FLORET_LONG_THREAD_FIXTURE_DIR=/absolute/empty/directory FLORET_LONG_THREAD_FIXTURE_SCALE=100 GOWORK=off go test github.com/floegence/floret/v7/runtime -run '^TestWriteLongThreadQualificationFixture$' -count=1 -timeout=30m -v

Scales 1, 10 and 100 produce 1,096, 10,960 and 109,600 presentation items.
Each unit has nine completed turns, 1,046 paired tools, and a 661-tool turn.
Renderer counts per unit are 872 terminal, 92 file, 29 patch, 19 structured,
17 web search, 16 SubAgent list operations and one web fetch. The completed
profile uses nine users and 41 assistant items in place of the reference's
eight users, 33 assistants, eight Goals and one interaction; it does not claim
to qualify interactive lifecycle correctness. Setting
`FLORET_LONG_THREAD_FIXTURE_SINGLE_TURN=1` concentrates the same item/tool
counts in one turn.

The manifest records counts, renderer distribution, the ordered journal hash,
complete public item hash and closed SQLite file hash. Preserve the exact
writer version, commit and test-source hash with downstream evidence. Copy the
closed file into fresh isolated product state; never edit domain records in a
host. Schema compatibility still applies: a newer fixture cannot qualify an
older writer that rejects its schema. The ordinary regression reopens the
fixture, checks completed canonical boundaries and continues it through the
public runtime. Generation and explicit complete validation are outside every
measured interval. Fixtures alone do not certify production performance.


## Provider request preparation

The Engine runner owns one execution-scoped summary: the first turn surface,
first exact TurnID provider/model, and whether any matching request disagreed.
It reads canonical requests once and retains no historical request collection.
Successful append/checkpoint operations update that summary; failed writes do
not. Each retry/restart execution reloads canonical facts. PreparedStore forwards
the same owner; dynamic tools still refresh at every existing boundary.

Ordinary lineage and surface comparisons use the existing exact latest-request
reader. Returning to an older render lineage still searches for its last matching
generation and checks the full segment prefix. First-turn surface matching keeps
its existing trimmed TurnID/RunID rules; model validation retains exact TurnID
matching and rejects any earlier conflicting request.

    GOWORK=off go test ./internal/provider/cache -run 'TestExecution|TestLineageFast'
    GOWORK=off go test ./internal/engine -run '^TestExecutionReadsTurnHistoryOnceAcrossToolSteps$'
    GOWORK=off GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench '^BenchmarkSQLiteExecutionRequestHistory$' -benchtime=10x -count=3 -benchmem

The real-Engine operation-count regression uses one or twelve tool calls and a
dynamic surface. Full request reads fall from 10/65 to one per execution. The
SQLite benchmark uses 100/1,000 historical requests, approximately 16.4 KiB system
prompts, a short canonical conversation and deterministic provider output. It
includes request preparation, successful SQLite request/response checkpoints,
and completed Engine execution. Each measured iteration appends a request; it
is not a fixed-history point-lookup benchmark.

October 22, 2026 Apple M5 Pro / Go 1.27.3 diagnostic medians of three alternating
10-iteration runs against da95596 (temporary source overlay, no module rewiring):

| Historical requests | Before ms/execution | After ms/execution | Before bytes/execution | After bytes/execution |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 94.95 | 21.37 | 245,131,366 | 46,496,092 |
| 1,000 | 778.78 | 150.89 | 2,305,077,316 | 389,844,408 |

At 1,000 records the median time falls 80.6% and allocation falls 83.1%. Some
samples overlap repository checks; timings are diagnostic, not product CPU
acceptance. First-read restore, historical surface switches, segments, response
history and canonical tokenization remain outside the bounded repeated path.
No user output cadence, history, tool permission or UI behavior is reduced.
Published Redeven adoption and production-renderer interaction measurements are
separate qualification gates; this benchmark cannot establish total app CPU.

## Parent receipt pages

Prepared parent reads use canonical path receipt ranks and a persistent tree of
canonical admission interval sources. Metadata contains source references and
positions, not report text or independently advancing lifecycle. Arbitrary
released disjoint admissions, branch rollback and Fork retain the complete
projector's semantics. Receipt, boundary, admission and interaction sources are
validated before use; pending-only reads validate an admission before skipping
its interval. Writes and deletion share the existing canonical transaction.

Selected interaction resolution uses exact interaction and waiting-boundary
sources in canonical journal order, including retained branches. It does not
load the child's unrelated turn entries or tool bodies. Missing historical
source metadata is prepared by the canonical owner before retrying the read.
Complete execution/admission validation retains its complete canonical path.

The regression with 1,000 unrelated parent turns uses 18 gets and no scans for
an empty page. One approval among 1,000 unrelated child messages uses 46 gets
while pending and 49 after resolution, with no scans. These are deterministic
access counts, not product latency. Source-thread filters can still inspect
unmatched receipts, selected canonical report records contain their report
text, and historical preparation is not bounded by page size. Do not describe
all parent-event browsing or first-upgrade reading as fully bounded.
