---
type: Maintainer Workflow
title: History Performance Qualification
description: Reproduce fixed-change SQLite scaling and distinguish upstream foundations from complete product acceptance.
resource: /internal/sessiontree/history_performance_test.go
tags: [workflow, performance, history]
timestamp: 2026-10-20T00:00:00Z
---

# Fixed-history SQLite mutation

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

    GOWORK=off go test ./internal/sessiontree -run TestQueryDocumentTermsMatch -bench BenchmarkQueryDocumentTermsLargeBody -benchtime=1x -benchmem
    GOWORK=off go test ./runtime -run TestCanonicalSharedContent -bench BenchmarkCanonicalSharedContentProjection -benchtime=1x -benchmem

The first diagnostic indexes repeated mixed ASCII/CJK/emoji text in 16 KiB
segments. A joined-string oracle checks identical sorted unigram, bigram and
trigram terms, including separators, presentation JSON, NUL, case mapping and
UTF-8 split across segments. Source bytes are visited once; only unique grams
are encoded. Memory still depends on distinct grams, not a fixed constant for
arbitrary content. On the same machine a single 40 MiB sample changed from
4.29 s / 1,999,104,776 allocated bytes to 0.60 s / 3,816 bytes. This is an
initialization subpath, not an incremental merged-source acceptance result.

The second diagnostic projects 10,960 canonical segments with one shared source
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

    GOWORK=off go test ./internal/sessiontree -run 'Test(CurrentStartup|StartupMetadataQuery|Historical|CanonicalBodies|Inspection|PersistentQuery)' -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkCurrentStartupCanonicalLoad -benchtime=1x -benchmem

Current-schema startup verifies each complete immutable entry before retaining
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

## Streaming query rebuild sources

    GOWORK=off go test ./internal/sessiontree -run 'Test(CanonicalQueryTerms|QueryDocumentTerms)' -count=1
    GOWORK=off go test ./internal/sessiontree -run '^$' -bench BenchmarkCanonicalQueryTermsStorage -benchtime=1x -benchmem

Derived-index rebuilds visit canonical content, arguments, results and typed
presentation in bounded source chunks. Complete and storage-backed paths share
one separator/Unicode/ngram collector. SQLite differential fixtures compare
all terms to an independent joined-string oracle, include split UTF-8, source
segments, references and hosted tools, reject private-reasoning reads, and
check that no complete source enters the body cache. Failure and cancellation
return no partial term set. A single actual-SQLite diagnostic measured 1.7 ms /
833,888 allocated bytes for one roughly 40 KiB source and 124.0 ms / 81,100,976
allocated bytes for 100 segments. These totals include physical decoding and
are not retained heap, RSS or fixed-change latency. Distinct term inventory and source metadata still
grow with content; incremental updates to a merged query document still require
separate work. This is not production-stack acceptance.
