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
