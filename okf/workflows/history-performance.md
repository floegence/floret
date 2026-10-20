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
