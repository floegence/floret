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
