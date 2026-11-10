---
type: Architecture Concept
title: Canonical Host Work
description: Minimal durable facts for asynchronous local tool effects and their model-visible completion.
resource: /runtime/thread_host_work.go
tags: [runtime, tools, storage]
timestamp: 2026-11-02T00:00:00Z
---

# Summary

Failed-turn retries may admit a completed host-work notice after their canonical
checkpoint. The retry boundary accepts that typed, canonically identified fact
without moving the checkpoint or replaying the tool. Ordinary user/assistant
suffixes still fail lineage validation. The notice and confirmed result survive
restart and subsequent turns ([regression](../../runtime/thread_retry_host_work_test.go)).

The host owns the OS process and text capture. Floret owns retained text, the
authorized invocation, immutable completion facts, notice admission and continuation eligibility.
`ThreadHostWorkService` adds four host methods without adding model tool names.

# Execution and delivery

`BindHostWork` requires an exact dispatching effect attempt. A work ID has one
binding and one immutable final fact. Result JSON is at most 16 KiB; notices are
at most 2 KiB; optional retained output is at most 8 MiB. Output-storage failure
may commit the factual completion without an output reference; arbitrary commit
failure is returned without pretending that persistence succeeded.

The next provider preparation admits pending notices once. If a thread is
naturally idle, a context-only continuation may run. `HostWorkSourceTurnID`
identifies the original admitted turn so hosts can restore its execution
configuration. `Result.ObservedHostWork` suppresses a duplicate notice after an
explicit observation has already returned the final result. A final Activity
updates the original tool item through existing canonical field sources.

A final fact can arrive while a later user turn is executing. It retains the
original invocation's turn and run, while occupying its actual chronological
journal position. Canonical turn reads validate and traverse these intervening
settlement facts without assigning them to the newer turn or rejecting that
turn's final response. Same-turn omissions, ordinary foreign execution entries,
invalid settlement facts and broken ancestry remain errors. Current-store reads
and restart need no record rewrite or schema migration.

# Shutdown and uncertain effects

`Host.SuspendExecution` rejects new executions, provider dispatch and local tool
dispatch while allowing settlement. A host suspends first, drains/reaps owned
processes, records confirmed results, then closes Floret. On abnormal restart,
bindings from the prior executor become unknown once. No PID adoption, shell
replay, stdin resend or automatic provider wake follows an unknown effect.
Legacy unsealed snapshots cannot be reconstructed. With streaming capture, the
committed prefix remains readable as incomplete after restart. Forks retain readable facts/output but
never inherit process control or completion wake authority. Explicit Stop and
deletion suppress autonomous completion continuation.

# Storage

Schema 34 to 35 changes only the writer fence; all historical automatic edges
remain supported. Bound/final facts and admitted notices live in the canonical
journal. The existing sparse index stores source locators only. Active executor
and per-thread recovery coordination stay in memory. Hosts never inspect or
rewrite Floret-owned records.

Schema 38 to 39 adds optional streaming capture, with bounded append/read/search
APIs and pinned Fork watermarks. It does not replace the original 8 MiB snapshot
API or create another execution lifecycle. See [tool output retention](tool-output-retention.md).

# Evidence

- [Public boundary and tests](../../runtime/thread_host_work_test.go)
- [Overlapping-turn read and restart](../../internal/sessiontree/canonical_turn_host_work_test.go)
- [Transactional settlement and fork](../../internal/sessiontree/host_work_test.go)
- [Migration preservation and rollback](../../internal/sessiontree/backend_repo_v35_test.go)

Notify controls automatic continuation only. Every unobserved final fact is
admitted at the next provider preparation, including work stopped during host
shutdown. A final synchronous tool result uses ObservedHostWork to avoid
repeating that fact. Unknown work remains unknown in both item metadata and
its final Activity; it must never be presented as success.

# Public Activity outcome contract

The additive public API decision exposes the already persisted unknown outcome
through `ActivityStatusUnknown`, `ActivityAttentionUnknown` and
`ActivityCounts.Unknown`. Redeven/Flower and other observation consumers must
render it as a terminal warning requiring attention. Successful run completion
cannot turn an unknown tool outcome into success; late dispatch/approval events
cannot reactivate it. Unknown does not authorize replay or claim effect failure.
Canonical full and window projections use the same status/attention facts.

Settled ranges include unknown observations. Their exclusion discriminator is
derived from the existing canonical status and range counts, including metadata
written before this correction. Reads do not rewrite current stores, introduce
a second lifecycle, or require a canonical schema migration.

Regression coverage includes the public validator after executor restart,
summary/counts, released metadata reads without mutation, and blank-module
adoption. Producer success and API symbol compatibility alone are insufficient;
consumers must qualify the final public DTO and rendered state.
