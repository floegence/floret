---
type: Architecture Concept
title: Canonical Host Work
description: Minimal durable facts for asynchronous local tool effects and their model-visible completion.
resource: /runtime/thread_host_work.go
tags: [runtime, tools, storage]
timestamp: 2026-11-02T00:00:00Z
---

# Summary

The host owns the OS process and output. Floret owns the authorized invocation,
immutable completion facts, notice admission and continuation eligibility.
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

# Shutdown and uncertain effects

`Host.SuspendExecution` rejects new executions, provider dispatch and local tool
dispatch while allowing settlement. A host suspends first, drains/reaps owned
processes, records confirmed results, then closes Floret. On abnormal restart,
bindings from the prior executor become unknown once. No PID adoption, shell
replay, stdin resend or automatic provider wake follows an unknown effect.
Unsealed output cannot be reconstructed. Forks retain readable facts/output but
never inherit process control or completion wake authority. Explicit Stop and
deletion suppress autonomous completion continuation.

# Storage

Schema 34 to 35 changes only the writer fence; all historical automatic edges
remain supported. Bound/final facts and admitted notices live in the canonical
journal. The existing sparse index stores source locators only. Active executor
and per-thread recovery coordination stay in memory. Hosts never inspect or
rewrite Floret-owned records.

# Evidence

- [Public boundary and tests](../../runtime/thread_host_work_test.go)
- [Transactional settlement and fork](../../internal/sessiontree/host_work_test.go)
- [Migration preservation and rollback](../../internal/sessiontree/backend_repo_v35_test.go)

Notify controls automatic continuation only. Every unobserved final fact is
admitted at the next provider preparation, including work stopped during host
shutdown. A final synchronous tool result uses ObservedHostWork to avoid
repeating that fact. Unknown work remains unknown in both item metadata and
its final Activity; it must never be presented as success.
