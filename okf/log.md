# Floret OKF Update Log

## 2026-06-28
* **Update**: Documented that provider continuation recovery preserves the
  Floret ordered transcript by committing live assistant prefixes and
  backfilling only uncommitted suffixes.
* **Update**: Documented ordered hosted thread detail events and committed
  thread-entry observations as Floret's public durable execution transcript
  read model for downstream hosts.

## 2026-06-26
* **Update**: Documented `runtime.ToolSurfaceProvider` as the product-neutral
  dynamic tool surface hook for refreshing tools, hosted tools, prompts, and
  host context at provider-loop safe points.
* **Update**: Documented the hosted context lifecycle boundary and removed the
  old projected transcript integration path from public runtime guidance.
* **Update**: Documented terminal control-signal output normalization from
  signal payloads or same-step assistant text.
* **Update**: Documented Floret-owned manual compaction admission and terminal
  `noop` observations for contexts that are too small, lack a safe cut point,
  or would not shrink enough to justify checkpoint creation.

## 2026-06-25
* **Update**: Documented public manual compaction operation identity, cancelled
  lifecycle observations, poll-stage diagnostics, and the Test UI context
  compaction scenario check.
* **Update**: Documented public compaction debug observations for safe
  diagnostics across generation, projected request rebuild, validation, and
  installation stages.
* **Update**: Documented projected manual context compaction, including active
  safe-point polling, idle compaction-only checkpoint results, and observation
  request correlation.

## 2026-06-24
* **Update**: Documented compacted context targets and the requirement that a
  complete compaction event follows full provider request validation.
* **Update**: Documented that durable compaction entries are committed only
  after the compacted provider request has passed validation.
* **Update**: Documented parent-scoped subagent detail APIs, bounded wait
  semantics, child run timeout behavior, and close-as-stop lifecycle rules.

## 2026-06-23
* **Update**: Documented provider-neutral reasoning selection in the public
  config and runtime APIs.
* **Update**: Added provider workflow guidance for model-level reasoning
  capabilities, official provenance, dynamic metadata, and adapter validation.

## 2026-06-20
* **Creation**: Added the initial OKF v0.1 project knowledge bundle.
* **Update**: Documented OKF maintenance rules in the repository guide.

## 2026-06-23
* **Update**: Documented parent-managed durable child threads in the runtime API,
  runtime layers, and execution identity concepts.
