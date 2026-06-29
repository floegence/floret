# Floret OKF Update Log

## 2026-06-29
* **Update**: Documented tool approval activity as a lifecycle state on the tool
  item itself, preventing duplicate tool and approval rows for one invocation.
* **Update**: Documented live `runtime.Event.ActivityTimeline`, event-time tool
  detail projection, and duration-consistent activity validation for tool
  lifecycle rows.
* **Update**: Documented `SettlePendingTool` as the detail-only counterpart to
  `CompletePendingTool`, including the rule that successful turns leave
  host-owned pending activity running until an explicit settlement arrives.
* **Update**: Documented terminal activity settlement for cancelled and failed
  turns, including the rule that hosts consume Floret terminal projections
  instead of synthesizing final tool state.
* **Update**: Documented public runtime not-found sentinels for host facade
  integrations so downstream products can use `errors.Is` without parsing error
  strings or importing internal packages.

## 2026-06-28
* **Update**: Documented `ThreadTurnProjection` as the Floret-owned display
  projection for hosted turns, including control-signal segments and the rule
  that hosts must not synthesize main-thread activity timelines.
* **Update**: Documented Floret-owned row activity timelines and structured tool
  result status for thread and subagent detail APIs, so hosts do not rebuild
  activity from raw content or audit tables.
* **Update**: Documented product-neutral pending approval snapshots as the
  current-state companion to approval detail events, while keeping approval UI
  and product policy in downstream hosts.
* **Update**: Documented parent stop versus thread delete lifecycle boundaries
  for subagents, including public `CloseSubAgents`, cascading
  `DeleteThread`, and engine-owned subagent fork mode.
* **Update**: Documented transcript-free thread summary recovery and
  parent-scoped subagent activity timelines for host UI integration.
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
