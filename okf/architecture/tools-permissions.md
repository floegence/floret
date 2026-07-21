---
type: Architecture Concept
title: Tools and Permissions
description: Floret separates provider-visible tool definitions, local tool dispatch, effects, resources, approvals, and hosted provider tools.
resource: /tools/doc.go
tags: [architecture, tools, permissions]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Local tools are registered through `tools.Registry`. Registration validates
names, schemas, effects, and permission modes before tools are exposed to a
provider.

# Local Tool Contracts

Each local tool has:

* a provider-visible `ToolDefinition`;
* strict input schema validation;
* typed invocation data;
* declared effects such as read, write, shell, or network;
* resource extraction for approval and observation;
* an output policy for visible and preserved output.

Read-only safe tools may default to allow. Riskier tools must explicitly ask,
allow, or deny. Open-world and destructive tools require careful permission
declarations.

# Hosted Tool Boundary

Hosted provider tools are provider-native capabilities. They are not dispatched
by the local tool runtime and must not be treated as ordinary local handlers.

# Dynamic Tool Surfaces

`runtime.ToolSurfaceProvider` is the public host hook for changing the active
tool surface during a run. The hook returns product-neutral data: a registry or
explicit local tool definitions, hosted tool definitions, system prompt text,
host context, and audit metadata. Floret refreshes that surface before provider
requests and again before local tool dispatch, so provider-visible capabilities
and executable local capabilities converge at safe points.

Product permission modes stay in the host. Floret only sees the projected
registry, tool definitions, hosted tools, and prompt/context text. A stale model
tool call cannot bypass a newer host policy because dispatch uses the refreshed
registry and the same resource, effect, permission, and approver lifecycle as
ordinary tool calls.

# Tool Activity Lifecycle

Floret separates model-observed tool calls from local dispatch. A `tool_call`
activity fact means the provider has requested a local tool and the invocation
is queued for Floret-owned permission, approval, and dispatch handling. It is a
`pending` activity item, not evidence that a handler is running. A
`tool_dispatch_started` fact is emitted only after validation, permission, and
approval gates pass and immediately before the handler is invoked; that fact
promotes the same tool item to `running`.

This split keeps batched tool calls honest: if the first sibling blocks on
approval, later siblings can remain visible as pending work without pretending
that they have started execution. Tool results and pending external results
continue to update the same tool item.

Floret validates and durably prepares every ordinary local call in a model batch
before any handler crosses dispatch. Eligible calls then start concurrently,
including calls with different effects or approval requirements. Floret does
not infer dependencies from tool names, arguments, resources, effects, or
permissions. The model expresses a dependency by waiting for prerequisite
results and emitting dependent calls in a later response. Observation events
may therefore arrive in completion order while provider-visible tool results
remain in the original model call order. Each captured handler result receives a
fresh finalization context when its ordered finalizer runs; a slow batch cannot
expire a faster sibling's persistence window, and one finalization failure does
not skip later sibling finalizers.

# Tool Approval State

Floret owns the generic approval lifecycle and the aggregate root/descendant
approval queue for local tool dispatch. Approval events update the durable
thread detail audit trail, while `runtime.ReadApprovalQueue` exposes queue
generation, ordered items, and exactly one decisionable current item. The queue
carries product-neutral ids, canonical root/child and turn/run identity, tool
names, effects, resources, labels, host context, state, timing, revision, batch
index, and batch size metadata. `ResolveApproval` requires the exact current
generation, revision, approval identity, and stable decision ID. Batch order
keeps presentation deterministic; it does not serialize unrelated handler
execution after authorization.

Hosts own the product authorization policy and user-facing approval experience.
They should translate the generic snapshot into product copy and controls
without moving product modes or UI semantics into Floret. If a host tool
definition supplies an `ActivityPresentation`, Floret carries the sanitized
presentation through approval requested/resolved observations and durable detail
events so the tool activity item keeps the same product-projected label while it
moves through requested, approved, denied, and tool-result states. Ordinary tool
approvals do not create a second visible activity row; approval is part of the
tool invocation lifecycle. Floret still treats that presentation as opaque
display data; tool-specific labels, renderers, and payload fields remain
host-owned. Floret may validate that renderer payloads are safe public data, but
it must not encode downstream UI layout or decide which payload fields should be
primary in a product surface. `requires_approval` remains true after approval or
denial because it is lifecycle history, not the current decision-needed state. A
host should treat only `approval_state=requested` with `status=waiting` as an
active pending approval. `approval_state=approved` may briefly pair with
`status=pending` between approval resolution and tool dispatch.

A submitted decision is durable before the host authorization gate runs. Exact
response-loss replay does not call the gate twice. Finalization atomically
settles the approval and effect, records the proof hash when approved, and
promotes the next queue item. User rejection, policy denial, unavailable policy,
invalid proof, pre-dispatch cancellation, and post-dispatch known/unknown
outcomes each have one deterministic terminal mapping. A downstream host submits
the decision and maps typed conflicts; it does not persist or promote its own
approval queue.

For polling tools, presentation-only arguments can be excluded from generic
repeat identity through the validated tools annotation contract. This keeps
product copy available to activity presentation without allowing copy changes
to bypass Floret's no-progress duplicate-call guard.

# Pending Tool Work

A tool may return a pending result when the host starts work whose lifecycle
continues outside Floret. The host later completes that work through the public
runtime facade.

# Key Source Files

* [Tools Package](/tools/tools.go)
* [Tool Invocation](/tools/invocation.go)
* [Permissions](/tools/permission.go)
* [Pending Results](/tools/pending.go)
