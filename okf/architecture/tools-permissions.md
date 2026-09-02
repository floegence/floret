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

Schema rejection remains a non-executable provider correction result. A host
may separately derive safe metadata from a parseable invalid JSON object through
`Definition.InvalidActivity`; that callback has no resource, permission,
approval, effect, or handler authority. The call and ordered validation result
remain available to the next provider request, while Floret emits no public
tool activity and therefore creates no canonical timeline row for the invalid
call.

Read-only safe tools may default to allow. Riskier tools must explicitly ask,
allow, or deny. Open-world and destructive tools require careful permission
declarations.

# Public Web Fetch

`tools/webfetch` owns one product-neutral `web_fetch` implementation. It uses a
fixed GET-only HTTP/HTTPS policy, validates every DNS and redirect target again
at dial time, bounds reads and model-visible output, decodes text charsets, and
converts sanitized HTML to Markdown. The tool does not provide authentication,
custom headers, non-GET methods, binary download, JavaScript rendering, or
browser state.

The package owns network safety, parsing, result fields, and typed Activity
metadata. A host owns visibility and supplies the current product permission
through `Options.Permission` and `Options.PermissionFor`. This keeps one fetch
execution path without moving product modes or approval UI into Floret.

# Hosted Tool Boundary

Hosted provider tools are provider-native capabilities. They are not dispatched
by the local tool runtime and must not be treated as ordinary local handlers.

`runtime.WithAgentRunLabels` carries immutable correlation and opaque host
context through provider requests, permission gates, and local tool
invocations. The maps are execution metadata, not authorization evidence;
effect gates still validate current host policy before dispatch.

# Dynamic Tool Surfaces

`runtime.ToolSurfaceProvider` is the public host hook for resolving the active
tool surface at safe execution boundaries. The hook returns product-neutral
data: a registry or explicit local tool definitions, hosted tool definitions,
system prompt text, host context, and audit metadata. The first provider
checkpoint freezes the complete surface for the Turn; later tool loops, Ask
User resume, retries, and restart recovery use that same snapshot.

Provider-visible system text and tool definitions are part of one render
lineage's stable envelope. A new Turn may use a changed surface, which starts a
new render lineage and sends the complete canonical history without compaction
or journal mutation. History-prefix mutation or reordering still fails with
`context_prefix_drift`; it is never repaired by rewriting an earlier request.

Provider history comes only from durable typed facts. Ordinary and control tool
calls keep their paired tool results even after their definition is removed.
Removed definitions are not re-registered, and a new unavailable call receives
a safe ordinary result. `ask_user` follows the same rule: the assistant call is
preserved, its durable resolution becomes the matching tool result, and secret
values stay outside canonical history. Canonical lineage hashes those typed
facts without journal entry identities, so the same unavailable-tool pair
remains append-only after an active loop is reconstructed for a later Turn.

An omitted (`nil`) local definition list derives provider definitions from the
returned registry; an explicit empty list clears them. Hosted definitions use
the same omitted-versus-explicit-empty contract against their configured
defaults. Runtime adapters preserve this distinction through the engine
boundary so registry inheritance cannot silently become an empty provider
toolset.

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

Floret owns the generic approval lifecycle inside the affected thread current
view. Each pending interaction carries the tool identity, effects, resources,
and presentation required for an independent decision. `ThreadService.Respond`
uses the stable interaction and request identities; the resolution updates the
original tool row and does not create a second approval queue authority. Batch
presentation order does not serialize unrelated effect execution after
authorization.

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
primary in a product surface. `requires_approval` remains true after approval,
but a user-declined terminal item clears it. A host should treat only
`approval_state=requested` with `status=waiting` as an
active pending approval. `approval_state=approved` may briefly pair with
`status=pending` between approval resolution and tool dispatch. Canonical turn
projection merges repeated activity segments by lifecycle progression rather
than boolean union: generic pending activity cannot regress a requested item
from `waiting`, and a resolved rejection clears the earlier approval requirement
and attention state. This remains true for large batches settled in rapid
succession. Canonical turn
projection also closes a historical requested approval when the durable turn
is failed or aborted, including interrupted recovery records that contain a
terminal tool result but predate the matching approval-resolution detail event.
When no later run marker exists, a terminal tool error closes the requested
approval as `timed_out`; a canceled terminal tool result from an interrupted
batch closes it as `canceled`; an ensuing aborted marker also takes precedence
and closes it as `canceled`. Projection never exposes `requested` with a
terminal error or canceled result.
The failed turn projects `timed_out` plus `error`; the aborted turn projects
`canceled` plus `canceled`. Non-terminal and successful conflicts remain invalid
instead of being repaired speculatively.

A submitted decision is durable before the host authorization gate runs. Exact
response-loss replay does not call the gate twice. Finalization atomically
settles the approval and effect, records the proof hash when approved, and
promotes the next queue item. User rejection, policy denial, unavailable policy,
invalid proof, pre-dispatch cancellation, and post-dispatch known/unknown
outcomes each have one deterministic terminal mapping. A downstream host submits
the decision and maps typed conflicts; it does not persist or promote its own
approval queue.

A user rejection settles the canonical approval as `rejected` with reason
`user_rejected`, never enters the host authorization gate or tool handler, and
returns a structured `declined` result to the provider. It records
`decision_source=user` and `executed=false` without `DispatchErr` or a tool-error
status. The provider loop may then explain,
revise, or complete the turn naturally. This expected user decision is not a
turn-level authorization failure. System policy, proof, authority, persistence,
and unknown-outcome failures remain turn-level failures and fail closed. Effect
result finalization applies only after the dispatcher actually enters the
handler; a pre-handler rejection has no effect result to finalize.

An authorized effect callback inherits the active turn's renewable lease
binding rather than a copied heartbeat proof. Approval and host-policy work may
span any number of normal heartbeat renewals; dispatch observes the current
proof while retaining the same thread, turn, owner, acquisition, and generation
authority. Cancellation while an effect is waiting for approval uses that same
current proof to atomically cancel the approval batch and settle the canonical
turn; it must not leave a prepared admission or requested approval behind after
normal heartbeat renewal. A different owner, generation, turn, or authority
lineage remains a stale proof and fails closed. Atomic approval dispatch,
provider-request inspection, and turn finalization apply this same monotonic
successor rule so a heartbeat transition cannot create contradictory authority
decisions between adjacent persistence boundaries.

An active provider turn does not hold the host mutation fence while it waits for
this decision. Approval resolution and the resumed dispatch use separate short,
serialized backend transactions. Transaction-fence acquisition respects the
operation context so cancellation and request deadlines remain effective while
another transaction is active. A per-thread execution coordinator keeps duplicate
or overlapping turn execution from invoking the provider or handler twice.

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
* [Web Fetch Tool](/tools/webfetch/webfetch.go)
