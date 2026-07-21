---
type: Public API
title: tools Package
description: The tools package defines local tool registration, validation, authority-gated dispatch, pending results, and output projection.
resource: /tools/doc.go
tags: [api, tools]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`tools` lets hosts expose product-specific local actions to an agent through
strict provider-visible schemas and typed handlers.

# Main Entry Points

* `tools.Define` builds a typed local tool.
* `tools.NewRegistry` and `tools.NewRegistryE` collect tool definitions.
* `Registry.Register` validates tool contracts.
* `Registry.Dispatch` is the engine-facing dispatch boundary and requires an
  `EffectDispatcher` supplied by Floret's durable thread runtime.
* Schema helpers build strict tool input shapes.

# Permission Model

Tools declare effects and permission behavior. A public registry does not expose
a direct handler runner. Floret first creates a canonical effect attempt, then
passes the exact invocation to the host `EffectAuthorizationGate`; only its
synchronous authorized callback can cross the handler boundary. Missing effect
authority fails before resource extraction or handler side effects.
The dispatcher callback accepts an explicit execution `context.Context` chosen
by the host gate. That context reaches the typed tool handler, while Floret also
binds it to the active turn context so host cancellation can narrow execution
but cannot extend it beyond canonical turn lifetime. Cancellation before the
dispatch boundary leaves the attempt prepared for atomic turn cancellation;
it is not rewritten as an authorization rejection.
`ApprovalRequest`, `PermissionDecision`, and `Approver` are not `tools` package
contracts; test-only authorization helpers live under `internal/testing`.
After the handler crosses `dispatching`, Floret owns result convergence. It
commits the captured success or failure once, or marks the attempt `unknown` if
that exact result cannot be durably finalized. Cancellation and adapter return
errors never authorize a second handler call.

# Dispatch Observation

`tools.DispatchOptions.DispatchStarted` is the product-neutral observer used by the
engine to mark the exact point where a validated, permitted, and approved local
tool leaves the queue and enters handler execution. It is emitted before the
handler is invoked and carries only the tool call identity, raw arguments for the
engine sanitizer, run/thread context, labels, and host context already available
to the local tool runtime.

# Activity Updates

`Invocation.UpdateActivity` lets a running tool publish sanitized presentation
updates for its own activity item without returning a tool result. The engine
emits those updates as ordered `tool_activity_updated` observations through
`tools.DispatchOptions.ActivityUpdated`; projections merge them into the existing
`tool:<tool_id>` item. This is for product-neutral public display payloads such
as a host-owned read handle, byte counters, or latest visible output. It does
not create a second activity row, change approval decisions, or complete the
tool invocation.

# Batch Execution

The engine validates and prepares the complete ordinary-call batch before any
handler may cross dispatch, then starts eligible calls concurrently.
`DispatchOptions.BatchIndex` and `BatchSize` identify each call's original
position for authorization and observation metadata. Handler results may arrive
in completion order, while captured results are finalized and returned to the
provider in original call order. A slow sibling therefore cannot consume a
faster sibling's persistence deadline: each finalization context starts only
when that sibling's finalizer is invoked, and every sibling finalizer is
attempted even if an earlier finalizer fails. A dependent call belongs in a
later model response after its prerequisite result is available.

# Repeat And Progress Metadata

Local tools are protected by the engine's duplicate-call guard by default. A
tool that represents idempotent polling may declare
`tools.AnnotationRepeatPolicy: tools.RepeatPolicyPolling` in its definition
annotations. The tool result must then include
`tools.ResultMetadataProgressToken` in `Result.Metadata` whenever the repeated
call made observable progress. The engine still fails repeated polling calls
when the progress token does not change.

Polling tools that accept presentation-only top-level arguments may declare
those names through `tools.AnnotationRepeatIdentityIgnoredArguments`. Floret
removes only those arguments when computing duplicate and progress identities;
the original validated arguments still reach activity presentation, handlers,
events, and provider-visible history. The ignored-argument declaration is valid
only for polling tools and must name unique properties from the input schema.

# Key Source Files

* [Tools Package](/tools/tools.go)
* [Schema Helpers](/tools/schema.go)
* [Permissions](/tools/permission.go)
* [Output Projection](/tools/output_projection.go)
