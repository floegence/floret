---
type: Public API
title: tools Package
description: The tools package defines local tool registration, validation, permission checks, execution, pending results, and output projection.
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
* `Registry.Run` and `Registry.RunBatch` execute validated tool calls.
* Schema helpers build strict tool input shapes.

# Permission Model

Tools declare effects and permission behavior. Approval requests include
validated arguments, resource references, effects, and risk flags so hosts can
make product-specific decisions outside Floret core.

# Dispatch Observation

`tools.RunOptions.DispatchStarted` is the product-neutral observer used by the
engine to mark the exact point where a validated, permitted, and approved local
tool leaves the queue and enters handler execution. It is emitted before the
handler is invoked and carries only the tool call identity, raw arguments for the
engine sanitizer, run/thread context, labels, and host context already available
to the local tool runtime.

# Activity Updates

`Invocation.UpdateActivity` lets a running tool publish sanitized presentation
updates for its own activity item without returning a tool result. The engine
emits those updates as ordered `tool_activity_updated` observations through
`tools.RunOptions.ActivityUpdated`; projections merge them into the existing
`tool:<tool_id>` item. This is for product-neutral public display payloads such
as a host-owned read handle, byte counters, or latest visible output. It does
not create a second activity row, change approval decisions, or complete the
tool invocation.

# Batch Execution

`Registry.RunBatchWithOptions` starts every ordinary call in the supplied model
batch concurrently. `RunOptions.BatchIndex` and `BatchSize` identify each call's
original position for approval and observation metadata; they do not gate
dispatch. Result slices remain in call order even when live observations arrive
in completion order. A dependent call belongs in a later model response after
its prerequisite result is available.

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
