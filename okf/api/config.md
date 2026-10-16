---
type: Public API
title: config Package
description: The config package resolves provider, model, prompt identity, skill configuration, loop defaults, and context policy.
resource: /config/config.go
tags: [api, config]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`config` is the public package for building or loading Floret runtime
configuration. Hosts may construct `config.Config` directly or use `config.Load`
to read environment-backed defaults.

# Responsibilities

* Resolve provider and model defaults.
* Load environment values when requested.
* Resolve agent profile and prompt identity.
* Normalize prompt-cache retention.
* Normalize context policy defaults.
* Normalize provider-neutral reasoning selection from `Config.Reasoning`,
  `FLORET_REASONING_LEVEL`, and `FLORET_REASONING_BUDGET_TOKENS`.
* Carry context compaction budgets, including the compacted message-context
  target used before the engine validates the full provider request.
* Validate required provider settings.

# Reasoning Selection

`Config.Reasoning` is the default reasoning request intent for Floret-managed
runs. It carries a provider-neutral level and optional budget tokens. Provider
adapters validate that intent against the selected model capability before
rendering provider-specific request fields. `ReasoningLevel`,
`ReasoningSelection`, `ReasoningBudget`, and `ReasoningCapability` are owned by
this package; runtime uses them directly and does not provide aliases.

`ReasoningLevelOn` is an explicit enable request, distinct from Default (omit
provider controls), Off, and effort levels. Only capabilities advertising On
accept it. Existing stored selections keep their original meaning.

# Context Policy

`Config.ContextPolicy` controls context window, output headroom, automatic
compaction threshold, retained summary/tail/user-message budgets, and
`CompactedContextTargetTokens`. The target is the message-context budget passed
to compaction; the engine still rebuilds and validates the full provider request
before treating compaction as complete.

# Use With

`RequestEstimate.Breakdown` is an optional comparable value containing disjoint
semantic input estimates for instructions, conversation, tool definitions, calls
and retained results, managed context, summaries, reasoning and expanded media.
Empty Source means unavailable. Offline BPE categories are approximate and do
not sum exactly to native input. Hosts must not rescale them into exact usage.
Supplemental requests omit the breakdown. Historical schema-17 records remain
valid without rewriting; this adds no domain lifecycle invariant.

Optional `ContextBreakdown.Details.Items` refine the tool-definition and managed
context totals. Each item carries a category, tool name or canonical section key,
nonnegative token estimate, and tool registry source/namespace when supplied.
Their sums equal their parent totals; consumers never add parent and child
weights together. Fragment attribution comes from the canonical renderer and
frozen tool surface, never prompt parsing or the latest snapshot. An empty
section key covers shared update framing. Floret supplies product-neutral
attribution; hosts own source grouping. The pointer envelope preserves the
comparability of existing public estimate structs; hosts must compare detail
contents explicitly and treat returned data as detached observation.

* [runtime](runtime.md) when constructing provider-backed runtime capabilities.
* [Observation](observation.md) when interpreting context pressure DTOs.

# Key Source Files

* [Config](/config/config.go)
* [Context Policy](/config/context.go)

## Request-fitted output

ContextPolicy.ModelMaxOutputTokens enables automatic output fitting. The Engine's
single budget solver uses complete projected input, the model capability and a
shared min(window / 100, 4096) safety margin. Automatic compaction starts at 90%;
explicit MaxOutputTokens may lower that threshold and is never silently clamped.
The margin is already within the default 10% buffer. The former reservation
semantics remain available for v7 callers omitting the new capability field.

[Provider gateways](../../provider/provider.go) declare client or server output
control. Client preparation refits the immutable input through the optional
PreparedRequestOutputLimiter; network retries retain its final bytes. Request
pressure exposes request_output_tokens, output_control and safety_margin_tokens.
These are request facts, not actual output usage or permanently occupied input.
