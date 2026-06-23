---
type: Maintainer Workflow
title: Add a Provider
description: Provider support must preserve normalized streaming contracts, capability boundaries, and deterministic fake-provider tests.
resource: /internal/provider/doc.go
tags: [workflow, provider, model]
timestamp: 2026-06-20T00:00:00Z
---

# When To Use

Use this workflow when adding a provider adapter, model catalog entry, or
provider-specific request rendering behavior.

# Steps

1. Add catalog metadata for model defaults, context policy, pricing, reasoning,
   and capability support.
2. Implement provider streaming through the normalized provider contract.
3. Validate terminal stream behavior, usage reporting, finish reasons, tool
   calls, and hosted tool result matching.
4. Keep capability selection derived from provider profiles and explicit support
   metadata.
5. Preserve fake-provider paths for deterministic tests.

# Reasoning Capabilities

Reasoning control is model-level capability data, not a provider-wide enum. A
catalog row that supports reasoning must include its official source URLs,
`source_checked_at`, wire shape, fixture name, supported values, disable support,
and budget bounds when the provider exposes a budget.

Adapters must reject unsupported reasoning levels or budgets before rendering a
provider payload. Dynamic metadata providers such as OpenRouter and local Ollama
models must not expose static selectable effort values until the provider/model
metadata has been resolved by the host.

# Guardrails

Do not add provider-name shortcuts to generic capability resolvers. Provider raw
plans are provider-specific rendered fragments; invalid fragments should fail
explicitly.
