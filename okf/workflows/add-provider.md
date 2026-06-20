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

1. Add catalog metadata for model defaults, context policy, pricing, and
   capability support.
2. Implement provider streaming through the normalized provider contract.
3. Validate terminal stream behavior, usage reporting, finish reasons, tool
   calls, and hosted tool result matching.
4. Keep capability selection derived from provider profiles and explicit support
   metadata.
5. Preserve fake-provider paths for deterministic tests.

# Guardrails

Do not add provider-name shortcuts to generic capability resolvers. Provider raw
plans are provider-specific rendered fragments; invalid fragments should fail
explicitly.
