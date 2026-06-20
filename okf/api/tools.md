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

# Key Source Files

* [Tools Package](/tools/tools.go)
* [Schema Helpers](/tools/schema.go)
* [Permissions](/tools/permission.go)
* [Output Projection](/tools/output_projection.go)
