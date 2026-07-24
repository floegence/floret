---
type: Adoption Workflow
title: Integrate a Model Gateway
description: Supply host-owned model transport without taking model-loop or continuation authority from Floret.
resource: /cmd/examples/custom-model-gateway/main.go
tags: [workflow, adoption, model-gateway, provider]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Use a product-owned provider adapter and credential path while Floret remains
the only owner of model-loop execution, tool continuation, context lifecycle,
ledgers, and opaque provider state.

# Steps

1. Implement `runtime.ModelGateway.StreamModel` as a transport adapter over the
   typed `ModelRequest` and `ModelEvent` contracts.
2. Supply non-sensitive `ModelGatewayIdentity`, including a compatibility key,
   and explicit `ModelGatewayCapabilities`.
3. Keep provider transport fields and credentials out of Floret configuration
   when a gateway is selected.
4. Preserve message order, tool-call grouping, result adjacency, cancellation,
   and terminal stream behavior exactly; reject unsupported provider shapes.
5. Pass the gateway only through the selected thread, compaction, or SubAgent
   capability options.

# Verify

Run the [custom gateway example](/cmd/examples/custom-model-gateway) and the
consumer-facing gateway contracts in [`florettest`](/florettest/doc.go). See
the [`runtime` API](../api/runtime.md) for the authoritative request and
capability rules.

# Boundary

The host owns wire transport, credentials, routing, and provider profile
persistence. It must not persist or reconstruct Floret's provider continuation,
request ledger, response ledger, or provider-visible history.
