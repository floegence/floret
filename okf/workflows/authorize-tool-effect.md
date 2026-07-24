---
type: Adoption Workflow
title: Authorize a Tool Effect
description: Join host product policy to Floret's generic effect, approval, and tool execution lifecycle.
resource: /cmd/examples/tool-effect-approval/main.go
tags: [workflow, adoption, tools, approval, effects]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Require an intuitive product approval flow for an effectful domain tool without
moving product authorization or UI semantics into Floret.

# Steps

1. Register a strict tool schema, effect classification, and product-neutral
   resource references through `tools.Registry`.
2. Implement `runtime.EffectAuthorizationGate` using the host's current user,
   workspace, and security policy.
3. Present Floret's canonical approval queue in the product UI. Make the target,
   effect, risk, and pending state clear, and keep approve and reject actions
   explicit.
4. Resolve only the exact current approval generation and revision with a
   stable `DecisionID`; treat stale decisions as conflicts and reload.
5. Invoke the one-shot `AuthorizedEffect` only after approval, while the tool
   handler independently enforces domain authorization and cancellation.

# Verify

Run the [effect approval example](/cmd/examples/tool-effect-approval) and the
approval/effect contracts in [`florettest`](/florettest/doc.go). The canonical
lifecycle remains the [`runtime` API](../api/runtime.md), not a host audit log.

# Boundary

Floret owns generic permission/effect metadata, durable approval identity and
lifecycle, dispatch, and result projection. The host owns policy, approver
identity, user-facing language, concrete domain action, and visual treatment.
