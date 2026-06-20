---
type: Maintainer Workflow
title: Change Public API
description: Public API changes must preserve Floret's compact downstream surface and update documentation, tests, and OKF knowledge together.
resource: /internal/architecture/architecture_test.go
tags: [workflow, public-api, maintenance]
timestamp: 2026-06-20T00:00:00Z
---

# When To Use

Use this workflow when changing exported contracts in `config`, `runtime`,
`tools`, or `observation`, or when considering a new public package.

# Steps

1. Confirm the capability is product-neutral and belongs in Floret.
2. Keep implementation details behind the public facade.
3. Update package tests and architecture boundary tests.
4. Update README public API guidance when downstream usage changes.
5. Update this OKF bundle when the change affects integration guidance or
   project knowledge.
6. Run the repository quality gate.

# Guardrails

Do not expose implementation packages as downstream contracts. Do not add a
new public package without updating the public package allowlist and
documentation.
