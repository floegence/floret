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
3. Run `scripts/check_v1_api_compatibility.sh` and update the generated v1 API
   baseline only after the compatibility and design review is explicit.
4. Update package tests and architecture boundary tests.
5. Update README public API guidance and `CHANGELOG.md` when downstream usage changes.
6. Update this OKF bundle when the change affects integration guidance or
   project knowledge.
7. Run the repository quality gate.

# Guardrails

Do not expose implementation packages as downstream contracts. Do not add a
new public package without updating the public package allowlist and
documentation. A compatible addition is still a governed API change. Do not
delete, rename, narrow accepted inputs, change JSON values, or reclassify
documented errors within v1.
