---
type: Workflow
title: Change The V2 Public API
description: Review and verify deliberate changes to exported v2 contracts.
resource: /internal/architecture/testdata/v2-public-api.txt
tags: [workflow, api, compatibility]
timestamp: 2026-07-29T00:00:00Z
---

# Change The V2 Public API

1. Decide whether the capability belongs in `config`, `provider`, `runtime`,
   `storage`, `tools`, `observation`, or test-only `florettest`.
2. Add external-package compile and behavioral tests before implementation.
3. Keep runtime authority identity-bound and prevent provider, SQLite, or host
   product types from entering runtime contracts.
4. Add Go documentation for every exported symbol.
5. Update README, OKF, changelog, and the generated v2 API baseline.
6. Run `scripts/check_v2_api_compatibility.sh` and the blank-module adoption
   gate. After v2.0.0, incompatible changes require the next major version.

Do not add aliases, deprecated facades, dual shapes, silent parsing, or fallback
execution paths to preserve an incorrect pre-release contract.
