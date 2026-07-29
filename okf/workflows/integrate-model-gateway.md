---
type: Workflow
title: Integrate A Provider Gateway
description: Route every model call through one explicit provider contract.
resource: /provider/provider.go
tags: [workflow, provider, gateway]
timestamp: 2026-07-29T00:00:00Z
---

# Integrate A Provider Gateway

Implement `provider.Gateway` with explicit Identity, Capabilities, and Stream.
Keep credentials, base URL, HTTP client, and provider routing in the host. Use
`provider.NewOpenAICompatible` or `provider.NewAnthropic` when their wire format
fits.

If attachments are expanded, also implement `provider.RequestPreparer` and
return the exact provider-native prepared fragment. Do not reconstruct missing
or malformed raw plans from higher-level requests.

Pass the Gateway to `runtime.NewAgent`. Runtime never selects another provider
when Gateway is nil; construction fails instead. Deterministic tests use
`florettest.ScriptedGateway`.
