---
type: Public API
title: Provider Package
description: The complete model transport boundary used by every Floret Agent.
resource: /provider
tags: [api, provider, gateway]
timestamp: 2026-07-29T00:00:00Z
---

# Provider

`provider.Gateway` is the only model execution path. It exposes explicit
`Identity`, `Capabilities`, and `Stream(Request)`. The package owns provider
messages, tool calls/results, stream events, usage, sources, opaque state, and
prepared-request contracts.

Official OpenAI-compatible, DeepSeek Responses, and Anthropic adapters implement the same Gateway
interface. Credentials, base URLs, and transport clients belong to adapter
construction in the host, never to `config.AgentConfig` or durable state.

A gateway declaring expanded attachment payloads must also implement
`provider.RequestPreparer`. Missing, malformed, or adapter-incompatible
prepared fragments fail explicitly; runtime never reconstructs a second
provider request.

Tests use `florettest.ScriptedGateway`. Production has no fake response field or
provider-name selection branch.

Provider request context has two invariants. The canonical message lineage is
provider-neutral and append-only for one prompt scope and compaction generation.
Rendered system, tool, and message segments use an independent lineage keyed by
provider, model, adapter revision, and cache namespace. Switching models on a
new Turn keeps the complete canonical path without treating the new envelope as
drift. Switching back reuses that model's earlier raw prefix and renders the
intervening canonical messages as a suffix.

Opaque provider state, response IDs, and native continuation metadata never
cross a provider/model change. A smaller target model triggers normal
pre-request compaction only when its own context policy reports pressure.

`provider.NewDeepSeek` is the DeepSeek V4 Pro/Flash transport owner. It sends
stateless Responses requests and exposes provider-hosted search as ordinary
hosted-tool observations, never local tool dispatch. Canonical-prefix hashes
bind opaque provider input/output items to the projected history. Current
system instructions are rendered from the current request. Invalid state,
unsupported items, malformed streams, and missing terminal responses fail
explicitly. Runtime clears provider state at its existing lineage boundaries. Turns with
SupplementalContext neither reuse nor persist provider state, preserving the
existing ephemeral-context fence; their next request uses canonical history.

`DeepSeekOptions` is an additive v7 API decision for the identified Redeven
consumer: it supplies the model, endpoint, credentials, HTTP client, state key,
and immutable sampling/output-format controls. It shares the existing Gateway,
RequestPreparer, Event, and State contracts. No new lifecycle or storage schema
is introduced. The explicit Chat constructor remains available throughout v7.

See [the implementation](../../provider/deepseek.go),
[wire tests](../../provider/deepseek_test.go),
[runtime restart test](../../runtime/deepseek_test.go), and
[DeepSeek's compatibility contract](https://api-docs.deepseek.com/guides/responses_api/).

Static hosted tools are frozen with `runtime.WithAgentHostedTools`; it is
mutually exclusive with the per-step dynamic tool surface option. Engine
admission and provider requests consume that same immutable Agent surface.
