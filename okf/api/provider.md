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

## Reasoning replay

`Capabilities.ReasoningHistory` is an additive v7.25 API decision for the
Redeven Ollama adapter. `ReasoningHistoryCurrentUser` omits assistant reasoning
before the latest canonical user message from the detached transport projection.
Historical visible answers and complete tool pairs remain present. Tool loops,
reasoning-only output truncation, and retry after that input retain their
reasoning. Ephemeral supplemental answers and compaction summaries do not create
new user boundaries. Pure reasoning messages made empty by this projection are
omitted. The canonical journal, forked history, and prior checkpoints keep the
original reasoning; no schema change or provider-output retry is introduced.

The zero policy and `ReasoningHistoryAll` preserve existing behavior. Unknown
policies fail capability validation. Gateways that require old reasoning keep
the default. Hosts changing policy must reflect it in their state compatibility
identity. Prepared requests estimate and dispatch the same projected payload;
descriptor gateways use the same projection for generic estimates. This prevents
expired tool-availability assumptions in old thinking from anchoring a fresh
request without discarding user-visible facts or disabling current reasoning.

Evidence: [projection and estimation](../../runtime/model_gateway_provider.go),
[boundary tests](../../runtime/reasoning_history_test.go), and
[tool, fork, and restart tests](../../runtime/thread_tool_history_test.go).

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

`provider.NewDeepSeek` owns the stateless DeepSeek Responses transport. The
2026-10-03 catalog supports `deepseek-flash` (V4.1 Flash, text/image) and
`deepseek-v4-pro` (text only), with `off`/`low`/`high`/`max` reasoning and a
`high` default. Context/output ceilings are 1,000,000/393,216 tokens. Official
legacy Flash aliases retain their exact selected identity and now share Flash
image/reasoning capabilities. Existing Redeven conversations are the identified
consumer; no alias removal is scheduled within v7. Any future removal requires
both supplier retirement and a major-version migration decision.

The official API ignores built-in search tools. The preset disables search;
previously published explicit hosted-tool input and opaque search-item replay
remain valid throughout v7. Hosted observations never dispatch local tools. Canonical-prefix hashes
bind opaque provider input/output items to the projected history. Current
system instructions are rendered from the current request. Invalid state,
unsupported items, malformed streams, and missing terminal responses fail
explicitly. Runtime clears provider state at its existing lineage boundaries. Turns with
SupplementalContext neither reuse nor persist provider state, preserving the
existing ephemeral-context fence; their next request uses canonical history.

`DeepSeekOptions` is an additive v7 API decision for the identified Redeven
consumer: it supplies the model, endpoint, credentials, HTTP client, state key,
and immutable sampling/output-format controls. The optional
`ResolveAttachment(context.Context, provider.Attachment) ([]byte, error)` field
is an additive v7.9 decision for Redeven's prepared image transport. Model
metadata gates image input independently of provider identity. Resolution stays
under host authorization and runs before estimation; prepared streaming never
resolves again. Opaque replay stores descriptor/digest pairs and verifies
freshly resolved bytes without persisting image payloads. It shares the existing Gateway,
RequestPreparer, Event, and State contracts. No new lifecycle or storage schema
is introduced. The explicit Chat constructor remains available throughout v7.

DeepSeek preparation estimates the exact rendered Responses body with a
conservative offline official V4 tokenizer count plus 10% text headroom and a documented upper bound of 1024
tokens for each image. Image transport URLs are not text tokens. This applies
equally to user images, tool-result images, and resolved history replay; tool
schemas, arguments, and literal data-URL text retain their text cost. Estimation
does not alter the frozen body, fingerprint, or image bytes. Hosts must use the
same prepared request for budget admission and streaming, not estimate an
intermediate host DTO. See the [vision budget contract](https://api-docs.deepseek.com/guides/vision/)
and [budget regression tests](../../provider/deepseek_budget_test.go).

Non-successful DeepSeek HTTP responses are returned as `provider.ProviderHTTPError`
with status, provider code, and a bounded sanitized message. Context-overflow
responses continue to use `provider.ErrContextOverflow`; hosts must use the
typed error for actionable provider diagnostics and must not expose credentials
or raw request bodies.

HTTP 413 preserves both the typed transport status and the established overflow
classification. The runtime translates public overflow errors at its engine
boundary for direct and streamed failures. The existing bounded compaction path
then prepares a new request from canonical context; it does not replay completed
tools, resize images, or change the thread. Image-history regression coverage
requires the newest tool image after compaction and exactly one execution per
original tool call. On a provider overflow, retention uses the latest complete
interaction instead of the ordinary text-token tail window, which cannot bound
image transport bytes. Matching tool calls/results and the latest batch stay
together; supplemental user anchors remain protected. Earlier exchanges enter
the canonical summary, while the journal and host image resources stay intact.
The decision is recorded as `retained_tail_strategy=latest_interaction`. Coverage
includes long earlier observations and mixed-size images rejected by actual
serialized body length, not only short histories or image counts. A protected
anchor or an oversized latest interaction may still prevent recovery; a failed
compaction or repeated overflow remains a failure.

See [the implementation](../../provider/deepseek.go),
[wire tests](../../provider/deepseek_test.go),
[runtime restart test](../../runtime/deepseek_test.go), and
[image overflow recovery tests](../../runtime/deepseek_overflow_test.go), and
[DeepSeek's compatibility contract](https://api-docs.deepseek.com/guides/responses_api/).

Static hosted tools are frozen with `runtime.WithAgentHostedTools`; it is
mutually exclusive with the per-step dynamic tool surface option. Engine
admission and provider requests consume that same immutable Agent surface.

Web-search observations retain the operation, target URL, query, and find
pattern through sparse completion events. `HostedToolResult.ResultsProvided`
records whether a structured result list was supplied, including an empty list.
The canonical `tools.WebSearchActivityPayload` carries these public facts and
source snippets. Status-only merges preserve facts; an explicit empty result
list clears earlier results. Hosts localize presentation and open safe links;
they must not decode opaque state or infer per-call results from answer citations.

Internal model metadata is generated from a pinned models.dev snapshot with
explicit official protocol corrections. It supplies token limits, cost estimates,
and reasoning capabilities; it does not own user selection or credentials.
The [maintenance workflow](../../scripts/model-catalog/README.md) regenerates
offline and keeps live OpenRouter/Ollama discovery in the host. No v7 public
catalog API or domain schema is added.

Native pressure calibration validates the existing request record's canonical
history prefix, model, prompt scope, execution envelope, rendering revision,
compaction lineage, and estimator identity. It uses the complete estimate delta,
including overhead, independently of transient message IDs. Missing evidence,
changed history, ephemeral input, and negative deltas invalidate calibration.
The tokenizer source and estimation limitations are documented in the
[regeneration guide](../../scripts/deepseek-tokenizer/README.md).

## Model-aware estimation

`provider.EstimateRenderedRequest` accepts a `RenderedRequest` with explicit provider,
model and `RequestFormat`. It counts frozen model input, excludes protocol controls,
and uses official OpenAI vocabularies only for verified model mappings. Unknown
models use an identified multilingual BPE proxy. Actual images are budgeted separately
from transport bytes; opaque files remain conservative. No resource resolution or
network preflight occurs. Custom opaque transports use `RequestFormatJSON` and the
generic estimate method, without claiming final server rendering. Native usage and
canonical-prefix calibration remain the pressure authority. See the
[estimator source and limits](../../scripts/openai-tokenizer/README.md) and
[public implementation](../../provider/request_estimate.go).

# Ollama Thinking Metadata

`ParseOllamaReasoningCapability` reads the `thinking` object from `/api/show`
without model-name inference. Boolean pairs expose On/Off, true-only is fixed
on, false-only is unsupported, and named efforts expose only exactly declared
standard levels. Unknown names stay provider-managed; they are never mapped to
an unrelated effort. Missing metadata does not imply toggle support. Malformed
metadata is an error. The server default is not persisted as explicit intent.

`OllamaReasoningEffort` validates the selection before producing the compatible
API field. Default is omitted, Off maps to `none`, boolean On uses the documented
`medium` compatibility alias, and named efforts remain exact. It rejects budget
requests and unsupported efforts instead of relying on server fallback.
