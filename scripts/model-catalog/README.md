# Model catalog maintenance

The engine embeds `internal/provider/catalog/models.generated.json`. No startup,
provider request, or Go test downloads model metadata. Provider credentials,
selection preferences, and discovery belong to the host.

Run `python3 scripts/model-catalog/generate.py --update` to explicitly fetch
models.dev. Review the filtered upstream snapshot and generated diff together.
The snapshot records the complete API response's SHA-256, so independent host
repositories can use the same source baseline without a local module dependency.
Use `--source /absolute/path/api.json` to import that exact downloaded response.
Use `--providers moonshot` with `--source` or `--update` for a scoped refresh.
Its raw source hash is recorded in `provider_sources`; unrelated provider data
and review dates stay unchanged. The top-level hash identifies the base snapshot.
Run without arguments to regenerate offline; `--check` verifies reproducibility.
Python 3.9 or later and the standard library are sufficient.

The generator includes tool-capable text-output models. It excludes deprecated
models (except still-callable official DeepSeek aliases), media-only and specialized agent endpoints, and third-party models on
the Qwen endpoint. Public preview models retain a lifecycle label. Input
modalities are restricted to text and image, which the host adapters understand.
Model metadata does not expand the attachment contract of the built-in public
adapters: hosts still supply prepared, expanded payloads through their Gateway.

`overrides.json` contains the review date, official sources, and exact corrections.
Reasoning options are mapped only through known provider protocol shapes; a
reasoning boolean alone does not enable an effort selector. Review newly added
reasoning options against the linked official documentation and add request
fixtures when the wire protocol changes. Token and price metadata originate in
models.dev unless corrected; an output limit in that source is not a claim of an
independently verified official maximum. Flat cost estimates do not model price
tiers.

The current exclusions remove retired Groq Llama models, xAI retired redirects,
Grok Build (a separate product), and Codex Spark (not a generally available API
model). Gemini Live, Deep Research, computer-use-only endpoints, and safeguard
models need other protocols and are excluded by purpose. Kimi K3 always thinks,
despite the upstream toggle flag. GLM 5.2/5.3 effort levels come from the native
Z.ai endpoint, not the broader DashScope proxy effort enum.

OpenRouter and Ollama have no embedded selectable model list. Hosts discover
OpenRouter models or the configured Ollama instance's installed tool-capable
models. Custom endpoints remain explicit host configuration. Catalog changes do
not choose a host's default or replace a conversation's current model.

The upstream data is MIT licensed; see `models-dev.LICENSE`.

The 2026-10-12 snapshot adds Claude Haiku 5.5 with adaptive thinking and corrects
Sonnet 4.5 to its current 200K context capacity. All nine embedded provider
inventories were reviewed; host choices and thread identities remain unchanged.
The snapshot includes current Sol/Luna, Claude 5.5, GLM FlashX,
Qwen Flash/Omni, Grok 4.7, and DeepSeek Flash metadata. GPT-6.1 Sol requires
Responses for tools and is excluded from Floret's built-in Chat transport;
hosts with a qualified Responses gateway may expose it independently.
Daybreak models require separate approval/provisioning and are excluded.
DeepSeek alias overrides preserve existing conversation IDs and explicitly clear
retired experimental labels. Explicit lifecycle overrides take precedence over
name-based inference. DeepSeek flat prices are off-peak rates, not peak-hour
billing estimates. The [official pricing page](https://api-docs.deepseek.com/quick_start/pricing)
owns current aliases and pricing; output metadata uses the pinned 393,216-token
value for the documented 384K ceiling.

The 2026-10-24 Moonshot-only refresh adds `kimi-k3.1-pro` and
`kimi-k3.1-pro-highspeed` from the exact source also reviewed by Redeven. The
[official K3.1 guide](https://platform.kimi.com/docs/guide/kimi-k3-1-pro-quickstart)
confirms K3-compatible reasoning, fixed sampling controls and
`max_completion_tokens`. Runtime rendering follows the reviewed capability,
not a single model-name exception. This refresh preserves all other inventories.
Run `python3 scripts/model-catalog/test_generate.py` to verify scoped provenance.

The 2026-11-10 full refresh adds Claude Fable 5.2 (always-on adaptive thinking,
default high), Gemini 4 Argon/Carbon and the preview `gemini-pro-latest` alias.
Gemini 4 uses a shared 1,048,576-token window and low/medium/high thinking.
The catalog retains the conservative 32,768-token foreground non-streaming
output ceiling; 65,536 streaming output and long decode need separate transport
qualification. See Google's [thinking limits](https://ai.google.dev/gemini-api/docs/thinking).
DeepSeek models and their official aliases now share the source's 1,048,576-token
window; their 393,216-token output ceiling is unchanged. Groq Qwen 3.8's window
is corrected to 131,072. GLM 5.2 Off uses `thinking.type: disabled`, not a `none`
effort. Grok 4.3/4.5 retain their reviewed effort levels: the official API treats
`xhigh` as `high`, so the catalog must not advertise it as an independent level.

The refreshed source marks Sonnet 4.5 and older Qwen models deprecated or removes
them. They leave the selectable inventory without replacing saved model IDs.
OpenRouter and Ollama remain discovery-owned. GPT-6.1 Terra, like GPT-6.1 Sol,
requires Responses for tools and is excluded from the built-in OpenAI Chat list.
The earlier GPT-6 Astra/Sol/Luna entries are retained as existing metadata, not
new transport qualification: [OpenAI's current guide](https://developers.openai.com/api/docs/guides/latest-model)
requires Responses for Astra tools and for Sol/Luna tools with reasoning enabled.
Hosts must enforce their gateway's supported protocol when exposing these IDs.
[Qwen4 Max Preview](https://www.alibabacloud.com/help/en/model-studio/token-plan-personal-overview)
is a limited Token Plan subscription preview, not the ordinary DashScope API
inventory. It is not added to the general-purpose provider catalog.
