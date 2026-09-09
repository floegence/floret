# Model catalog maintenance

The engine embeds `internal/provider/catalog/models.generated.json`. No startup,
provider request, or Go test downloads model metadata. Provider credentials,
selection preferences, and discovery belong to the host.

Run `python3 scripts/model-catalog/generate.py --update` to explicitly fetch
models.dev. Review the filtered upstream snapshot and generated diff together.
The snapshot records the complete API response's SHA-256, so independent host
repositories can use the same source baseline without a local module dependency.
Use `--source /absolute/path/api.json` to import that exact downloaded response.
Run without arguments to regenerate offline; `--check` verifies reproducibility.
Python 3.9 or later and the standard library are sufficient.

The generator includes tool-capable text-output models. It excludes deprecated
models, media-only and specialized agent endpoints, and third-party models on
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
