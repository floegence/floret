# Floret

Floret is a reusable runtime for interactive AI chat and coding agents.

The Stage 1 implementation focuses on the engine core rather than host-specific shell,
filesystem, terminal, or UI integrations. Hosts provide model adapters, tools, storage,
approval decisions, and presentation surfaces through small contracts.

## Package Map

- `engine`: explicit turn loop, status outcomes, loop guards, provider recovery, tool execution, and event emission.
- `provider`: normalized streaming provider request and event contracts.
- `tools`: tool registry, approval checks, read-only parallel scheduling, mutation serialization, and panic recovery.
- `memory`: context assembly and compaction with tool-result pair preservation.
- `session`: append/replace message storage with an in-memory implementation.
- `event`: presentation-neutral lifecycle events and a thread-safe recorder for tests.
- `harness`: deterministic scripted provider for engine and host tests.
- `config`: `.env.local` and environment variable loading.
- `adapters`: provider adapters, including fake and OpenAI-compatible chat completions.
- `runtime`: configuration-to-engine assembly helpers for hosts.
- `eval`: lightweight task-eval runner with oracle checks and artifacts.

## Runtime Shape

The engine is intentionally small and explicit:

1. Append the user message.
2. Assemble bounded provider context.
3. Stream provider output.
4. Recover from empty, truncated, or context-overflow responses.
5. Treat a terminal provider response with assistant text and no pending tools as natural completion.
6. Treat `ask_user` as the explicit user-interrupt signal.
7. Execute normal tools through the registry and feed results back into the next step.
8. Stop through loop guards for max steps, cancellation, no progress, repeated tools, truncation, and duplicate tool call IDs.

Events are presentation-neutral so a terminal UI, desktop app, test harness, or automation
surface can render the same runtime facts without parsing human text.

## Local Provider Configuration

Floret reads `.env.local` by default through `config.Load`. The file is intentionally ignored
by git so local API keys and model choices stay private.

Common variables:

```bash
FLORET_PROVIDER=fake
FLORET_MODEL=fake-model
FLORET_FAKE_RESPONSE=floret local provider ok
FLORET_RUN_ID=local
FLORET_SYSTEM_PROMPT=You are Floret.
FLORET_MAX_CONTEXT_MESSAGES=32
FLORET_MAX_STEPS=16
FLORET_HARD_MAX_STEPS=16
FLORET_WALL_TIME=30s
```

Floret includes a built-in provider/model catalog inspired by the Flower settings catalog
and mature coding-agent registries. `FLORET_PROVIDER` may be one of:

```text
fake, openai, anthropic, google, moonshot, chatglm, deepseek, qwen,
openrouter, xai, groq, cerebras, mistral, together, fireworks,
vercel-ai-gateway, ollama, openai-compatible
```

When `FLORET_MODEL` is omitted, Floret uses the catalog default for that provider. When
`FLORET_BASE_URL` is omitted, Floret uses the provider default endpoint. `FLORET_API_KEY`
always works, and provider-specific environment variables such as `OPENAI_API_KEY`,
`ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `DEEPSEEK_API_KEY`, `DASHSCOPE_API_KEY`,
`MOONSHOT_API_KEY`, and `OPENROUTER_API_KEY` are also recognized.

Example:

```bash
FLORET_PROVIDER=openai
OPENAI_API_KEY=your-api-key
```

For a custom OpenAI-compatible gateway:

```bash
FLORET_PROVIDER=openai-compatible
FLORET_MODEL=your-model
FLORET_BASE_URL=https://api.example.com/v1
FLORET_API_KEY=your-api-key
```

Environment variables override `.env.local` values, which makes CI and one-off smoke tests
easy to run without editing the local file.

## Verification

Run:

```bash
go test ./...
go test -race ./...
```

The test suite covers deterministic agent loop behavior, provider usage aggregation,
trace redaction, OpenAI-compatible streaming tool arguments, registry-owned tool
scheduling, context/tool-result pairing, and a minimal oracle-based eval runner.

## Local Agent Console

Start the visual agent console:

```bash
go run ./cmd/floret-test-ui
```

Then open `http://127.0.0.1:8765`.

The console is an interactive runtime inspector. It can manage one or more local
provider profiles, save the active profile to `.env.local`, run a single agent turn
from a first user message, and show the engine state transitions, provider requests,
provider stream events, session messages, final output, and token/tool metrics.

The sidebar also exposes quick local checks for `go test`, `go test -race`, and the
deterministic eval demo. Live provider runs use the selected profile and can make
real model calls when the active provider is not `fake`.

Console eval artifacts are written under `.floret-test-ui/`, which is ignored by git.
