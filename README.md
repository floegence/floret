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
5. Treat `task_complete` as the explicit completion signal.
6. Treat `ask_user` as the explicit user-interrupt signal.
7. Execute normal tools through the registry and feed results back into the next step.
8. Stop through loop guards for max steps, cancellation, no progress, repeated tools, and duplicate tool call IDs.

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

For OpenAI-compatible providers:

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
