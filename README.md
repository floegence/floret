# Floret

Reusable runtime primitives for interactive AI chat and coding agents in Go.

Floret is a prompt-first agent runtime for hosts that need model streaming, tool
execution, session state, context management, and observable lifecycle events without
adopting a graph workflow or multi-agent orchestration framework. Hosts keep control
of UI, approvals, storage, provider choice, and domain-specific tools while Floret
handles the repeatable mechanics of an interactive agent loop.

Floret currently provides:

- A small, explicit engine loop with provider streaming, tool continuations, loop guards,
  compaction triggers, and normalized run results.
- Presentation-neutral events for provider requests, deltas, tool calls, tool results,
  compaction, budgets, and final run state.
- A tool registry with strict schemas, approval hooks, read-only parallel scheduling,
  mutation serialization, result limits, and panic recovery.
- Deterministic test harnesses for fake providers, scripted tool calls, evals, and host
  integration tests without real model calls.
- Threaded session primitives for start, resume, fork, retry, interruption recovery, and
  context projection over a session tree.
- Memory, file-oriented, and SQLite-backed storage options, plus prompt-cache rendering
  for provider-specific request shapes.
- Built-in provider adapters, an OpenAI-compatible chat completions adapter, a provider
  and model catalog, built-in workspace/shell/search tools, and a local runtime inspector.

## Why Floret

Most agent applications need the same hard-to-test runtime pieces: prompt assembly,
streaming provider adapters, tool-call validation, human approval, session persistence,
context pressure handling, retry/fork flows, usage metrics, and UI-friendly events.
Floret keeps those contracts small and separable so product code can focus on the host
experience.

The project is intentionally not a workflow graph engine. It is designed for interactive
chat and coding-agent hosts where a model, a tool registry, a session store, and a UI
surface cooperate around one observable turn loop.

## Quick Start

Clone the repository and run the test suite:

```bash
go test ./...
```

Start the local agent console:

```bash
go run ./cmd/floret-test-ui
```

Then open `http://127.0.0.1:8765`.

The console can run fake-provider turns immediately, manage local provider profiles,
save the active profile to `.env.local`, inspect provider requests and stream events,
view session messages, review token/tool metrics, and run local checks such as package
tests, race tests, provider smoke tests, tool scenarios, and the deterministic eval demo.

## Install / Import

Use Floret as a Go module:

```bash
go get github.com/floegence/floret
```

Floret targets Go 1.22.

## Provider Configuration

Floret reads `.env.local` by default through `config.Load`. The file is intentionally
ignored by git so local API keys and model choices stay private. Environment variables
override `.env.local`, which is useful for CI and one-off smoke tests.

A minimal fake-provider configuration:

```bash
FLORET_PROVIDER=fake
FLORET_MODEL=fake-model
FLORET_FAKE_RESPONSE=floret local provider ok
FLORET_RUN_ID=local
FLORET_SYSTEM_PROMPT=You are Floret.
```

Common context and runtime controls:

```bash
FLORET_CONTEXT_WINDOW_TOKENS=128000
FLORET_MAX_OUTPUT_TOKENS=4096
FLORET_RESERVED_OUTPUT_TOKENS=4096
FLORET_RESERVED_SUMMARY_TOKENS=2048
FLORET_RECENT_TAIL_TOKENS=12000
FLORET_WALL_TIME=30s
```

Supported provider IDs include:

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

For OpenAI:

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

## Using Floret in a Host

The `runtime` package wires configuration, a provider adapter, prompt cache, memory
manager, session store, and tool registry into an `engine.Engine`.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

type echoArgs struct {
	Text string `json:"text"`
}

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	registry := tools.NewRegistry()
	err = registry.Register(tools.Define[echoArgs](
		tools.Definition{
			Name:        "echo",
			Title:       "Echo",
			Description: "Return the supplied text.",
			InputSchema: tools.StrictObject(map[string]any{
				"text": tools.String("Text to echo."),
			}, []string{"text"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(ctx context.Context, inv tools.Invocation[echoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text}, nil
		},
	))
	if err != nil {
		log.Fatal(err)
	}

	store := session.NewMemoryStore()
	eng, err := floretruntime.NewEngine(cfg, store, registry)
	if err != nil {
		log.Fatal(err)
	}

	result := eng.Run(ctx, "Say hello in one short sentence.")
	if result.Err != nil {
		log.Fatal(result.Err)
	}
	fmt.Println(result.Output)
}
```

For lower-level hosts, construct `engine.Engine` directly with your own `provider.Provider`,
`session.Store`, `tools.Registry`, `event.Sink`, approver, stop hook, compaction generator,
and engine options.

## Threaded Agent Harness

`agentharness` builds on the engine for interactive products that need persistent threads
instead of isolated runs. It supports:

- Starting and resuming threads.
- Forking from an earlier session entry.
- Retrying failed or waiting turns.
- Marking interrupted turns when a process restarts.
- Moving a thread leaf after branch selection.
- Emitting harness lifecycle events alongside engine events.

The harness stores entries in a `sessiontree.Repo` and projects the active path into
engine context for each turn. This keeps product-level thread operations separate from
provider and tool execution.

## Tools

Floret tools are registered through `tools.Registry` with strict JSON object schemas.
The registry validates arguments before execution, extracts resource references for
approval decisions, applies result limits, recovers from panics, and reports structured
tool results back into the engine.

Tool scheduling is owned by the registry:

- Read-only tools may run in parallel.
- Mutating tools are serialized.
- `PermissionAsk` tools call the host approver before execution.
- `PermissionDeny` tools are rejected before execution.

`builtintools` includes workspace reads (`read`, `list`, `glob`, `grep`), workspace
mutations (`apply_patch`, `edit`, `write`), shell execution, and `web_search`.

## Storage

Floret separates session message storage, session-tree storage, prompt-cache storage, and
host metadata:

- `session.NewMemoryStore` is the simplest in-memory store for isolated engine runs.
- `sessiontree.NewMemoryRepo` and `sessiontree.NewFileRepo` support threaded sessions.
- `promptcache.NewMemoryStore` and `promptcache.NewFileStore` store rendered prompt
  fragments and provider response records.
- `sqlitestore.Open` provides one SQLite-backed store for session trees, prompt cache,
  metadata, and session deletion.

The local test UI defaults to SQLite at `.floret-test-ui/floret.db`. It can also run with
file or memory storage through `go run ./cmd/floret-test-ui --storage=file` or
`--storage=memory`.

## Architecture

Floret keeps runtime concerns separated:

```text
Host UI / CLI / automation
        |
        v
agentharness or engine.Engine
        |
        +--> provider.Provider       model streaming and usage
        +--> tools.Registry          tool schemas, approvals, scheduling
        +--> session/sessiontree     messages, threads, forks, retries
        +--> promptcache             provider-specific rendered context
        +--> memory/contextpolicy    system prompt and active context policy
        +--> event.Sink              presentation-neutral observability
```

Important behavior is observable through events or testable state. Hosts should render
events rather than parse assistant text to infer engine progress.

## Package Map

Runtime core:

- `engine`: explicit turn loop, status outcomes, loop guards, provider recovery, tool
  execution, compaction triggers, and event emission.
- `provider`: normalized streaming provider request and event contracts.
- `tools`: registry, schemas, approval checks, scheduling, result limits, and panic
  recovery.
- `event`: presentation-neutral lifecycle events and a thread-safe recorder for tests.
- `memory`, `contextpolicy`, `compaction`: prompt assembly and token-aware context control.

Host integration:

- `runtime`: configuration-to-engine assembly helpers.
- `config`: `.env.local` and environment variable loading.
- `adapters`: fake and provider adapters, including OpenAI-compatible chat completions.
- `builtintools`: workspace, shell, and search tools for local agent hosts.
- `modelcatalog`: built-in provider/model metadata and defaults.

Sessions and storage:

- `agentharness`: threaded agent runtime with resume, fork, retry, and lifecycle events.
- `sessiontree`: append-only thread entries and active-path projection.
- `session`: append/replace message storage with an in-memory implementation.
- `storage`: combined storage contract for session trees, prompt cache, and metadata.
- `sqlitestore`: SQLite-backed storage implementation.
- `promptcache`: rendered prompt fragments, toolset tracking, and provider response records.

Testing and evaluation:

- `harness`: deterministic scripted provider for engine and host tests.
- `eval`: lightweight task-eval runner with oracle checks and artifacts.
- `cmd/floret-test-ui`: local runtime inspector and smoke-test surface.

## Testing and Evals

Run the primary quality gate:

```bash
go test ./...
```

Run race-enabled tests:

```bash
go test -race ./...
```

The test suite covers deterministic agent loop behavior, provider usage aggregation,
trace redaction, OpenAI-compatible streaming tool arguments, registry-owned tool
scheduling, context/tool-result pairing, session-tree branching, SQLite persistence,
prompt-cache rendering, and a minimal oracle-based eval runner.

The local console also exposes quick checks for package tests, race tests, provider smoke
tests, tool scenarios, live tool scenarios, and the deterministic eval demo. Console
artifacts are written under `.floret-test-ui/`, which is ignored by git.

## Project Status

Floret is under active development. APIs may evolve while the runtime contracts settle,
especially around host integration, storage, provider-specific capabilities, and the
local inspector UI.

## Contributing

Keep changes small, observable, and easy to test without real model calls. Prefer focused
contracts over framework-style extension layers, and keep provider, context, tool runtime,
session storage, and host UI concerns separated.

Before proposing integration, run:

```bash
go test ./...
```

## License

A license has not been published yet.
