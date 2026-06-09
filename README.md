# Floret

<p align="center">
  <strong>A Go library for building interactive, tool-using AI agents.</strong><br />
  <sub>Floret handles the agent loop; your application keeps the UI, permissions, storage choices, and domain tools.</sub>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret">
    <img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/floegence/floret.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="License" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go Version" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

<p align="center">
  <img alt="Agent loop" src="https://img.shields.io/badge/Agent-Loop-0f766e?style=for-the-badge" />
  <img alt="Bring your UI" src="https://img.shields.io/badge/Bring-Your%20UI-1d4ed8?style=for-the-badge" />
  <img alt="Fake providers" src="https://img.shields.io/badge/Fake-Providers-7c2d12?style=for-the-badge" />
</p>

<p align="center">
  <a href="#-why-floret">Why Floret</a> ·
  <a href="#-at-a-glance">At a glance</a> ·
  <a href="#-quick-start">Quick Start</a> ·
  <a href="#using-floret-in-a-host">Using Floret</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#-packages">Packages</a> ·
  <a href="#license">License</a>
</p>

Floret is a Go library for applications that need model streaming, tool execution,
session state, context management, storage, compaction, and runtime events. It is not
a graph workflow framework. It keeps the agent runtime small so your application can
decide how users see and control it.

## ✨ Why Floret

Most AI agent applications end up rebuilding the same plumbing: prompt assembly,
provider adapters, tool-call validation, approval hooks, session persistence, long
conversation handling, retries, usage metrics, and events that a UI can render.
Floret packages those pieces as small Go APIs.

- **Agent loop**: stream model output, continue after tool calls, enforce loop limits,
  and return a clear run result.
- **Tools**: register strict JSON-schema tools, ask for approval when needed, run safe
  read-only tools in parallel, and serialize mutating tools.
- **Threads**: start, resume, fork, retry, and recover interrupted conversations with
  `agentharness`.
- **Context**: estimate usage, keep a recent tail, and compact older history when a
  run exceeds the configured context policy.
- **Events**: send provider, tool, compaction, budget, and final-state events without
  tying them to a specific UI.
- **Tests**: use fake providers and scripted tool calls to test agent behavior without
  real model calls.

## 🧭 At a glance

| You need to... | Use... |
| --- | --- |
| Run one agent turn | `engine.Engine` for provider streaming, tool continuation, stop hooks, loop limits, and run metrics |
| Build durable conversations | `agentharness` for start, resume, fork, retry, waiting, and interruption recovery |
| Expose your own tools | `tools.Registry` for schemas, effects, permission hooks, scheduling, and result limits |
| Keep long sessions within budget | `session/contextpolicy`, `session/compaction`, and `engine/compaction` |
| Show progress in your UI | `event` records plus sanitized public observations |
| Test without model calls | `testing/harness`, `testing/eval`, and the fake provider adapter |

## 🧩 Core building blocks

| Building block | Purpose |
| --- | --- |
| `engine` | Low-level turn executor with provider streaming, tool execution, compaction triggers, and events |
| `agentharness` | Durable conversation API for threaded sessions, retries, forks, and lifecycle events |
| `provider` | Streaming provider request, response, usage, and tool-call contracts |
| `tools` | Tool registry, JSON schemas, resource references, approval checks, scheduling, result limits, and panic recovery |
| `session` / `sessiontree` | Provider-visible transcript shape plus append-only thread entries and active-path projection |
| `event` | Runtime events and a thread-safe recorder for tests |
| `runtime` / `runtime/storage` | Configuration helpers plus memory, file, prompt-cache, and SQLite-backed storage |

## Runtime Shape

Context compaction replaces older assistant/tool history with a structured
checkpoint while retaining a verbatim recent tail for execution continuity.
Recent user inputs outside that tail are protected inside the checkpoint: the
latest user message is always represented, and recent older user messages are
preserved within a 15k-token user-input budget without replaying them as new
standalone turns.

Most hosts should use `runtime.NewHarness`, then `StartThread` or `ResumeThread`,
and finally `Thread.Run` for each user turn. That API keeps durable conversation
state in `sessiontree.Repo` and serializes only the same durable `ThreadID`.
Different threads and forked threads can run concurrently.

The project is intentionally not a workflow graph engine. It is designed for interactive
agent hosts where a model, a tool registry, a session store, and a UI work around one
turn loop that emits events.

Events are UI-neutral so a terminal UI, desktop app, test harness, or automation surface
can render the same runtime facts without parsing human text.
Public views should expose sanitized events and observations. Raw provider deltas,
reasoning, tool arguments/results, prompt segments, and artifact paths are local
debug data and require explicit opt-in at the host boundary.

### Context Pressure

Floret keeps context-budget signals separate so hosts and storage do not have to
guess what a token number means:

- `provider.Usage` is native provider usage and cost data from a successful
  response. `WindowInputTokens` is the context-window input; `InputTokens` may be
  cost-normalized. Cached prompt reads and writes are still model-visible input
  for window pressure.
- `contextpolicy.RequestEstimate` is a preflight prediction for the actual
  provider request, including prefix messages, active messages, local tool
  definitions, and provider-hosted tool options. It is stored with provider
  request records, not as fake provider usage.
- `contextpolicy.ContextPressure` is the only structure that drives ordinary
  auto-compaction. It distinguishes native usage, projected request pressure,
  and provider overflow. Provider overflow compacts, rebuilds the request,
  re-estimates pressure, and retries the same logical request once.
- `contextpolicy.MessageContextEstimate` and `contextpolicy.Usage` are
  compaction-internal message budgets. They are used for retained tail and
  summary prompts, not for deciding whether an ordinary provider request can be
  sent.

After a successful ordinary request with native usage, Floret stores a pressure
anchor in the prompt cache. The next preflight projection can use that anchor plus
a request delta when the session, provider, model, rendered request shape, and
active message lineage still match; otherwise it falls back to a full request
estimate.

## 🚀 Quick Start

1. Clone the repository and run the test suite:

   ```bash
   go test ./...
   ```

2. Start the local agent console:

   ```bash
   go run ./cmd/floret-test-ui
   ```

3. Open `http://127.0.0.1:8765`.

The console can run fake-provider turns immediately, manage local provider profiles,
save the active profile to `.env.local`, inspect provider requests and stream events,
view session messages, review token/tool metrics, and run local checks such as package
tests, race tests, provider smoke tests, tool scenarios, and the deterministic eval demo.
New Session keeps context policy controls in Advanced options by default; those values
are derived from the active provider/model catalog and backend context defaults, while
still being submitted with the session create request.
Public API responses are sanitized by default. Local raw inspection requires an explicit
launch-time capability:

```bash
go run ./cmd/floret-test-ui -allow-debug-raw
```

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

Tip: start with the fake provider first. It lets you validate the host loop, tool
registry, storage, and UI wiring before adding any external model credentials.

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
FLORET_CONTEXT_WINDOW_TOKENS=256000
FLORET_MAX_OUTPUT_TOKENS=0
FLORET_RESERVED_OUTPUT_TOKENS=64000
FLORET_RESERVED_SUMMARY_TOKENS=20000
FLORET_RECENT_TAIL_TOKENS=12000
FLORET_RECENT_USER_TOKENS=15000
FLORET_WALL_TIME=30s
```

When `FLORET_MAX_OUTPUT_TOKENS` is omitted, a selected catalog model's `max_tokens` can provide the ordinary response cap. Setting `FLORET_MAX_OUTPUT_TOKENS=0` explicitly leaves ordinary assistant responses uncapped by Floret. The reserved output and summary settings are context-budget controls; they are not ordinary response caps.

Built-in catalog defaults target models with at least a 256000-token context window. Custom or newly released models are still allowed, including smaller-context configurations, but Floret may warn that they can behave poorly on long agent tasks.

Built-in provider IDs include:

```text
fake, openai, anthropic, google, moonshot, chatglm, deepseek, qwen,
openrouter, xai, groq, ollama, openai-compatible
```

Some provider entries, such as local or fast-changing OpenAI-compatible services, use a `custom-model` placeholder instead of predefined low-context model names. Set `FLORET_MODEL`, `FLORET_CONTEXT_WINDOW_TOKENS`, and related output budget values to match the model you intend to run.

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

The `runtime` package wires configuration, a provider adapter, prompt cache, system
prompt assembly, durable session storage, and a tool registry into an
`agentharness.AgentHarness`.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/sessiontree"
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

	h, err := floretruntime.NewHarness(cfg, floretruntime.HarnessOptions{
		Store: sessiontree.NewMemoryRepo(),
		Tools: registry,
	})
	if err != nil {
		log.Fatal(err)
	}
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{})
	if err != nil {
		log.Fatal(err)
	}

	result, err := thread.Run(ctx, "Say hello in one short sentence.", agentharness.RunOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Output)
}
```

For lower-level hosts, construct `engine.Engine` with `engine.New(engine.Config{...})`
using your own `provider.Provider`, `session.Store`, `tools.Registry`, `event.Sink`,
approver, stop hook, compaction generator, and engine options. Direct engine use is best
for tests, eval runners, and specialized hosts that already own session persistence.

## Threaded Agent Harness

`agentharness` builds on the engine for applications that need persistent threads
instead of isolated runs. It supports:

- Starting and resuming threads.
- Forking from an earlier session entry.
- Retrying failed or waiting turns.
- Marking interrupted turns when a process restarts.
- Moving a thread leaf after branch selection.
- Emitting harness lifecycle events alongside engine events.

The harness stores entries in a `sessiontree.Repo` and projects the active path into
engine context for each turn. `Thread.Read()` returns a host-safe snapshot with lifecycle
state and display messages. Raw session-tree data is available through `Thread.Journal()`
for tests, debug consoles, migrations, and admin tooling. This keeps thread operations
separate from provider and tool execution.

## Tools

Floret tools are registered through `tools.Registry` with strict JSON object schemas.
The registry validates arguments before execution, extracts resource references for
approval decisions, applies result limits, recovers from panics, and reports structured
tool results back into the engine. Tool definitions are part of the host contract: the
runtime owns validation, scheduling, permission hooks, and result shaping, while host
applications decide which domain tools exist and what those tools do.

Tool scheduling is owned by the registry:

- Read-only tools may run in parallel.
- Mutating tools are serialized.
- `PermissionAsk` tools call the host approver before execution.
- `PermissionDeny` tools are not exposed to the provider and are rejected before execution.
- Safe read-only tools may omit `Permission.Mode`; mutating, shell, network, destructive,
  and open-world tools must declare `Permission.Mode` explicitly.
- Open-world tools cannot use `PermissionAllow`; prefer `PermissionAsk` or
  `PermissionDeny` for high-risk tools unless the host has a narrower policy gate.

`tools/builtin` is an optional default tool set for local host applications. It includes
workspace reads (`read`, `list`, `glob`, `grep`), workspace mutations (`apply_patch`,
`write`), shell execution, and `web_search`, but those tools are examples and conveniences
rather than the whole project.

## Storage

Floret separates session message storage, session-tree storage, prompt-cache storage, and
host metadata:

- `session.NewMemoryStore` is the simplest in-memory store for isolated engine runs.
- `sessiontree.NewMemoryRepo` and `sessiontree.NewFileRepo` support threaded sessions.
- `provider/cache.NewMemoryStore` and `provider/cache.NewFileStore` store rendered prompt
  fragments and provider response records.
- `runtime/storage/sqlite.Open` provides one SQLite-backed store for session trees,
  prompt cache, metadata, and session deletion.

The local test UI defaults to SQLite at `.floret-test-ui/floret.db`. It can also run with
file or memory storage through `go run ./cmd/floret-test-ui --storage=file` or
`--storage=memory`.

## Architecture

Floret keeps runtime concerns separated from your application code:

```text
Your application
  UI / CLI / automation
  domain tools / permission policy / app state
        |
        v
agentharness or engine.Engine
        |
        +--> provider.Provider       model streaming and usage
        +--> tools.Registry          tool schemas, approvals, scheduling
        +--> session/sessiontree     messages, threads, forks, retries
        +--> provider/cache          provider-specific rendered context
        +--> session/contextpolicy   active context policy
        +--> event.Sink              UI-neutral runtime events
```

Important behavior is available through events or testable state. Hosts should render
events rather than parse assistant text to infer progress.
Provider-visible local tools always come from `tools.Registry`; host code should not
hand-write raw provider tool definitions to bypass registry validation or permission
policy.

## 📦 Packages

| Area | Packages | What they do |
| --- | --- | --- |
| Runtime core | `engine`, `provider`, `tools`, `event` | Run turns, define provider streams, execute tools, and emit events |
| Context and compaction | `session/contextpolicy`, `session/compaction`, `engine/compaction` | Track context usage and summarize older history |
| Host integration | `runtime`, `config`, `provider/adapters`, `provider/catalog`, `provider/cache` | Load config, create providers, track prompt fragments, and store provider response records |
| Optional capabilities | `tools/builtin`, `tools/mcp`, `tools/skills` | Add local tools, MCP tools, or skill disclosure when a host wants them |
| Sessions and storage | `agentharness`, `session`, `sessiontree`, `runtime/storage`, `runtime/storage/sqlite` | Store transcripts, thread trees, prompt cache data, and SQLite-backed runtime state |
| Testing and inspection | `testing/harness`, `testing/eval`, `cmd/floret-test-ui` | Script turns, run small evals, and inspect a local runtime |

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

Keep changes small, visible in events or tests, and easy to test without real model calls. Prefer focused
contracts over framework-style extension layers, and keep provider, context, tool runtime,
session storage, and host UI concerns separated.

Before proposing integration, run:

```bash
go test ./...
```

## License

Floret is licensed under the [MIT License](LICENSE).
