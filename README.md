# Floret

Floret is a reusable Go engine for interactive AI agents. It owns the model
loop and the complete admitted Agent lifecycle: canonical messages, threads,
turns, runs, tools, approvals, todos, artifacts, context, SubAgents, recovery,
provider state, prompt cache, and observable execution facts.

The host application owns product UI, credentials, provider profiles, resource
authorization, routing, read state, uploads before admission, and transport
diagnostics. It must not persist or rebuild a second queryable Agent lifecycle.

Floret is not a graph workflow framework, a multi-agent orchestrator, or a
product persistence layer.

## Install

```bash
go get github.com/floegence/floret/v4@v3.2.8
```

Production integrations must resolve the published module. Do not use a local
`replace`, `go.work`, or sibling repository path. v1 and v2 remain available
only from their published tags; v3 has no legacy facade or runtime decoder.

## Quick Start

Every Agent uses one explicit `provider.Gateway`. Floret allocates durable
thread, turn, and run identities; an application supplies only a stable
`identity.LogicalRequestID` for each logical mutation. This production example
uses the official OpenAI-compatible Gateway and SQLite:

```go
package main

import (
    "context"
    "os"

    "github.com/floegence/floret/v4/config"
    "github.com/floegence/floret/v4/provider"
    "github.com/floegence/floret/v4/runtime"
    "github.com/floegence/floret/v4/storage"
)

func main() {
    ctx := context.Background()
    gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
        Provider: "openai", Model: "gpt-4.1-mini",
        BaseURL: "https://api.openai.com/v1", APIKey: os.Getenv("OPENAI_API_KEY"),
        StateCompatibilityKey: "openai:gpt-4.1-mini:chat-completions:v1",
        Capabilities: provider.Capabilities{
            Reasoning: provider.ReasoningUnsupported,
            AttachmentPayload: provider.AttachmentDescriptors,
        },
    })
    if err != nil { panic(err) }
    agent, err := runtime.NewAgent(config.AgentConfig{
        Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
        SystemPrompt: "Answer clearly and concisely.",
        Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
    }, gateway)
    if err != nil {
        panic(err)
    }

    host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("floret.db")})
    if err != nil {
        panic(err)
    }
    defer func() {
        if err := host.Shutdown(context.Background()); err != nil {
            panic(err)
        }
    }()

    created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
        LogicalRequestID: "create-conversation-42",
    })
    if err != nil {
        panic(err)
    }
    thread, err := host.Thread(ctx, created.ThreadID)
    if err != nil {
        panic(err)
    }
    executor, err := thread.TurnExecutor(agent)
    if err != nil {
        panic(err)
    }
    command := runtime.StartTurnCommand{
        LogicalRequestID: "send-message-42",
        UserMessage:      runtime.TurnInput{Text: "Hello"},
    }
    _, err = executor.StartTurn(ctx, command)
    if err != nil {
        panic(err)
    }
}
```

Run it with `OPENAI_API_KEY=... go run ./cmd/examples/openai-sqlite`. The complete
example also reads the authoritative assistant projection. `florettest` remains
test-only; use it for deterministic provider scripts and `florettest.NewIDSource`
for deterministic lifecycle identities.

## Public Packages

| Package | Responsibility |
| --- | --- |
| `identity` | Thread, turn, run, prompt-scope, trace, logical-request, and artifact identities |
| `config` | Provider-neutral Agent profile, prompt, context, and reasoning policy |
| `runtime` | Immutable Agent construction, durable Host lifecycle, commands, queries, and subscriptions |
| `observation` | Sanitized runtime events and host-facing projections |
| `tools` | Local tool definitions, permissions, resources, effects, and results |
| `provider` | Model Gateway contract and official provider constructors |
| `storage` | Opaque storage values and official memory and SQLite constructors |
| `storage/spi` | Advanced physical storage implementation contract |
| `florettest` | Scripted gateways and public conformance suites for tests only |

Ordinary applications use `identity`, `config`, `runtime`, `observation`,
`tools`, the official `provider` constructors, and opaque `storage.Source`
values. Custom provider transports and physical storage implementations are
advanced integration surfaces with separate conformance suites. Downstream
code must never import `internal/*`.

Tool definitions may provide `InvalidActivity` when a host needs to preserve a
safe display label from a parseable JSON object that fails the input schema.
Floret uses that callback only for presentation: the invalid invocation still
fails closed before permission, effect dispatch, and handler execution.

Question activity may include host-authored answer summaries for completed
prompts. A secret answer is represented only by `Redacted: true`; Floret
rejects a redacted answer that also carries values. These fields are display
data, not an alternate input-response or durable message authority.

## Runtime Boundary

`runtime.Host` belongs in the composition root. `Host.ThreadService` returns the
single typed lifecycle boundary. Its `Create`, `Fork`, `Delete`, `SetTitle`,
`List`, `View`, `History`, `Send`, `Respond`, `Cancel`, `Retry`, `RetryEffect`,
queue, import, and `Subscribe` methods all operate on stable thread and request
identities. Child agents are ordinary child threads with explicit parent
identity, so they use the same current-view and command contracts.

Each thread has one in-memory runtime owner. `Send` first commits canonical turn
acceptance, then publishes and returns the user item and active current view
before provider dispatch. The canonical journal is the only durable fact
source. Provider and tool I/O execute outside the thread lock.
`Cancel` is idempotent for every known thread and fences late provider output.
`Respond` resolves the matching approval or input interaction in place. An
uncertain effect is never replayed automatically; `RetryEffect` requires an
explicit risk acknowledgement and remains attached to the original tool row.

`runtime.NewAgent` snapshots the resolved Agent profile, system prompt,
Gateway, tools, capabilities, reasoning policy, and execution policy. The
effective snapshot and continuation state used by each run are Floret-owned
durable facts. Provider credentials and editable profile sources remain in the
host.

Current views contain one Floret-ordered sequence of directly renderable user,
thinking, assistant, tool, and interaction items, plus pending interactions and
the accepted queue. Each item has a stable ID and ordinal; live deltas grow the
same item in place, and tool approval, dispatch, and result state update the
original tool item. Canonical reload derives the same sequence without a
presentation ledger. `AssistantDraft` and `ThinkingDraft` remain deprecated v4
wire fields derived from the active item and are not a second ordering source.
`ViewVersion` is process-local notification ordering, not a durable replay
cursor. Production hosts leave `runtime.Options.IDSource` nil; deterministic
identity injection belongs to `florettest.NewIDSource`.

## Consistent Reads

`ThreadService.View` returns one complete replaceable current view.
The value returned by `Host.ThreadService` also implements
`ThreadContextReader`; `Context` returns Floret's canonical context policy,
usage, and one latest lifecycle record per compaction operation, including
terminal state restored after runtime restart.
`ThreadService.Subscribe` publishes workspace summary and current-view updates;
reconnecting clients refresh summaries and the currently visible view. There is
no durable cursor, replay ledger, materialized projection, or second lifecycle
authority.

## Storage

For ordinary hosts, `storage.Source` is an opaque value consumed exclusively by
`runtime.Open`:

```go
runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("agent.db")})
```

Applications cannot use a Source as a lifecycle query path. Teams implementing
a physical backend use the advanced `storage/spi` contracts and their
conformance suite. SPI records remain opaque Floret data; a backend must not
decode them into a second Agent model. Memory, SQLite, and third-party backends
all run the same Floret-owned domain kernel.

## Source Of Truth

Floret exclusively owns admitted messages and references, thread/turn/run
lifecycle, titles, approvals, todos, tool invocation and outcome, pending-work
settlement, artifacts, control signals, context and compaction, provider
ledgers and state, prompt cache, SubAgent hierarchy, and Activity projections.

Hosts may persist product authorization and audit, routing, credentials,
editable persona sources, resource catalogs, read state, unadmitted commands,
upload staging, and transport diagnostics. Those records must not contain a
serialized Floret DTO or support reconstruction of Agent state. Canonical
message references are opaque durable facts; rich material needed only for the
current provider turn belongs in `SupplementalContext` and never becomes
conversation history or continuation state.

## Shutdown

`Host.Shutdown(ctx)` stops admission, cancels Host-managed provider and tool
execution, waits for it to finish, and then closes storage. If the context
expires, Shutdown returns `ctx.Err()` and Host remains closing; a later call
continues waiting. After completion, every retained handle returns
`ErrHostClosed`.

## Development

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go test -race ./...
scripts/check_candidate_release_adoption.sh
```

Repository workflow and compatibility rules are defined in [AGENTS.md](AGENTS.md).
Architecture and maintenance knowledge lives in [okf/](okf/).
