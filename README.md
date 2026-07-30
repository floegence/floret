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
go get github.com/floegence/floret/v3@v3.0.0
```

Production integrations must resolve the published module. Do not use a local
`replace`, `go.work`, or sibling repository path. v1 and v2 remain available
only from their published tags; v3 has no legacy facade or runtime decoder.

## Quick Start

Every Agent uses one explicit `provider.Gateway`. Floret allocates durable
thread, turn, and run identities; an application supplies only a stable
`identity.LogicalRequestID` for each logical mutation.

```go
package main

import (
    "context"
    "fmt"

    "github.com/floegence/floret/v3/config"
    "github.com/floegence/floret/v3/florettest"
    "github.com/floegence/floret/v3/provider"
    "github.com/floegence/floret/v3/runtime"
    "github.com/floegence/floret/v3/storage"
)

func main() {
    ctx := context.Background()
    gateway := florettest.NewScriptedGateway(
        provider.Identity{
            Provider: "example", Model: "deterministic",
            StateCompatibilityKey: "example:deterministic:v1",
        },
        provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
        florettest.Step{Events: []provider.Event{
            {Type: provider.EventDelta, Text: "Hello from Floret v3."},
            {Type: provider.EventDone, Reason: "stop"},
        }},
    )
    agent, err := runtime.NewAgent(config.AgentConfig{
        Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
        SystemPrompt: "Answer clearly and concisely.",
        Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
    }, gateway)
    if err != nil {
        panic(err)
    }

    host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
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
    turns, err := thread.Turns(agent)
    if err != nil {
        panic(err)
    }
    started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
        LogicalRequestID: "send-message-42",
        UserMessage:      runtime.TurnInput{Text: "Hello"},
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(started.ThreadID, started.TurnID, started.RunID)
}
```

`florettest` is test-only. Production applications normally construct a
gateway with `provider.NewOpenAICompatible`, `provider.NewAnthropic`, or a
custom `provider.Gateway`.

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

## Runtime Boundary

`runtime.Host` belongs in the composition root. Its public entry points are
`Open`, `Host.Threads`, `Host.Thread`, recovery handles, and
`Host.Shutdown(ctx)`. A `Thread` binds one exact `identity.ThreadID`; its
`Turns`, `SubAgents`, child, and descendant capabilities do not repeat that
authority in each command.

`Thread.Child` binds direct-child read authority for canonical detail and
pending tool targets. `Thread.DescendantReader` binds one validated descendant
at any depth for turn pages, exact turn reads, and artifacts. Neither handle
accepts another thread identity after binding, and unrelated, deleted, or
corrupt ancestry fails closed.

Commands use stable logical request identities and explicit names:
`CreateThread`, `StartTurn`, `RetryTurn`, `ForkThread`, `DeleteThread`,
`ContinuePendingTool`, `RecordPendingToolOutcome`, `ResolveApproval`,
`UpdateTodos`, `SpawnSubAgent`, `SendSubAgentMessage`, and
`InterruptSubAgent`. Floret allocates all lifecycle identities. Replaying the
same logical request under the same operation and bound authority returns the
original identities; changing durable input returns a typed request conflict.

`runtime.NewAgent` snapshots the resolved Agent profile, system prompt,
Gateway, tools, capabilities, reasoning policy, and execution policy. The
effective snapshot and continuation state used by each run are Floret-owned
durable facts. Provider credentials and editable profile sources remain in the
host.

## Consistent Reads

Every exact thread has one monotonic `runtime.ThreadRevision`. A reconnecting
consumer uses one fixed handshake:

1. Read `Thread.Snapshot` and retain revision `R`.
2. Load every initial turn, approval, todo, pending-work, artifact, and
   SubAgent page with `AtRevision=R`.
3. Call `Thread.Subscribe` with `AfterRevision=R`.

Page cursors bind thread, revision, direction, and position. Page limits are
1 through 200. If a revision is no longer readable, Floret returns
`ErrRevisionUnavailable`; the consumer obtains a new snapshot instead of
silently switching to current state.

`Thread.Subscribe` observes only the exact bound thread. It is a linearized
pull protocol through `Subscription.Next(ctx)`, not a callback or bare channel.
Durable revision events tell consumers which canonical domain to query.
Provider, token, and tool progress is transient. On queue overflow the
subscription yields one Gap with its last delivered and resync revisions, then
returns `ErrSubscriptionStale` until the consumer repeats the snapshot/query/
subscribe handshake. Parent streams contain child publication and close facts,
not child execution events.

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
