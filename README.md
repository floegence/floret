# Floret

<!-- readme-locales:start -->
<p align="center">
  <strong>English</strong> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <a href="README.ko-KR.md">한국어</a> |
  <a href="README.de-DE.md">Deutsch</a> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>The agent runtime that leaves your product in your hands.</strong><br />
  Durable conversations, tool execution, context lifecycle, and observable runtime facts for Go applications.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="License" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go Version" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

<p align="center">
  <a href="#-why-floret">Why Floret</a> ·
  <a href="#-at-a-glance">At a glance</a> ·
  <a href="#what-you-keep">What You Keep</a> ·
  <a href="#quick-start">Quick Start</a> ·
  <a href="#production-shape">Production Shape</a> ·
  <a href="#-downstream-integration-surface">Integration Surface</a>
</p>

![Floret AI agent app runtime](okf/assets/readme/floret-agent-app-whiteboard.png)

## ✨ Why Floret

Most agent libraries help a model call a tool. Shipping a serious agent product
requires much more: a resilient model loop, a durable conversation record,
approval-aware tool execution, long-context management, recoverable work, and
runtime facts your UI can trust.

Floret provides that runtime without taking over the product around it. It is
not an agent UI, a workflow graph, or a multi-agent framework. It is the Go
layer behind the experience you are building.

That distinction matters when your product needs to be genuinely its own. You
can keep your interface, identity system, permission model, model routing,
secrets, data model, and domain tools while delegating the difficult agent
execution lifecycle to Floret.

### Built for products that cannot accept a preset

- **Bring your model path.** Use built-in configuration or provide a
  `runtime.ModelGateway`; Floret drives the request and continuation lifecycle
  while your product retains transport and credential control.
- **Give every agent a business-native role.** Set
  `config.AgentProfile.SystemPrompt` or `config.Config.SystemPrompt` to define
  the role, voice, business scenario, and operating rules your product needs,
  rather than shipping a generic assistant.
- **Keep tools and instructions in step with the work.** Register strict domain
  tools with `tools.Registry`, then use `runtime.ToolSurfaceProvider` to
  refresh tools, hosted capabilities, instructions, and host context at safe
  points during a run.
- **Keep conversations durable.** The Floret runtime manages threads, turns,
  retries, forks, parent-managed child threads, and provider-safe history.
- **Put approval policy where it belongs.** Floret understands generic effects,
  resources, and approval state. Your product decides who may do what, where,
  and why.
- **Make runtime behavior visible.** Stream sanitized events, context pressure,
  compaction facts, and neutral activity timelines into any UI without exposing
  prompts, secrets, or internal storage records.
- **Test the product, not a model.** The fake provider makes agent flows
  deterministic in local and CI tests.

## 🧭 At a glance

| You need to... | Use... |
| --- | --- |
| Configure an agent and a provider | `config.Config` or `config.Load` |
| Run durable conversations | `runtime.NewTurnExecutionHostBinder` |
| Compact an idle thread | `runtime.NewThreadCompactionHostBinder` with `runtime.CompactThreadRequest` |
| Manage interactive child threads | `runtime.NewSubAgentHostBinder` |
| Recover one exact interrupted turn | `runtime.NewInterruptedTurnRecoveryHostBinder` |
| Reload canonical context state | `runtime.NewThreadReadHostBinder` |
| Keep Floret runtime data in memory or SQLite | `runtime.NewMemoryStore` or `runtime.OpenSQLiteStore` |
| Keep model transport under product control | `runtime.ModelGateway` |
| Define an agent's role and business instructions | `config.AgentProfile.SystemPrompt` or `config.Config.SystemPrompt` |
| Change tools and instructions during a run | `runtime.ToolSurfaceProvider` |
| Register domain actions | `tools.Registry` |
| Render neutral runtime facts | `runtime.EventSink` and `observation` DTOs |

## What You Keep

Floret has a deliberately narrow boundary. It owns engine mechanics; your
application owns every product decision.

| Floret runs | Your application decides |
| --- | --- |
| Provider loop, retries, tool continuation, and finish state | When users can start, retry, interrupt, or cancel work |
| Durable thread journal, prompt scope, provider ledger, opaque continuation, and runtime artifacts | Users, workspaces, billing, product metadata, and retention policy |
| Tool schema validation, generic effect metadata, approval lifecycle, and result projection | Authorization, approval UX, domain actions, and user-facing copy |
| Context pressure, compaction lifecycle, and provider-visible history | What product data is safe to supply and how it appears in the interface |
| Sanitized events and neutral activity facts | Layout, workflows, controls, routing, and diagnostics policy |

This is what lets an operations console, a coding environment, a support tool,
or an industry-specific assistant share a dependable runtime without becoming
the same product.

## Quick Start

Install the downstream packages:

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

Start a durable thread with the deterministic fake provider:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/runtime"
)

func main() {
	ctx := context.Background()

	store := runtime.NewMemoryStore()
	defer store.Close()
	var createBinder *runtime.ThreadCreateHostBinder
	var turnBinder *runtime.TurnExecutionHostBinder
	err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
		var err error
		createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
		if err != nil {
			return err
		}
		turnBinder, err = runtime.NewTurnExecutionHostBinder(bootstrap)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	createIntentID := runtime.CreateIntentID("create-thread-1")
	threadCreator, err := createBinder.Bind("thread-1", createIntentID)
	if err != nil {
		log.Fatal(err)
	}
	turnFactory, err := turnBinder.Bind("thread-1")
	if err != nil {
		log.Fatal(err)
	}
	thread, err := threadCreator.CreateThread(ctx, runtime.CreateThreadRequest{
		ThreadID: "thread-1", CreateIntentID: createIntentID,
	})
	if err != nil {
		log.Fatal(err)
	}
	turnHost, err := turnFactory.NewHost(ctx, runtime.TurnExecutionHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "Hello from Floret.",
			AgentProfile: config.AgentProfile{
				ID:           "support-agent",
				Name:         "Support Agent",
				SystemPrompt: "Answer clearly and briefly.",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := turnHost.RunTurn(ctx, runtime.RunTurnRequest{
		ThreadID: thread.ID,
		TurnID:   "turn-1",
		RunID:    "run-1",
		Input: runtime.TurnInput{
			Text: "Welcome a new customer in one sentence.",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Output)
}
```

Replace the fake configuration with an OpenAI-compatible gateway or a
host-supplied `runtime.ModelGateway` when your product owns model transport.
When Floret should persist its own runtime data, inspect the Store first.
Missing or empty stores can be opened with that initialize-only state. Current
stores must be verified. Upgradeable stores must go through an explicit
`MigrateSQLiteStore` apply and then be verified again. Pass the final
verification's state and observed schema to `runtime.OpenSQLiteStore(ctx, path,
request)`; open never migrates implicitly. Your product data stays in your own
store, keyed by `runtime.ThreadID`.
The caller owns the runtime Store, may share it across runtime facades, and
closes it once after all active work has stopped. Runtime facades never close an
injected Store.

For a production-shaped integration, start with the runnable
[minimal durable host](cmd/examples/minimal-durable-host), then use the focused
[examples](cmd/examples) for a custom model gateway, effect approval, message
references, SubAgents, startup recovery, and Store maintenance. Integration
tests can use the public deterministic helpers in
[`florettest`](florettest). Operators can inspect, verify, plan, and explicitly
apply SQLite maintenance through the
[Store maintenance example](cmd/examples/store-maintenance-host) or the
[`floret-store` command](cmd/floret-store).

Floret's journal is the only durable source for admitted user input, assistant
output, turn/run lifecycle, control signals, approvals, projections, and Agent
todos. Use `ListThreadTurns` for ordered bootstrap and pagination, `ReadThread`
for transcript-free thread lifecycle metadata, and the typed Agent todo CAS
methods for `write_todos` state. A host may keep product metadata and commands
that Floret has not admitted yet, but it must not copy these Agent facts into a
second conversation or run database.

## Production Shape

### Let prompts carry product intent

Floret does not prescribe a generic persona. Give an agent its initial role,
voice, business scenario, and operating rules through
`config.AgentProfile.SystemPrompt` or `config.Config.SystemPrompt`. The prompt
is host-owned product configuration, so different agents can behave like a
support specialist, an operations analyst, a coding assistant, or a domain
expert without changing the runtime.

For context-dependent behavior, return a `runtime.ToolSurface` from a
`ToolSurfaceProvider`. It can replace the current system prompt alongside the
tool surface, hosted capabilities, and host context. This is useful when a
conversation moves between product modes, workspaces, permissions, or business
stages. Floret refreshes that surface before model requests and local dispatch,
so an old model decision cannot silently run under newer instructions or
product policy.

### Give the runtime only the authority it needs

Define domain actions through `tools.Registry`. Each tool has a strict JSON
schema and can describe its effects and resources. Floret validates the call,
records its generic approval lifecycle, and routes effectful dispatch through
the host's `runtime.EffectAuthorizationGate` before invoking the handler. The
handler must still enforce your authorization rules. The gate invokes the
one-shot `AuthorizedEffect` with the execution context selected by the host;
Floret also binds that context to the active turn lifetime, so neither a longer
host context nor a retained callback can outlive canonical turn cancellation.

| Tool concern | Floret handles | Host handles |
| --- | --- | --- |
| Schema | strict provider-visible JSON shape | domain argument meaning |
| Permission | effect metadata and durable approval lifecycle | product authorization and approval policy through `EffectAuthorizationGate` |
| Execution | scheduling, panic recovery, and result projection | the domain action itself |
| Output | model projection, neutral activity, and artifact references | product-specific display choices |

### Design around explicit identity

Keep Floret's runtime identities in product work records:

- `ThreadID` identifies the durable conversation.
- `TurnID` identifies one user-facing turn.
- `RunID` identifies one concrete provider execution.
- `PromptScopeID` identifies prompt-cache and provider-ledger reuse.
- `ForkOperationID` identifies one replayable durable thread-fork operation.

They are intentionally separate. For example, a host-owned process that later
settles pending tool work must use the recorded Floret `ThreadID`, `TurnID`, and
`RunID`, rather than a UI, audit, or display identifier.

When a host cancels its exact `RunTurn` context, an exact terminal `TurnResult`
means Floret has committed the canonical terminal outcome and released active
turn authority. That valid terminal result is the handoff point for settling
remaining host-owned pending work through the bound `PendingToolRecoveryHost`.
If execution returns without a valid exact terminal result, the host must first
confirm the exact terminal turn through the public canonical read API. Hosts
must not poll or retry around `ErrThreadBusy` before either proof exists.

Thread forks require a host-supplied `ForkOperationID`. Floret saves the source
leaf, destination identities, terminal child-thread plan, and turn/run mappings
before creating any target. Repeating the same operation resumes missing plan
nodes or returns the stored result; reusing it for a different request, finding
an unrelated destination, or losing a completed target fails explicitly.

### Render facts, not engine internals

Send a `runtime.EventSink` to receive sanitized lifecycle events and use the
`observation` DTOs for context pressure, compaction, and activity timelines.
Events are designed for host rendering and diagnostics; they are not a secret
store or a replacement for your product database.

Public lifecycle fields use finite types such as `observation.EventType`,
`ContextPhase`, `ContextDisplayStatus`, `CompactionPhase`, and
`CompactionStatus`. Normalized finish, completion, and continuation reasons are
also finite public fields. Raw provider finish text remains separate in
`RawFinishReason`, and `FinishInferred` records whether normalization required
inference. Hosts should call `runtime.Event.Validate` at their integration
boundary. It validates the event plus nested stream observations, activity
timelines, context/compaction observations, committed detail identity, and turn
projections so unknown values cannot acquire a normal display state or lifecycle
semantics from `Metadata`.

Thread titles are always persisted by Floret. Set
`TurnExecutionHostOptions.ThreadTitleMode = runtime.ThreadTitleModeProvider`
when Floret
should issue the dedicated provider request automatically. Products that choose
titles themselves keep the default `runtime.ThreadTitleModeHostOwned` behavior
and call `SetThreadTitle`; they must not store a second title copy.

`RunTurnRequest.Input` is a structured `runtime.TurnInput`. A user turn may
contain text, opaque `MessageAttachment` resource references, or both. Attachment
resources remain host-owned and are resolved only by a host-supplied
`ModelGateway`; Floret persists the message-to-resource association without
reading the resource or storing file bytes. An attachment may include optional
host-attested `TextStats` with Unicode code-point and logical-line counts;
Floret validates and preserves that immutable display snapshot but does not
derive it from MIME type or content. Public hard limits bound attachment count,
field size, referenced byte totals, and descriptor JSON before admission.

A gateway that expands attachments into provider-native content declares
`ModelGatewayAttachmentPayloadExpanded` and implements the optional
`ModelGatewayRequestPreparer`. Preparation receives the complete `ModelRequest`
and returns one immutable, single-use `PreparedModelRequest`, a stable rendered
payload fingerprint, and a complete exact or conservative token estimate.
Floret uses that estimate for context pressure and input-limit checks, streams
the same prepared instance, and closes every prepared handle on compaction,
request rejection, cancellation, startup failure, normal completion, or Store
shutdown; standalone manual compaction closes its validated handle without
streaming it. The handle is never journaled, cached, or persisted. Existing
descriptor-only gateways keep the original direct `ModelGateway` contract and
the legacy estimate for requests without attachments. When attachment
descriptors are present, Floret conservatively counts at least one input token
per UTF-8 byte of the complete serialized `ModelRequest`; this complete-request
upper bound is partitioned into additive prefix, message, and tool components so
native-usage anchors retain attachment deltas. It participates in context
pressure but is not an exact token count.

When a host needs a durable display projection, use `ThreadTurnProjection` and
the public detail APIs. Do not read Floret's storage tables or rebuild
provider-visible history in the host.

Every turn projection carries `ThroughOrdinal`, the greatest durable detail
event ordinal included in that projection. Compare it only within the explicit
thread, turn, and run identities to reject duplicate or stale projections.
`ProjectedAt` is observation time only and is not an ordering key. Live
projections report `Status=running` from the committed turn-start marker until a
completed, waiting, failed, or cancelled marker becomes durable.

Turn execution and display projection availability are independent outcomes.
`TurnResult.ProjectionAvailability` is `ready` or `unavailable`; an unavailable
projection keeps terminal status, output, metrics, signal, and the ordinary
engine error unchanged. Opaque provider continuation is owned and persisted by
Floret's Store and is never exposed on `TurnResult`. `ProjectionError` is
diagnostic, while `ReadTurnProjection` is the explicit durable reload operation.
Runtime event sinks receive only public observation event types; harness
lifecycle events stay on the separate internal harness sink.

### Runtime flow

```text
Host UI/API
  |                  |                       |
  | CreateThread     | RunTurn / RetryTurn    | CompactThread
  v                  v                       v
ThreadCreateHost  TurnExecutionHost  ThreadCompactionHost
  |                  |                       |
  | canonical journal| provider/tool loop    | context lifecycle
  +------------------+-----------+-----------+
                                 v
Floret runtime
```

### Run it with confidence

Floret is deterministic with the fake provider, so tool behavior, approval
flows, retries, context pressure, and host UI projections can be tested without
real model calls.

Provider-backed execution uses separate thread-bound `TurnExecutionHost`,
`ThreadCompactionHost`, and parent-bound `SubAgentHost` capabilities. Creation,
read, title, fork, delete, parent-bound SubAgent read/maintenance, and bound
pending-tool settlement are also separate concrete capabilities. Create and
retain only the required narrow binders inside the one-time
`ConfigureHostCapabilities` callback. The callback bootstrap is sealed before
configuration returns, and retained binders become active only after the
callback succeeds. Afterward, each binder can issue only its named capability.
Provider binders fix one thread or parent before provider options are supplied;
provider-free binders return either the create-only coordinator handle or an
exact existing-authority handle. Pass only that selected factory or handle to
its owner; binders stay at the composition root.
Requests retain explicit identities and fail when they do not match the handle
authority.

```bash
go test ./...
go run ./cmd/floret-test-ui
```

The local test console is for contributors to inspect fake-provider sessions,
sanitized events, tool scenarios, and hosted child threads. It is not a
downstream integration surface.

## 📦 Downstream integration surface

Downstream applications should import only these public packages:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Everything under `internal/` is implementation detail. The package reference at
[pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) is the API
source of truth; [the OKF knowledge bundle](okf/index.md) explains the runtime's
architecture and vocabulary for contributors.

Every published release is checked from a blank temporary Go module with
workspace discovery disabled, no local replacement, and a fresh module cache.
The gate verifies the exact tag, module zip, and checksums before compiling and
running the durable host, custom gateway, tool approval, startup recovery, and
Store maintenance examples. Maintainers can run the same post-release check
with `scripts/check_published_release_adoption.sh <exact-tag>`.

## License

Floret is licensed under the [MIT License](LICENSE).
