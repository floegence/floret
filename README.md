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

## Verification

Run:

```bash
go test ./...
go test -race ./...
```
