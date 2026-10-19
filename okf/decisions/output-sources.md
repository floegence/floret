---
type: decision
---

# Canonical response output sources

The Engine is the sole owner of ordered response fragments. Run, physical
provider attempt and native/normalized part identity determine each source;
visible row counts and text equality never determine current identity.

Harness commits exact fragments, classified calls and provider state through
[CommitProviderContext](../../internal/sessiontree/provider_context.go). The
memory-only typed batch handoff binds the sources to actual canonical entries
before tools execute. Partial output uses the same transaction after the Engine
has stopped; it may preserve text after a cancellation request, but cannot admit
local calls, clear state or advance provider state. Individual observation events remain available, but cannot
independently mark output committed. No receipt table, UI transcript or second
lifecycle is introduced.

The actor merges canonical content and only current uncommitted sources.
Canonical text wins; existing tool progress remains newer than a persisted call.
Replacement attempts discard only their own uncommitted output. Native suffixes
update their original source. Hosts preserve IDs/order and interaction state.

The explicit additive public API decision adds optional provider.OutputPart for
native item/content positions, serving Redeven's DeepSeek bridge and other
provider gateways. Existing providers remain valid without the descriptor. The
v7 API baseline, compatibility gate and blank-consumer adoption protect released
contracts; strict-tool-schema defaults and bounded corrections do not change.

Schema 24 -> 25 makes sources durable on original messages and confines all old
shape decoding to startup migration. It preserves released public row IDs and
canonical references, and adds previously hidden correction reasoning without
exposing invalid calls as actionable tools. See [storage](../api/storage.md#canonical-output-source-migration).

Focused regressions exercise every intermediate publication of repeated schema
correction, native suffixes around hosted activity, identical text from distinct
sources, replacement attempts, rollback, current startup and nested forks.
