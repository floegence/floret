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

## Output timing

The additive public API decision exposes optional runtime.ThreadItem.OutputStartedAt
and OutputEndedAt for host duration displays. The Engine owns both timestamps:
first and last nonempty chunk receipt for each exact source, including native
continuations and split UTF-8 input. Live observation carries those same facts;
finalization does not count tool, delivery or commit delay after the last chunk.
This span is not provider compute time. A single chunk has equal endpoints.

Harness persists the pair as optional canonical entry metadata. Existing schema
32 records remain unchanged; missing facts mean unmeasured. Invalid, incomplete
or reversed pairs fail integrity validation. Views, bounded windows, history,
restart and Fork project these facts without a host timing ledger. Cancellation
retains the same timing with accepted partial output. Live items omit the end
until sealed. Public API baseline and external adoption gates cover the addition.
