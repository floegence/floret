---
type: Architecture Decision
title: Prompt Scope Identity
description: Prompt cache and provider ledgers use PromptScopeID as their reuse boundary rather than RunID.
resource: /internal/provider/cache/promptcache.go
tags: [decision, identity, prompt-cache]
timestamp: 2026-08-31T00:00:00Z
---

# Decision

Prompt-cache rows and JSON use `prompt_scope_id` / `PromptScopeID` as the reuse
boundary. Segment authorship may record run or turn information, but those
fields do not define reuse.

# Reason

A durable thread and a standalone run have different ownership shapes. A
separate prompt-scope identity keeps cache reuse explicit instead of depending
on incidental equality between other IDs.

# Consequences

Storage, ledgers, events, and public request shapes must carry explicit
identity fields. Code must not infer prompt-cache ownership from `RunID`.

Within one prompt scope, the canonical message lineage is shared by every
model. Raw render lineage is narrower and keyed by provider, model, adapter
revision, provider-context projection revision, and cache namespace. This lets
a new Turn switch models or adopt a corrected projection without changing
canonical history while preserving an exact raw prefix whenever the same
rendering surface is used again.

During one execution the storage reader can retain a bounded prefix of immutable
committed segments. Each preparation still reads the complete uncached suffix
and pending segments under the kernel lock, then applies the requested provider
and model filter. Successful scope deletion invalidates retained prefixes;
restart, retry and new executions start fresh. This introduces no persisted
index and does not change which messages or tool results enter a request.

# Related

* [Execution Identities](../architecture/identities.md)
