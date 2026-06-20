---
type: Architecture Decision
title: Prompt Scope Identity
description: Prompt cache and provider ledgers use PromptScopeID as their reuse boundary rather than RunID.
resource: /internal/provider/cache/promptcache.go
tags: [decision, identity, prompt-cache]
timestamp: 2026-06-20T00:00:00Z
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

# Related

* [Execution Identities](../architecture/identities.md)
