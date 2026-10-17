---
type: Public API
title: Canonical thread queries
description: Authorized indexed search, immutable pagination and bounded full reads.
tags: [api, threads, queries, references]
---

# Canonical thread queries

`runtime.ThreadQueryReader` is optional; `ThreadService` implementations keep
all previously required methods. `Query`, `Read`, and `Aggregate` use public
[`threadquery`](../../threadquery/query.go) contracts. The canonical journal
owner derives metadata and source locators through one canonical projector.
SQLite maintains contentless FTS5 postings and document metadata in the same
transaction as canonical writes. Other physical backends use the optional
transaction-local index SPI or Floret's opaque KV adapter. Native errors never
switch to a second index or a full canonical scan.

Startup validates canonical state and source correspondence before reusing the
index. Missing derived state initializes transactionally before Host exposure;
ordinary reopen does not rebuild postings. Canonical damage and unknown formats
fail closed. Physical format 2 -> 3 and logical storage 8 -> 9 preserve domain
schema 19 and every historical migration edge. Atomic ASCII encodings of Go
Unicode-lowercased 1/2/3-rune grams preserve Chinese, code, punctuation, emoji
and NUL candidate semantics; one exact matcher verifies candidates. No full
search body or global positions are stored. Only bounded active projections and
frozen pagination values remain in memory.

Hosts reconstruct `Scope` before every call. `Authority` identifies the user,
runtime and namespace; `ThreadIDs` is the exact authorized set, including
readable children and archived threads when host policy permits them. Empty sets
grant nothing. Model filters may narrow this scope, never grant access. Private
tool arguments and confirmed result bodies belong only to an authorized query
consumer, never an ordinary thread list or timeline DTO.

Objects are `threads`, `messages`, `tool_calls`. Filters across fields use AND;
values within one field use OR. `TextMatch` supports `contains`, literal
`keywords` (all by default, any when requested), and a contiguous `phrase`.
Matching uses Unicode lowercase by default or exact case when requested. It
supports Chinese, English, punctuation and code fragments without fuzzy matching.
Time ranges compare UTC instants as `[from, to)`. Wrong object-specific fields
or unsupported sorts fail explicitly.

| Object | Fields | Sort keys |
| --- | --- | --- |
| threads | IDs, parents, title, created/updated, canonical activity/outcome, lifecycle | created, updated, title, thread_id |
| messages | IDs, exact turn/run, user/assistant role, body/reference text, time | time, order |
| tool_calls | exact execution identity, tool name, local/hosted kind, confirmation status, arguments/result, invocation/completion time | time, completed, tool_name, elapsed |

Stable record identity breaks every tie. `order` follows the canonical order
within a thread; across threads, equal ordinals use identity. Messages use the
same legacy concatenation and segmented-entry rules as the timeline. Tool calls
count once per `(ThreadID, TurnID, RunID, ToolCallID)`; confirmations
update that fact. Unconfirmed calls have `unconfirmed` status. Imported standalone
tool messages do not invent invocations. Hosted tools expose only their persisted
visible name, status and presentation; absent raw arguments/output stay absent.
Correction records, reasoning, secret inputs, private provider state, queue facts
and internal recovery entries never enter search.

`Aggregate` uses the same scope/filter path. It provides an exact total with
optional thread/role/tool_name/status or UTC day/week/month grouping. Weeks start
Monday. Groups sort by key or count, with stable key ties, and support paging.

`Query` defaults to 50 rows, permits at most 200, and returns a bounded hit snippet
and opaque `RecordRef`. The first call freezes the matching canonical facts;
continuations use the stable last record key and never project history again.
`Read` takes a returned ref and returns explicit field-local UTF-8 byte-offset
chunks (at most 16 KiB each) in responses at most 64 KiB. Continue until no cursor
remains; full content is never silently truncated or fetched from host resources. Message
reads retain ordered visible reference metadata and ordinary admitted context;
opaque resource locators are excluded. Hosted presentation stays a reference to
the canonical typed record and is rendered only for search/read.

Snapshots live in memory for at most five minutes with a conservative global
64 MiB budget, including cursor bookkeeping. Oversized queries return
`ErrSnapshotLimit` and require narrower filters. Append, confirmation and rename
leave existing snapshots intact. Changed authority or readable IDs, deletion,
restart, expiration or snapshot retirement fail with `ErrCursorInvalid` or
`ErrRecordUnavailable`; query again rather than expanding or substituting scope.
Refs and cursors are opaque random capabilities bound to this process and scope.

`MessageReferenceThread` stores target `ReferencedThreadID`, `Label`, and frozen
`Text` on the canonical user message, with no `ResourceRef`. It renders the
explicit identity as untrusted data. The host validates readability on initial
admission and compares frozen input on transport retry. Schema 14 -> 15 validates
old authority and preserves historical record bytes and fingerprints.

## Canonical Attachment Discovery

User attachments and confirmed tool-result attachments contribute safe source
labels to text search and chunked reads. Each label includes a stable descriptor
locator, name, MIME and size, excluding ResourceRef. Name and MIME search use the
canonical owner's persistent derived index; no durable media mapping is added. Locators
are lookup keys, never access grants. Hosts must reauthorize the current thread
and exact canonical descriptor before opening immutable bytes.
