---
type: architecture
---

# Message Revisions

The optional `runtime.ThreadMessageEditor` owns editing the latest ordinary
user input. Hosts own editor UI, resource authorization and execution grants;
they must not compose Cancel, MoveLeaf and Send or mutate Floret records.

One actor holds a bounded edit transition. Opening/canceling an editor is a host
operation. Submitting with StopActive first settles the exact active turn, then
rechecks source identity and full expected input. Admission writes a
`message_revision` boundary, fresh turn marker and replacement user atomically.
The revision branches before the original turn marker. Historical records remain
unchanged; the previous leaf supports read-only version inspection.

Goal and usage totals are validated snapshots at the revision boundary. Todo
and queue keep their existing canonical owners. Provider context, display,
queries, Retry and Fork consume the selected branch. A canonical revision ID
permits exactly one prompt lineage reset while preserving old request audit.
Superseded compaction and provider state are not reused. Unknown effects block
reexecution; editing never undoes external effects or cancels independent work.

LookupMessageEdit resolves a committed request before host resource preparation.
A disconnected caller can query the same key. Stop and admission are distinct:
an admission failure can leave the old turn stopped. A crash before admission
never auto-submits a draft. Existing runtime recovery owns admitted turns.

Sources: [public contract](../../runtime/thread_message_edit.go),
[admission](../../internal/sessiontree/message_revision.go),
[acceptance tests](../../runtime/thread_message_edit_external_test.go).

An open child owned by the discarded turn prevents editing until its own lifecycle closes. Editing never cancels or detaches that child. Goal deletion suppression follows the replacement input, including across Fork. Revision source identity, branch boundary, Goal and usage facts are validated against the immutable previous path.
