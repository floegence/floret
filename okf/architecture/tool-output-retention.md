---
type: Architecture Concept
title: Bounded Tool Output Retention
description: Canonical pre-preview text, append-only captures, explicit retention facts and authorized bounded reads.
resource: /runtime/thread_output.go
tags: [tools, runtime, storage]
timestamp: 2026-11-09T00:00:00Z
---

# Summary

Floret owns canonical tool text, retention facts and lifecycle. Hosts capture and
privacy-process text before admission, authorize every read, and render UI. A
preview, a retained source and a tool execution outcome are independent facts.
No output failure authorizes replay, target substitution or mandatory input.

# Public API decision

The additive `tools.OutputHeadTail` strategy provides an approximately equal
UTF-8 head/tail preview with an explicit omitted-byte marker inside the byte and
line budgets. Existing default policies remain compatible; a host can choose
8000 bytes and this strategy. Pagination is always contiguous, never head/tail.
`Result.FullOutput` explicitly preserves pre-preview, host-sanitized text while
`Text` contains a bounded structured envelope. It does not claim raw PTY bytes,
undo redaction, copy media, or preserve an entire file when only a page was read.

`tools.OutputReference` is a versioned locator, not an authorization capability.
New archived results render a valid JSON envelope with separate execution status,
result and retention reference. `ThreadToolResultReader` exposes that reference
for authorized on-demand UI disclosure; it does not fetch the source body.

`runtime.ThreadOutputService` supplies bounded reads, literal search, host-work
append and sequence-preserving chunk reads. A read defaults to 8000 bytes and is
at most 64 KiB. Byte offsets must be UTF-8 boundaries and returned next offsets
are exact. Search is case-sensitive literal UTF-8 (1–256 bytes), at most 32
matches and 1 MiB scanned per call; continuation preserves cross-page matches.
There is no global text index, startup scan, regex evaluation or implicit search
outside the exact referenced output. Hosts reauthorize the reference's thread
and their product permission scope on every request.

# Streaming and integrity

`BindHostWorkInput.StreamOutput` allocates a source atomically with the authorized
binding. Hosts append complete UTF-8 chunks before evicting their live cache.
The expected observed offset rejects duplicate or out-of-order appends; callers
must not blindly resend after an uncertain persistence result. Each chunk,
watermark, hash state and quota update commits in one transaction. Chunk pages
preserve append sequences; a page budget must fit its first complete chunk.

Completion seals the source independently of exit status. Capture loss can be
reported with `OutputReason`. After abnormal restart, the original execution is
unknown and its committed prefix remains readable with `interrupted`, never
misrepresented as complete. `ObservedBytes` counts committed append inputs,
including quota-discarded inputs; a storage/capture error means it may not cover
all bytes the host observed. No process is adopted or replayed.

Each source retains at most 64 MiB and a backend retains at most 1 GiB of source
text. These are logical UTF-8 byte limits, not physical database size limits.
Quota exhaustion preserves the prefix and reports `output_limit` or
`storage_limit`; it never evicts another referenced source. Failure to retain a
whole-result artifact rolls back that source and admits the already observed
execution result with an explicit unavailable reason. Failure to commit even the
execution fact remains an error; the runtime does not invent a successful save.

Checksummed 64 KiB-or-smaller chunks and a small offset index support random
access without reading the prefix or materializing the source. Full legacy
artifact reads remain supported. Fork and integrity verification stream checksums
with bounded memory. Referenced bodies have no TTL: compaction, archiving and UI
folding do not delete them. Fork pins an immutable watermark and retains the
shared source; only deletion of the last canonical owner releases its bytes.

# Compatibility

Schema 38 to 39 changes the manifest fence and admits new source references,
without rewriting historical inline artifacts. Logical storage 13 to 14 rejects
released older readers before domain decoding. Every released migration remains
supported. Current-store restart does not rescan output histories. Hosts never
inspect or migrate the opaque source namespace.

# Evidence

- [Public API and consumer qualification](../../runtime/thread_output_external_test.go)
- [Transactional append, Fork, corruption and retention failure](../../internal/sessiontree/host_work_output_test.go)
- [UTF-8 pages and source hashes](../../internal/sessiontree/output_source_test.go)
- [Schema preservation and rollback](../../internal/sessiontree/backend_repo_v39_test.go)
- [Head/tail and explicit full-plan budgets](../../tools/output_head_tail_test.go)
- Explicit 50 MiB qualification uses `FLORET_OUTPUT_QUALIFICATION=1 go test ./runtime -run '^TestOutputStream50MiBQualification$' -count=1 -v`; ordinary consumer gates always exercise smaller source, model boundary and restart tests.
