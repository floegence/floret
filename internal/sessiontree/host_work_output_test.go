package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestHostWorkStreamForkPinsPrefixAndReleasesReferences(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "stream", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "stream")}
	if _, err := repo.BindHostWork(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	in := HostWorkOutputAppend{ThreadID: "thread", WorkID: work.ID, ExecutorID: work.ExecutorID, Text: "first 世界\n"}
	source, err := repo.AppendHostWorkOutput(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := repo.AppendHostWorkOutput(t.Context(), in); !errors.Is(err, ErrRequestConflict) {
		t.Fatal("stale watermark appended", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("conflict mutated capture")
	}
	if _, err := repo.FinishEffectDispatch(t.Context(), canonicalEffectRequest(attempt)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	in.Offset = source.ObservedBytes
	in.Text = "last 🌸\n"
	source, err = repo.AppendHostWorkOutput(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	req := HostWorkOutputRequest{ThreadID: "fork", WorkID: work.ID, MaxBytes: 8000}
	page, err := repo.ReadHostWorkOutputPage(t.Context(), req)
	if err != nil || page.Text != "first 世界\n" || !page.Source.Sealed || page.Source.Reason != "interrupted" {
		t.Fatalf("fork watermark changed: %+v %v", page, err)
	}
	in.ThreadID = "fork"
	in.Offset = source.ObservedBytes
	if _, err := repo.AppendHostWorkOutput(t.Context(), in); !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatalf("fork appended: %v", err)
	}
	if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ThreadID: "thread", ID: work.ID, ExecutorID: work.ExecutorID, State: "completed", Notice: "completed"}); err != nil {
		t.Fatal(err)
	}
	req.ThreadID = "thread"
	chunks, err := repo.ReadHostWorkOutputChunks(t.Context(), req, 0)
	if err != nil || len(chunks.Chunks) != 2 || !chunks.Source.Sealed || chunks.Source.Reason != "" {
		t.Fatalf("complete chunks: %+v %v", chunks, err)
	}
	req.MaxBytes = len("first 世界\n")
	first, err := repo.ReadHostWorkOutputChunks(t.Context(), req, 0)
	if err != nil || !first.HasMore || len(first.Chunks) != 1 || first.Chunks[0].Seq != 1 {
		t.Fatalf("first contiguous chunk: %+v %v", first, err)
	}
	last, err := repo.ReadHostWorkOutputChunks(t.Context(), req, 1)
	if err != nil || last.HasMore || len(last.Chunks) != 1 || last.Chunks[0].Seq != 2 || last.Chunks[0].Text != "last 🌸\n" {
		t.Fatalf("last contiguous chunk: %+v %v", last, err)
	}
	req.MaxBytes = 8000
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	req.ThreadID = "fork"
	if retained, err := repo.ReadHostWorkOutputPage(t.Context(), req); err != nil || retained.Text != "first 世界\n" {
		t.Fatalf("source deletion lost fork: %+v %v", retained, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		retained, err := readOutputSource(tx, source.ID)
		if err != nil {
			return err
		}
		if retained.References != 1 {
			t.Fatal("fork reference not retained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "fork"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := readOutputSource(tx, source.ID)
		if !errors.Is(err, spi.ErrNotFound) {
			t.Fatal("source survived last release", err)
		}
		used, err := outputStoreUsage(tx)
		if used != 0 {
			t.Fatal("quota not released")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestHostWorkStreamAppendRollbackAndCorruptPage(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "stream", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "stream")}
	if _, err := repo.BindHostWork(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("disk failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	in := HostWorkOutputAppend{ThreadID: "thread", WorkID: work.ID, ExecutorID: work.ExecutorID, Text: "preserved"}
	if _, err := repo.AppendHostWorkOutput(t.Context(), in); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("watermark advanced before commit")
	}
	repo.backend = backend
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.AppendHostWorkOutput(ctx, in); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := repo.AppendHostWorkOutput(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return putOutputRecord(tx, outputChunkKey(work.OutputStream, 1), OutputSourceChunk{Seq: 1, Text: "corrupted", SHA256: strings.Repeat("0", 64)})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadHostWorkOutputPage(t.Context(), HostWorkOutputRequest{ThreadID: "thread", WorkID: work.ID, MaxBytes: 8000}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatal("corruption presented as EOF", err)
	}
}

func TestArtifactQuotaFailurePreservesConfirmedEffect(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return putOutputRecord(tx, outputUsageKey(), outputStoreMaxBytes) }); err != nil {
		t.Fatal(err)
	}
	req := canonicalEffectRequest(attempt)
	req.FullOutput = &artifact.FullOutput{Text: strings.Repeat("x", 100000)}
	got, err := repo.FinishEffectDispatch(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Artifact != nil || got.Result.Message.ToolResult.OutputUnavailable != "storage_limit" || got.Attempt.State != EffectAttemptCompleted {
		t.Fatalf("output loss changed execution: %+v", got)
	}
	replay, err := repo.FinishEffectDispatch(t.Context(), req)
	if err != nil || !replay.Replayed {
		t.Fatalf("result replay: %+v %v", replay, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := readOutputSource(tx, outputSourceID("thread", "unused"))
		if !errors.Is(err, spi.ErrNotFound) {
			t.Fatal(err)
		}
		used, err := outputStoreUsage(tx)
		if used != outputStoreMaxBytes {
			t.Fatal("partial retention committed")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
