package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
)

func contextMetadataFixture(t *testing.T, count int) (*MemoryRepo, *migrationTestBackend, *BackendRepo) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "context-source"}); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		status := observation.ContextStatus{ThreadID: "context-source", TurnID: "turn", RunID: "run", Provider: "test", Model: "model", Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusEstimated, ObservedAt: time.Unix(int64(i+1), 0).UTC()}
		if i%3 == 0 {
			status.Phase, status.Status = observation.ContextPhaseProviderUsage, observation.ContextStatusStable
			status.Usage = observation.ProviderUsage{Available: true, InputTokensReported: true, InputTokenScope: "request_input", InputTokens: int64(i), OutputTokens: 2}
		}
		entry, err := NewThreadContextStatusEntry(status)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint("status-", i), Now: status.ObservedAt}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "context-source"); err != nil {
		t.Fatal(err)
	}
	return memory, backend, repo
}

func scanContextMetadata(t *testing.T, repo *BackendRepo) ([]Entry, error) {
	t.Helper()
	var entries []Entry
	err := repo.ScanPathProjection(t.Context(), "context-source", "", func([]PathMetadata) error { return nil }, func(PathMetadata) []string { return nil }, func(page PathProjectionPage) error {
		for _, source := range page.Entries {
			entries = append(entries, source.Entry)
		}
		return nil
	})
	return entries, err
}

func TestContextSourceMetadataColdReadRetainsOnlyCurrentCompleteMeasurements(t *testing.T) {
	for _, count := range []int{32, 320} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory, backend, repo := contextMetadataFixture(t, count)
			want, err := memory.Path(t.Context(), "context-source", "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := scanContextMetadata(t, repo); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
					var err error
					repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				got, err := scanContextMetadata(t, repo)
				if err != nil || len(got) != len(want) {
					t.Fatalf("entries=%d err=%v", len(got), err)
				}
				complete := 0
				for i, entry := range got {
					status, err := ProjectionThreadContextStatus(entry)
					if err != nil {
						t.Fatal(err)
					}
					expected, err := DecodeThreadContextStatusEntry(want[i])
					if err != nil {
						t.Fatal(err)
					}
					if entry.contextStatusMetadata == nil {
						complete++
						if !reflect.DeepEqual(status, expected) {
							t.Fatal("selected complete measurement differs")
						}
					} else {
						if _, err := DecodeThreadContextStatusEntry(entry); err == nil {
							t.Fatal("partial source escaped complete decoder")
						}
						if status.Provider != expected.Provider || status.Model != expected.Model || status.Phase != expected.Phase || status.Usage.InputTokens != expected.Usage.InputTokens || status.Usage.OutputTokens != expected.Usage.OutputTokens || !status.ObservedAt.Equal(expected.ObservedAt) {
							t.Fatal("numeric source facts differ")
						}
					}
				}
				if complete > 2 {
					t.Fatalf("decoded %d historical status bodies", complete)
				}
			}
		})
	}
}

func TestContextSourceMetadataPreparationRollsBackAndResumes(t *testing.T) {
	_, backend, repo := contextMetadataFixture(t, 260)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: contextMetadataNamespace, Limit: 128})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				if err := tx.Delete(contextMetadataNamespace, row.Key); err != nil {
					return err
				}
			}
			if !page.HasMore {
				return nil
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("injected context metadata failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := scanContextMetadata(t, repo); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed preparation changed source")
	}
	ctx, cancel := context.WithCancel(t.Context())
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { cancel(); return ctx.Err() }}
	if err := repo.prepareContextSourceMetadata(ctx, "context-source"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("cancelled preparation changed source")
	}
	repo.backend = backend
	if entries, err := scanContextMetadata(t, repo); err != nil || len(entries) != 260 {
		t.Fatalf("resume entries=%d err=%v", len(entries), err)
	}
	if err := repo.MoveLeaf(t.Context(), "context-source", "status-20"); err != nil {
		t.Fatal(err)
	}
	if entries, err := scanContextMetadata(t, repo); err != nil || len(entries) != 21 {
		t.Fatalf("branch entries=%d err=%v", len(entries), err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "context-source"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		page, err := tx.Scan(spi.ScanRequest{Namespace: contextMetadataNamespace, Limit: 1})
		if err == nil && len(page.Records) != 0 {
			t.Fatal("deleted thread retained source metadata")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
