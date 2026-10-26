package sessiontree

import (
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
	"reflect"
	"strings"
	"testing"
)

func TestMutationSummaryReadsBoundedCanonicalPrefixWithoutDetailPreparation(t *testing.T) {
	for _, size := range []int{100, 8000} {
		for _, count := range []int{1, 2, 200} {
			t.Run(fmt.Sprintf("bytes=%d/files=%d", size, count), func(t *testing.T) {
				backend, repo := projectionSourcesFixture(t, 1)
				mutations := tools.FileMutationActivityPayloads{}
				for range count {
					mutations = append(mutations, tools.FileMutationActivityPayload{DisplayName: "panda-bicycle.html", ChangeType: "delete", UnifiedDiff: strings.Repeat("🌸", size), DiffUnavailableReason: strings.Repeat("🌸", size)})
				}
				activity := &tools.ActivityPresentation{Label: "apply_patch", Renderer: tools.ActivityRendererPatch, Payload: tools.PatchActivityPayload{FilesChanged: count, Additions: 13, Deletions: 11, Mutations: &mutations}}
				entry, err := repo.Append(t.Context(), Entry{ID: "patch-summary", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "apply_patch", ToolCallID: "patch", Activity: activity}}, AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var ref canonicalBodyRef
				if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
					source, err := readCanonicalEntryMetadata(tx, "thread", entry.ID)
					ref = source.bodyFields["activity_json"]
					return err
				}); err != nil {
					t.Fatal(err)
				}
				// Reopening uses the previously published immediate-shape summary index.
				repo = reopenCurrentStartupRepo(t, backend, false)
				measured := &activityBudgetBackend{Backend: backend, bodyReads: map[string]int{}}
				repo.backend = measured
				for attempt := range 2 {
					measured.bodyBytes, measured.gets = 0, 0
					measured.bodyReads = map[string]int{}
					before := cloneMigrationRecords(backend.records)
					summary, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:patch")
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(summary.Values, activityfields.SummaryValues(activity)) {
						t.Fatalf("resident/canonical mismatch: %s", summary.Values)
					}
					for _, namespace := range []string{activityFieldsNamespace, canonicalBodyNamespace, backendDomainV32Namespace} {
						if !reflect.DeepEqual(before[namespace], backend.records[namespace]) {
							t.Fatalf("summary changed %s", namespace)
						}
					}
					if attempt > 0 && !reflect.DeepEqual(before, backend.records) {
						t.Fatal("repeated summary wrote records")
					}
					if measured.bodyBytes > 100000 {
						t.Fatalf("read grew with diff: %d bytes", measured.bodyBytes)
					}
					if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
						_, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(ref.SHA256))
						if ref.Bytes > 64<<10 && !errors.Is(err, spi.ErrNotFound) {
							t.Fatalf("summary prepared hidden detail: %v", err)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					t.Logf("attempt=%d body_bytes=%d gets=%d", attempt, measured.bodyBytes, measured.gets)
				}
			})
		}
	}
}
