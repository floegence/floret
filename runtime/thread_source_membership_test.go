package runtime

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
)

// A single displayed response may contain arbitrarily many immutable commits.
// Installing one more commit must not copy the full private source membership.
func BenchmarkRetainedActorMergedAppend(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			host, err := Open(b.Context(), Options{Storage: storage.Memory()})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = host.Shutdown(b.Context()) })
			service := &threadRuntimeService{host: host}
			projection := newThreadCanonicalProjection()
			entries := make([]sessiontree.Entry, count)
			for i := range entries {
				entries[i] = sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, CreatedAt: time.Unix(1, 0), Message: session.Message{Role: session.Assistant, Content: "body", ContentSourceID: "source"}}
				if i > 0 {
					entries[i].ParentID = entries[i-1].ID
				}
			}
			if err := projection.append(entries); err != nil {
				b.Fatal(err)
			}
			actor := &threadRuntimeState{threadID: "thread", service: service, canonical: projection, state: threadRuntimeData{view: ThreadView{ThreadID: "thread", Activity: ThreadActivityActive}}}
			if err := actor.installCanonicalChanges(projection); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				projection.resetChanges()
				entry := entries[0]
				entry.ID, entry.ParentID = fmt.Sprint("append-", i), projection.leafID
				if err := projection.append([]sessiontree.Entry{entry}); err != nil {
					b.Fatal(err)
				}
				if err := actor.installCanonicalChanges(projection); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestRetainedActorDropsPrivateSourceMembership(t *testing.T) {
	_, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	projection := newThreadCanonicalProjection()
	actor := &threadRuntimeState{threadID: "thread", service: service, canonical: projection, state: threadRuntimeData{view: ThreadView{ThreadID: "thread", Activity: ThreadActivityActive}}}
	for i := range 512 {
		projection.resetChanges()
		entry := sessiontree.Entry{ID: fmt.Sprint(i), ParentID: projection.leafID, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "中🌸", ContentSourceID: "source", Reasoning: "reason", ReasoningSourceID: "reason"}}
		if err := projection.append([]sessiontree.Entry{entry}); err != nil {
			t.Fatal(err)
		}
		if err := actor.installCanonicalChanges(projection); err != nil {
			t.Fatal(err)
		}
		for _, items := range [][]ThreadItem{actor.state.view.Items, projection.items.items} {
			for _, item := range items {
				if len(item.sourceEntryIDs) != 0 {
					t.Fatalf("retained %d sources at %d", len(item.sourceEntryIDs), i)
				}
			}
		}
		if got := actor.state.view.Items[1].textBytes; got != (i+1)*len("中🌸") {
			t.Fatalf("extent=%d", got)
		}
	}
}

func TestCompleteTurnSourcesResolveSegmentedAssistantIdentity(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	entries[0].Metadata = map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}
	projection := newThreadCanonicalProjection()
	for i := range entries {
		if i > 0 {
			entries[i].ParentID = entries[i-1].ID
		}
	}
	if err := projection.append(entries); err != nil {
		t.Fatal(err)
	}
	items, _ := projection.items.snapshot()
	want, err := projection.turns.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	got, err := threadRuntimeTurnsFromEntries(entries, items)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(got)
	z, _ := json.Marshal(want)
	if string(a) != string(z) {
		t.Fatalf("got=%s want=%s", a, z)
	}
}
