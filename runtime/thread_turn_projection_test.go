package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestThreadTurnsPreserveHistoricalRandomizedPrefixes(t *testing.T) {
	hash := sha256.New()
	random := rand.New(rand.NewPCG(27, 1024))
	for scenario := 0; scenario < 100; scenario++ {
		entries := incrementalProjectionFixture(t)
		entries[0].Metadata = map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}
		entries = entries[:len(entries)-1]
		for i := 0; i < 100; i++ {
			kind := []sessiontree.EntryType{sessiontree.EntryInteractionAsked, sessiontree.EntryInteractionDone, sessiontree.EntryAssistantMessage, sessiontree.EntryCustom, sessiontree.EntryTurnMarker}[random.IntN(5)]
			entry := sessiontree.Entry{ID: fmt.Sprintf("entry-%d", i), Type: kind, ThreadID: "thread", TurnID: "turn", RunID: "run", CreatedAt: time.Unix(110+int64(random.IntN(200)), 0).UTC()}
			switch kind {
			case sessiontree.EntryInteractionAsked:
				entry.ID = "interaction-requested:" + fmt.Sprint(random.IntN(10))
				entry.Payload = []byte(fmt.Sprintf("{\"id\":%q,\"turn_id\":\"turn\",\"run_id\":\"run\",\"kind\":\"approval\"}", strings.TrimPrefix(entry.ID, "interaction-requested:")))
			case sessiontree.EntryInteractionDone:
				entry.ID = "interaction-resolved:" + fmt.Sprint(random.IntN(10))
				entry.Payload = []byte("{}")
			case sessiontree.EntryAssistantMessage:
				entry.Message = session.Message{Role: session.Assistant, Content: "body", ContentSourceID: fmt.Sprintf("answer-%d", i)}
			case sessiontree.EntryTurnMarker:
				entry.TurnStatus = []sessiontree.TurnMarkerStatus{sessiontree.TurnSavePoint, sessiontree.TurnWaiting, sessiontree.TurnCompleted, sessiontree.TurnFailed, sessiontree.TurnAborted}[random.IntN(5)]
			}
			entries = append(entries, entry)
		}
		for i := 1; i <= len(entries); i++ {
			items, _, err := threadRuntimeItemsFromEntries(entries[:i])
			if err != nil {
				t.Fatal(err)
			}
			got, err := threadRuntimeTurnsFromEntries(entries[:i], items)
			encoded, _ := json.Marshal(got)
			hash.Write(encoded)
			if err != nil {
				hash.Write([]byte(err.Error()))
			}

		}
	}
	if got := fmt.Sprintf("%x", hash.Sum(nil)); got != "2edbf26174fadf5bb6a5aee5a8b681a6575b59c5a9535256e5fda6abc3a7e2cb" {
		t.Fatalf("historical turn digest: %s", got)
	}
}

func TestThreadTurnIncrementalPrefixesAndDetachedSnapshots(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	entries[0].Metadata = map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}
	projection := threadTurnProjection{}
	items := newThreadItemProjection()
	lookup := func(id string) (ThreadItem, bool) {
		var found ThreadItem
		ok := false
		for _, item := range items.items {
			for _, source := range item.sourceEntryIDs {
				if source == id {
					found = item
					ok = true
				}
			}
		}
		return found, ok
	}
	var previous []ThreadTurn
	var frozen []byte
	for i, entry := range entries {
		if err := items.append([]sessiontree.Entry{entry}, nil); err != nil {
			t.Fatal(err)
		}
		if err := projection.observe(entry, lookup); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(previous)
		if string(encoded) != string(frozen) && i > 0 {
			t.Fatal("later append mutated prior turn snapshot")
		}
		got, err := projection.snapshot()
		fullItems, _ := items.snapshot()
		want, werr := threadRuntimeTurnsFromEntries(entries[:i+1], fullItems)
		left, _ := json.Marshal(got)
		right, _ := json.Marshal(want)
		if (err == nil) != (werr == nil) || string(left) != string(right) {
			t.Fatalf("prefix=%d got=%s want=%s errors=%v / %v", i, left, right, err, werr)
		}
		previous = got
		frozen = left
	}
}

func BenchmarkIncrementalTurnFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			projection := threadTurnProjection{}
			lookup := func(string) (ThreadItem, bool) { return ThreadItem{}, false }
			for i := 0; i < count; i++ {
				id := fmt.Sprint(i)
				if err := projection.observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnID: id, RunID: id, TurnStatus: sessiontree.TurnStarted}, lookup); err != nil {
					b.Fatal(err)
				}
				if err := projection.observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnID: id, RunID: id, TurnStatus: sessiontree.TurnCompleted}, lookup); err != nil {
					b.Fatal(err)
				}
			}
			if err := projection.observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnID: "active", RunID: "active", TurnStatus: sessiontree.TurnStarted}, lookup); err != nil {
				b.Fatal(err)
			}
			entry := sessiontree.Entry{Type: sessiontree.EntryCustom, TurnID: "active", RunID: "active"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := projection.observe(entry, lookup); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
