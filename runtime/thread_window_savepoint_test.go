package runtime

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestWindowSavepointSelectionMatchesCompleteTurnPrefixes(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	var path []sessiontree.Entry
	appendEntry := func(entry sessiontree.Entry) {
		t.Helper()
		saved, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID})
		if err != nil {
			t.Fatal(err)
		}
		path = append(path, saved)
	}
	for i, entry := range incrementalProjectionFixture(t) {
		appendEntry(entry)
		for n := 0; n < 4 && entry.TurnStatus != sessiontree.TurnCompleted; n++ {
			appendEntry(sessiontree.Entry{ID: fmt.Sprintf("neutral-%d-%d", i, n), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint, CreatedAt: entry.CreatedAt.Add(time.Duration(n) * time.Millisecond), Metadata: map[string]string{"reason": "tool_result_batch"}})
		}
		items, _, err := threadRuntimeItemsFromEntries(path)
		if err != nil {
			t.Fatal(err)
		}
		want, err := threadRuntimeTurnsFromEntries(path, items)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) == 0 {
			continue
		}
		got, _, err := service.loadSourceWindow(t.Context(), ThreadWindowInput{ThreadID: "thread", Limit: 150, ActivityFields: true})
		if err != nil {
			t.Fatalf("prefix %d: %v", i, err)
		}
		a, _ := json.Marshal(got.Turns)
		b, _ := json.Marshal(want)
		if string(a) != string(b) {
			t.Fatalf("prefix %d turn facts differ: got %s want %s", i, a, b)
		}
		var lifecycle threadLifecycleProjection
		for _, source := range path {
			lifecycle.observe(source)
		}
		turn, run, activity, outcome, failure := lifecycle.snapshot()
		a, _ = json.Marshal([]any{got.TurnID, got.RunID, got.Activity, got.LastOutcome, got.Failure})
		b, _ = json.Marshal([]any{turn, run, activity, outcome, failure})
		if string(a) != string(b) {
			t.Fatalf("prefix %d lifecycle differs: got %s want %s", i, a, b)
		}
	}
}
