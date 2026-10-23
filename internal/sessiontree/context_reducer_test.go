package sessiontree

import (
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"reflect"
	"testing"
)

// Frozen pre-optimization algorithm is independent of the incremental reducer.
func originalContextMessages(path []Entry) ([]session.Message, error) {
	var steeringErr error
	path, steeringErr = projectSteering(path)
	if steeringErr != nil {
		return nil, steeringErr
	}
	redundant, err := RedundantControlResultIDs(path)
	if err != nil {
		return nil, err
	}
	compactionIndex := -1
	for i, entry := range path {
		if entry.Type == EntryCompaction {
			compactionIndex = i
		}
	}
	var messages []session.Message
	start := 0
	if compactionIndex >= 0 {
		prefix, err := originalContextMessages(path[:compactionIndex])
		if err != nil {
			return nil, err
		}
		messages, err = projectCheckpoint(prefix, path[compactionIndex], path[:compactionIndex])
		if err != nil {
			return nil, err
		}
		start = compactionIndex + 1
	}
	for _, entry := range path[start:] {
		if redundant[entry.ID] {
			continue
		}
		var err error
		messages, err = appendProviderVisible(messages, entry)
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func TestContextReducerMatchesOriginalCheckpointsAcrossPages(t *testing.T) {
	var path []Entry
	add := func(entry Entry) {
		entry.ThreadID = "thread"
		entry.TurnID = "turn"
		entry.RunID = "run"
		if len(path) > 0 {
			entry.ParentID = path[len(path)-1].ID
		}
		path = append(path, entry)
	}
	add(Entry{ID: "retained-user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retain across checkpoints"}})
	for checkpoint := range 10 {
		for i := range 20 {
			id := fmt.Sprintf("%d-%d", checkpoint, i)
			add(Entry{ID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "Unicode 🌸"}})
		}
		add(Entry{ID: fmt.Sprintf("compact-%d", checkpoint), Type: EntryCompaction, Summary: "complete synthetic summary", CompactedThroughEntryID: path[len(path)-1].ID, KeptUserEntryIDs: []string{"retained-user"}})
	}
	add(Entry{ID: "tail", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "tail"}})
	for _, size := range []int{1, 3, 17, 256} {
		var reducer ContextReducer
		for start := 0; start < len(path); start += size {
			end := min(start+size, len(path))
			err := reducer.Append(path[start:end])
			want, wantErr := originalContextMessages(path[:end])
			if err != nil || wantErr != nil || !reflect.DeepEqual(reducer.Messages(), want) {
				t.Fatalf("page=%d end=%d err=%v original=%v", size, end, err, wantErr)
			}
		}
	}
	broken := append([]Entry(nil), path...)
	broken[21].CompactedThroughEntryID = "absent"
	_, wantErr := originalContextMessages(broken)
	var reducer ContextReducer
	err := reducer.Append(broken)
	if !errors.Is(wantErr, ErrAuthorityCorrupt) || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt: %v %v", wantErr, err)
	}
}

func TestContextEntrySelectionPreservesRetryAndAppliedSteering(t *testing.T) {
	path := []Entry{
		{ID: "user", Type: EntryUserMessage, TurnID: "first", RunID: "run", Message: session.Message{Role: session.User, Content: "original"}},
		{ID: "answer", ParentID: "user", Type: EntryAssistantMessage, TurnID: "first", RunID: "run", Message: session.Message{Role: session.Assistant, Content: "superseded"}},
		{ID: "steer", ParentID: "answer", Type: EntrySteeringRequested, TurnID: "first", RunID: "run", RequestKey: "steer", RequestFingerprint: "steer", Message: session.Message{Role: session.User, Content: "retained correction"}},
		{ID: "apply", ParentID: "steer", Type: EntrySteeringApplied, TurnID: "first", RunID: "run", Payload: []byte("{\"steering_id\":\"steer\"}")},
		{ID: "retry", ParentID: "apply", Type: EntryTurnMarker, TurnStatus: TurnStarted, TurnID: "second", RunID: "run2", Metadata: map[string]string{RetrySourceTurnIDMetadataKey: "first", RetrySourceEntryIDMetadataKey: "user"}},
		{ID: "final", ParentID: "retry", Type: EntryAssistantMessage, TurnID: "second", RunID: "run2", Message: session.Message{Role: session.Assistant, Content: "corrected"}},
	}
	metadata := make([]PathMetadata, len(path))
	byID := map[string]Entry{}
	for i, entry := range path {
		byID[entry.ID] = entry
		metadata[i] = PathMetadata{Entry: entry}
		metadata[i].Entry.Message.Content = ""
	}
	ids, err := ContextEntrySelection(metadata)
	if err != nil {
		t.Fatal(err)
	}
	var reducer ContextReducer
	for _, id := range ids {
		if err := reducer.Append([]Entry{byID[id]}); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := retryProjectedContextPath(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := originalContextMessages(selected)
	if err != nil || !reflect.DeepEqual(want, reducer.Messages()) {
		t.Fatalf("retry differs: %v", err)
	}
	if len(want) != 3 || want[1].Content != "retained correction" {
		t.Fatal("Retry lost accepted correction")
	}
	metadata[4].Entry.Metadata = map[string]string{RetrySourceTurnIDMetadataKey: "first", RetrySourceEntryIDMetadataKey: "absent"}
	if _, err := ContextEntrySelection(metadata); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatal(err)
	}
}
