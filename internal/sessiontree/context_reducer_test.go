package sessiontree

import (
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"reflect"
	"runtime"
	"testing"
	"unsafe"
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
		{ID: "model", ParentID: "steer", Type: EntryModelContext, TurnID: "first", RunID: "run", Message: session.Message{Role: session.User, ModelContextSnapshot: &session.ModelContextSnapshot{}}},
		{ID: "apply", ParentID: "model", Type: EntrySteeringApplied, TurnID: "first", RunID: "run", Payload: []byte("{\"steering_id\":\"steer\"}")},
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
	if len(want) != 4 || want[2].Content != "retained correction" {
		t.Fatal("Retry lost accepted correction")
	}
	metadata[5].Entry.Metadata = map[string]string{RetrySourceTurnIDMetadataKey: "first", RetrySourceEntryIDMetadataKey: "absent"}
	if _, err := ContextEntrySelection(metadata); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatal(err)
	}
}

func TestRetryProjectionReusesHistoryCapacity(t *testing.T) {
	path := make([]Entry, 0, 2500)
	for i := range 2000 {
		path = append(path, Entry{ID: fmt.Sprint("user-", i), Type: EntryUserMessage, TurnID: "original", RunID: "run", Message: session.Message{Role: session.User, Content: "keep"}})
	}
	for i := range 50 {
		path = append(path, Entry{ID: fmt.Sprint("retry-", i), Type: EntryTurnMarker, TurnStatus: TurnStarted, TurnID: fmt.Sprint("turn-", i), RunID: fmt.Sprint("run-", i), Metadata: map[string]string{RetrySourceTurnIDMetadataKey: "original", RetrySourceEntryIDMetadataKey: "user-1999"}})
		path = append(path, Entry{ID: fmt.Sprint("answer-", i), Type: EntryAssistantMessage, TurnID: fmt.Sprint("turn-", i), RunID: fmt.Sprint("run-", i), Message: session.Message{Role: session.Assistant, Content: "answer"}})
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := retryProjectedContextPath(path)
	runtime.ReadMemStats(&after)
	if err != nil || len(got) != 2002 || got[len(got)-1].ID != "answer-49" {
		t.Fatalf("result=%d err=%v", len(got), err)
	}
	if bytes := after.TotalAlloc - before.TotalAlloc; bytes > uint64(len(path))*uint64(unsafe.Sizeof(Entry{}))*5 {
		t.Fatalf("retry repeatedly copied complete history: %d bytes", bytes)
	}
	if path[2000].ID != "retry-0" || path[2001].ID != "answer-0" {
		t.Fatal("source path mutated")
	}
}

func TestCurrentContextStatusRejectsEveryExplicitIdentity(t *testing.T) {
	for _, key := range []string{"run_id", "thread_id", "turn_id"} {
		for _, value := range []string{"null", "\"\"", "\"identity\""} {
			if _, err := decodeThreadContextStatusPayload("{\""+key+"\":"+value+"}", false); err == nil {
				t.Fatalf("accepted %s=%s", key, value)
			}
		}
	}
}

func TestModelContextReducerMatchesCompleteCheckpointFacts(t *testing.T) {
	model := &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "workspace", Title: "Workspace", Text: "preserved 🌸"}}}
	path := []Entry{
		{ID: "managed", Type: EntryModelContext, Message: session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: model}},
		{ID: "user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retain this source"}},
		{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "read", ToolName: "read", ToolArgs: `{"path":"source"}`}},
		{ID: "result", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "read", ToolName: "read", Content: "large historical result"}},
		{ID: "compact", Type: EntryCompaction, Summary: "complete summary", CompactedThroughEntryID: "result", ModelContextBaseline: model, KeptUserEntryIDs: []string{"user"}},
		{ID: "clear", Type: EntryModelContext, Message: session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{}}}},
	}
	for _, corrupt := range []bool{false, true} {
		source := append([]Entry(nil), path...)
		if corrupt {
			source[4].FirstKeptEntryID = "absent"
		}
		var complete ContextReducer
		wantErr := complete.Append(source)
		var partial ModelContextReducer
		for i := range source {
			if source[i].Type == EntryToolCall || source[i].Type == EntryToolResult {
				source[i].Message.Content, source[i].Message.ToolArgs = "", ""
			}
		}
		gotErr := partial.Append(source)
		if (gotErr == nil) != (wantErr == nil) || corrupt && !errors.Is(gotErr, ErrAuthorityCorrupt) {
			t.Fatalf("corrupt=%v got=%v want=%v", corrupt, gotErr, wantErr)
		}
		if corrupt {
			continue
		}
		var want []session.Message
		for _, message := range complete.Messages() {
			if message.ModelContextSnapshot != nil {
				want = append(want, session.Message{EntryID: message.EntryID, ModelContextSnapshot: message.ModelContextSnapshot})
			}
		}
		if !reflect.DeepEqual(partial.ModelContexts(), want) {
			t.Fatal("managed fact projection differs from complete history")
		}
	}
}
