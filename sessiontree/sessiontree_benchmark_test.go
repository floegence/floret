package sessiontree

import (
	"fmt"
	"testing"

	"github.com/floegence/floret/session"
)

func BenchmarkBuildContextTenThousandEntriesWithCompactions(b *testing.B) {
	path := make([]Entry, 0, 10000)
	var lastCompactionID string
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("entry-%05d", i)
		parentID := ""
		if i > 0 {
			parentID = path[i-1].ID
		}
		entry := Entry{
			ID:       id,
			ThreadID: "thread",
			ParentID: parentID,
			Type:     EntryUserMessage,
			Message:  session.Message{Role: session.User, Content: fmt.Sprintf("message %05d", i)},
		}
		if i > 0 && i%1000 == 0 {
			entry.Type = EntryCompaction
			entry.Message = session.Message{}
			entry.CompactionID = fmt.Sprintf("compaction-%02d", i/1000)
			entry.PreviousCompactionID = lastCompactionID
			entry.CompactedThroughEntryID = path[i-900].ID
			entry.FirstKeptEntryID = path[i-100].ID
			entry.CompactionGeneration = i / 1000
			entry.CompactionWindowID = entry.CompactionID
			entry.Summary = fmt.Sprintf("summary through %05d", i)
			lastCompactionID = entry.CompactionID
		}
		path = append(path, entry)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		messages := BuildContext(path, ContextOptions{})
		if !containsCompactionSummary(messages) {
			b.Fatalf("missing compaction summary in context: %#v", messages)
		}
	}
}

func containsCompactionSummary(messages []session.Message) bool {
	for _, msg := range messages {
		if msg.Kind == session.MessageKindCompactionSummary {
			return true
		}
	}
	return false
}
