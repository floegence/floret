package sessiontree

import (
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestFirstTurnStartedIndexMatchesCanonicalScan(t *testing.T) {
	repo := NewMemoryRepo()
	repo.rebuildTurnMarkersLocked("thread")
	for i := range 300 {
		entry := Entry{ThreadID: "thread", TurnID: fmt.Sprint(i % 7), RunID: "run", Type: EntryCustom}
		if i%9 == 0 {
			entry.Type, entry.TurnStatus = EntryTurnMarker, TurnStarted
		}
		if i%11 == 0 {
			entry.Type, entry.TurnStatus = EntryTurnMarker, TurnSavePoint
		}
		entry.ID = fmt.Sprint(i)
		repo.entries["thread"] = append(repo.entries["thread"], entry)
		repo.observeTurnMarkerLocked("thread", i, entry)
		for _, exclude := range []string{"", entry.ID} {
			var want Entry
			for _, source := range repo.entries["thread"] {
				if source.TurnID == entry.TurnID && source.Type == EntryTurnMarker && source.TurnStatus == TurnStarted && source.ID != exclude {
					want = source
					break
				}
			}
			got, found := repo.firstTurnStartedLocked("thread", entry.TurnID, exclude)
			if found != (want.ID != "") || got.ID != want.ID {
				t.Fatalf("prefix=%d exclude=%s got=%s want=%s", i, exclude, got.ID, want.ID)
			}
		}
	}
}

func TestTypedTurnAdmissionChecksHistoricalMembersTransactionally(t *testing.T) {
	for _, kind := range []string{TurnKindContextCompaction, TurnKindGoalContinuation} {
		t.Run(kind, func(t *testing.T) {
			repo := newSizedMutationRepo(t, 0)
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "active", TurnID: "typed", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "existing input"}}, AppendOptions{ID: "existing-input"}); err != nil {
				t.Fatal(err)
			}
			before, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			metadata := map[string]string{TurnKindMetadataKey: kind}
			if kind == TurnKindGoalContinuation {
				metadata[ContinueGoalMetadataKey] = "1"
			}
			_, err = repo.Append(t.Context(), Entry{ThreadID: "active", TurnID: "typed", RunID: "run", RequestKey: "typed-admission", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: metadata}, AppendOptions{ID: "typed-start"})
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("invalid admission=%v", err)
			}
			after, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("rejected typed turn changed canonical state")
			}
		})
	}
}
