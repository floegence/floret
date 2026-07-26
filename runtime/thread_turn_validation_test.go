package runtime

import (
	"fmt"
	"testing"
	"time"
)

func TestThreadTurnSnapshotValidateUsesStoredAttachmentShape(t *testing.T) {
	now := time.Now().UTC()
	attachments := make([]MessageAttachment, MaxMessageAttachmentsPerTurn+1)
	for index := range attachments {
		attachments[index] = MessageAttachment{
			ResourceRef: fmt.Sprintf("legacy-resource:%d", index), Name: fmt.Sprintf("legacy-%d.txt", index),
			MIMEType: "text/plain", SizeBytes: 1,
		}
	}
	turn := ThreadTurnSnapshot{
		TurnID: "turn", RunID: "run", Ordinal: 1, StartedAt: now, UpdatedAt: now,
		UserEntryID: "user", UserMessageOrigin: ThreadUserMessageOriginUser, UserAttachments: attachments,
		Status: TurnStatusCompleted, ThroughOrdinal: 2,
		Projection: ThreadTurnProjection{
			ThreadID: "thread", TurnID: "turn", RunID: "run", TraceID: "run",
			Status: TurnStatusCompleted, ThroughOrdinal: 2, ProjectedAt: now,
		},
	}
	if err := turn.Validate(); err != nil {
		t.Fatalf("stored legacy attachments failed validation: %v", err)
	}

	turn.UserAttachments[0].ResourceRef = ""
	if err := turn.Validate(); err == nil {
		t.Fatal("invalid stored attachment passed validation")
	}
}

func TestPublicThreadTurnValidatorsRejectContradictoryShapes(t *testing.T) {
	now := time.Now().UTC()
	turn := ThreadTurnSnapshot{
		TurnID: "turn", RunID: "run", Ordinal: 1, StartedAt: now, UpdatedAt: now,
		UserEntryID: "user", UserMessageOrigin: ThreadUserMessageOriginUser, UserInput: "input",
		Status: TurnStatusCompleted, ThroughOrdinal: 2,
		Projection: ThreadTurnProjection{
			ThreadID: "thread", TurnID: "turn", RunID: "run", TraceID: "run",
			Status: TurnStatusCompleted, ThroughOrdinal: 2, ProjectedAt: now,
		},
	}
	if err := turn.Validate(); err != nil {
		t.Fatal(err)
	}

	recoverable := turn
	recoverable.Recoverable = true
	if err := recoverable.Validate(); err == nil {
		t.Fatal("completed recoverable turn passed validation")
	}

	invalidSignal := turn
	invalidSignal.ControlSignals = []ThreadControlSignal{{Name: "ask_user", CallID: "call", ArgsHash: "hash", Disposition: "unknown"}}
	if err := invalidSignal.Validate(); err == nil {
		t.Fatal("unknown control signal disposition passed validation")
	}

	page := ThreadTurnsPage{ThreadID: "thread", Turns: []ThreadTurnSnapshot{turn}, ThroughOrdinal: 2, GeneratedAt: now}
	if err := page.Validate(); err != nil {
		t.Fatal(err)
	}
	duplicate := turn
	duplicate.Ordinal = 2
	duplicate.ThroughOrdinal = 3
	duplicate.Projection.ThroughOrdinal = 3
	page.Turns = append(page.Turns, duplicate)
	page.ThroughOrdinal = 3
	if err := page.Validate(); err == nil {
		t.Fatal("duplicate turn identity passed page validation")
	}

	thread := ThreadSnapshot{
		ID: "thread", CreatedAt: now, UpdatedAt: now, Phase: ThreadPhaseIdle, Status: ThreadStatusCompleted,
		LatestTurnID: "turn", LatestRunID: "run", ThroughOrdinal: 2, CanAppendMessage: true,
	}
	overview := ThreadOverview{Thread: thread, LatestTurn: &turn}
	if err := overview.Validate(); err != nil {
		t.Fatal(err)
	}
	overview.LatestTurn.TurnID = "other"
	if err := overview.Validate(); err == nil {
		t.Fatal("overview with mismatched latest identity passed validation")
	}
}
