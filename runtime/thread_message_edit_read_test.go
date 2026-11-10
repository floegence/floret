package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
)

// Opening a thread must never hydrate its entire journal for an edit action.
type editReadRepo struct {
	*coldSummaryRepo
	complete int
}

func (r *editReadRepo) Path(context.Context, string, string) ([]sessiontree.Entry, error) {
	return nil, errors.New("edit eligibility materialized complete history")
}
func (r *editReadRepo) PathEntriesWindow(ctx context.Context, o sessiontree.PathWindowOptions) (sessiontree.PathEntriesWindow, error) {
	r.complete += o.Limit
	return r.BackendKernel.PathEntriesWindow(ctx, o)
}

func TestLatestEditableMessageReadsOnlySelectedInput(t *testing.T) {
	for _, tail := range []int{2, 600} {
		t.Run(fmt.Sprint(tail), func(t *testing.T) {
			host, service := testThreadService(t, newBlockingThreadGateway())
			const id = "bounded-edit"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id}); err != nil {
				t.Fatal(err)
			}
			add := func(e sessiontree.Entry) {
				t.Helper()
				e.ThreadID, e.TurnID, e.RunID = id, "turn", "run"
				if _, err := host.store.repo.Append(t.Context(), e, sessiontree.AppendOptions{ID: e.ID}); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 300 {
				add(sessiontree.Entry{ID: fmt.Sprint("old-", i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("unrelated history 🌸", 1000)}})
			}
			add(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
			input := UserInput{Text: strings.Repeat("complete input 🌸", 2000), References: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "note", Text: "exact captured text"}}}
			add(sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: canonicalEditInput(input)})
			for i := range tail {
				add(sessiontree.Entry{ID: fmt.Sprint("answer-", i), Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("large output 🌸", 1000)}})
			}
			repo := &editReadRepo{coldSummaryRepo: &coldSummaryRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}}
			host.store.repo = repo
			got, err := service.(ThreadMessageEditor).LatestEditableMessage(t.Context(), id)
			if err != nil || got.ItemID != "user" || !reflect.DeepEqual(got.Input, input) {
				t.Fatalf("input identity/content changed: %s %v", got.ItemID, err)
			}
			if repo.complete != 1 || repo.pages > (tail+2+127)/128+1 {
				t.Fatalf("unbounded read: complete=%d pages=%d", repo.complete, repo.pages)
			}
			ctx, cancel := context.WithCancel(t.Context())
			repo.pages = 0
			repo.afterPage = func(int) { cancel() }
			if _, err := service.(ThreadMessageEditor).LatestEditableMessage(ctx, id); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled lookup: %v", err)
			}
			repo.afterPage = func(int) {
				if err := repo.MoveLeaf(t.Context(), id, "old-0"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.(ThreadMessageEditor).LatestEditableMessage(t.Context(), id); err == nil {
				t.Fatal("accepted replaced active path")
			}
		})
	}
}

func TestLatestEditableMessageRetainsCanonicalBarriers(t *testing.T) {
	for _, barrier := range []string{"none", "unknown", "steering", "delegated", "goal", "child", "old-child", "closed-child", "unbound-child", "later-turn-child"} {
		t.Run(barrier, func(t *testing.T) {
			host, service := testThreadService(t, newBlockingThreadGateway())
			const id = "edit-barriers"
			_, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id})
			if err != nil {
				t.Fatal(err)
			}
			add := func(e sessiontree.Entry) {
				t.Helper()
				e.ThreadID = id
				e.RunID = "run"
				if e.TurnID == "" {
					e.TurnID = "turn"
				}
				if _, err := host.store.repo.Append(t.Context(), e, sessiontree.AppendOptions{ID: e.ID}); err != nil {
					t.Fatal(err)
				}
			}
			add(sessiontree.Entry{ID: "old-failure", Type: sessiontree.EntryRunFailure, Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureEffectOutcomeUnknown}})
			add(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
			user := sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "exact"}}
			if barrier == "delegated" {
				user.Metadata = map[string]string{sessiontree.SubAgentUserMessageOriginMetadataKey: "delegated_mission"}
			}
			if barrier == "goal" {
				user.RequestKey = "goal-submit:key"
			}
			add(user)
			switch barrier {
			case "unknown":
				add(sessiontree.Entry{ID: "failure", Type: sessiontree.EntryRunFailure, Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureEffectOutcomeUnknown}})
			case "steering":
				add(sessiontree.Entry{ID: "request", RequestKey: "steer", RequestFingerprint: "steer-fp", Type: sessiontree.EntrySteeringRequested, Message: session.Message{Role: session.User, Content: "steer"}})
				add(sessiontree.Entry{ID: "steering", Type: sessiontree.EntrySteeringApplied, Payload: []byte(`{"steering_id":"request"}`)})
			case "later-turn-child":
				add(sessiontree.Entry{ID: "compact", TurnID: "later", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
			}
			if strings.Contains(barrier, "child") {
				child := sessiontree.ThreadMeta{ID: "child", ParentThreadID: id, ParentTurnID: "turn", TaskName: "child", HostProfileRef: "test"}
				switch barrier {
				case "old-child":
					child.ParentTurnID = "old"
				case "closed-child":
					child.Lifecycle = sessiontree.ThreadLifecycleClosed
				case "unbound-child":
					child.ParentTurnID = ""
				case "later-turn-child":
					child.ParentTurnID = "later"
				}
				if _, err := host.store.repo.CreateThread(t.Context(), child); err != nil {
					t.Fatal(err)
				}
			}
			_, err = service.(ThreadMessageEditor).LatestEditableMessage(t.Context(), id)
			switch barrier {
			case "none", "old-child", "closed-child":
				if err != nil {
					t.Fatal(err)
				}
			default:
				if err == nil {
					t.Fatalf("accepted %s", barrier)
				}
			}
		})
	}
}
