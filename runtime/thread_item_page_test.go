package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestItemPageReadsWithoutInstallingInterest(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/pages.sqlite"), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		t.Error("page prepared execution")
		return nil, errors.New("unexpected execution")
	}))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "pages"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	add := func(n int) {
		t.Helper()
		_, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(n)}}, sessiontree.AppendOptions{ID: fmt.Sprint("item-", n)})
		if err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 180; n++ {
		add(n)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	first, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, Direction: ThreadWindowAfter, Limit: 40})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 40 || first.Items[0].ID != "item-0" || first.Items[39].ID != "item-39" || !first.HasAfter {
		t.Fatalf("first page: %+v", first)
	}
	add(180)
	second, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AncestorLeafID: first.LeafID, AnchorID: first.LastItemID, Direction: ThreadWindowAfter, Limit: 40})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 40 || second.Items[0].ID != "item-40" || second.LeafID == first.LeafID {
		t.Fatalf("append changed page boundary: %+v", second)
	}
	before, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AnchorID: second.FirstItemID, Direction: ThreadWindowBefore, Limit: 40})
	if err != nil || len(before.Items) != 40 || before.FirstItemID != "item-0" {
		t.Fatalf("reverse page: %+v %v", before, err)
	}
	if _, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AncestorLeafID: "missing-leaf"}); err == nil {
		t.Fatal("accepted missing ancestor")
	}
	_, err = host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("long history &", 10000)}}, sessiontree.AppendOptions{ID: "large"})
	if err != nil {
		t.Fatal(err)
	}
	large, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AnchorID: "large", Direction: ThreadWindowAround, Limit: 1, PreviewBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	if len(large.Items) != 1 || large.Items[0].Body == nil || len(large.Items[0].Text) > 128 {
		t.Fatalf("large body was not bounded: %+v", large)
	}
	raw, _ := json.Marshal(large)
	if len(raw) > 512<<10 {
		t.Fatal("oversized response")
	}

	// An ancestor check must reject a branch replacement.
	if err := host.store.repo.MoveLeaf(t.Context(), id.String(), "item-39"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AncestorLeafID: second.LeafID, AnchorID: "item-39", Direction: ThreadWindowAfter}); err == nil {
		t.Fatal("accepted an ancestor from an abandoned branch")
	}
	if _, err := service.ItemPage(t.Context(), scope, ThreadItemPageInput{ThreadID: id, AncestorLeafID: "item-39", Direction: ThreadWindowBefore}); err != nil {
		t.Fatalf("active ancestor rejected: %v", err)
	}

	if len(service.updateSubscribers) != 0 {
		t.Fatal("page installed a subscription")
	}
	if _, err := service.ItemPage(t.Context(), threadquery.Scope{Authority: "revoked"}, ThreadItemPageInput{ThreadID: id}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("authorization: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ItemPage(ctx, scope, ThreadItemPageInput{ThreadID: id}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	for _, in := range []ThreadItemPageInput{{ThreadID: id, Limit: 151}, {ThreadID: id, Direction: "invalid"}, {ThreadID: id, PreviewBytes: 1}} {
		if _, err := service.ItemPage(t.Context(), scope, in); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("invalid input accepted: %+v %v", in, err)
		}
	}
}
