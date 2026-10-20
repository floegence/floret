package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

type queueHydrationRepo struct {
	sessiontree.Repo
	reader             sessiontree.JournalMetadataReader
	pages, inventories int
	failAt             int
}

func (repo *queueHydrationRepo) Entries(context.Context, string) ([]sessiontree.Entry, error) {
	repo.inventories++
	return nil, errors.New("complete inventory forbidden")
}
func (repo *queueHydrationRepo) JournalMetadata(ctx context.Context, options sessiontree.JournalMetadataOptions) (sessiontree.JournalMetadataPage, error) {
	repo.pages++
	if repo.pages == repo.failAt {
		return sessiontree.JournalMetadataPage{}, errors.New("injected inventory failure")
	}
	return repo.reader.JournalMetadata(ctx, options)
}

func TestQueueHydrationUsesBodyFreeInventoryAndRejectsPartialFailure(t *testing.T) {
	host, _ := testThreadService(t, newBlockingThreadGateway())
	const id = "queue-paged"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID = id
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 600 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint(i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("large history 中文🌸", 100)}})
	}
	queueEntry := func(id, text string) sessiontree.Entry {
		payload, _ := json.Marshal(QueuedInput{ID: id, Input: UserInput{Text: text}})
		return sessiontree.Entry{ID: id, Type: sessiontree.EntryQueueAdded, Payload: payload}
	}
	appendEntry(queueEntry("restored", "restore me"))
	appendEntry(queueEntry("pending", "keep me"))
	payload, _ := json.Marshal("restored")
	appendEntry(sessiontree.Entry{ID: "deleted", Type: sessiontree.EntryQueueDeleted, Payload: payload, Metadata: map[string]string{restoredFromBackupMetadata: "true"}})
	appendEntry(sessiontree.Entry{ID: "deleted-again", Type: sessiontree.EntryQueueDeleted, Payload: payload, Metadata: map[string]string{restoredFromBackupMetadata: "true"}})
	all, err := host.store.repo.Entries(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	want, err := canonicalQueueFromEntries(all)
	if err != nil {
		t.Fatal(err)
	}
	repo := &queueHydrationRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.JournalMetadataReader)}
	queue, restored, err := hydrateThreadQueues(t.Context(), repo, id)
	if err != nil || !reflect.DeepEqual(queue, want) || len(restored) != 2 || restored[0].Input.Text != "restore me" || restored[1].Input.Text != "restore me" || repo.pages != 3 || repo.inventories != 0 {
		t.Fatalf("queue=%+v restored=%+v pages=%d inventories=%d err=%v", queue, restored, repo.pages, repo.inventories, err)
	}
	repo.pages, repo.failAt = 0, 2
	queue, restored, err = hydrateThreadQueues(t.Context(), repo, id)
	if err == nil || queue != nil || restored != nil {
		t.Fatal("partial inventory escaped failed read")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := hydrateThreadQueues(ctx, repo, id); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
