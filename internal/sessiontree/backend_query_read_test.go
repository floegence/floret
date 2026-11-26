package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

type queryCandidateBackend struct {
	spi.Backend
	reads int
}

func TestStatelessQueryContentReadsWithoutIssuingHandles(t *testing.T) {
	ctx := t.Context()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("x", 7999) + "\U0001F338" + strings.Repeat("z", 9000)
	if _, err := memory.Append(ctx, Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{ID: "immutable"}); err != nil {
		t.Fatal(err)
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"thread"}}
	rows, err := repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.Messages, IncludeContent: true})
	if err != nil || len(rows.Records) != 1 || rows.Records[0].Content == nil || len(*rows.Records[0].Content) == 0 {
		t.Fatalf("rows %v %v", rows, err)
	}
	in := (*rows.Records[0].Content)[0].Read
	repo.threadQuerySnapshots = nil
	var got strings.Builder
	for next := &in; next != nil; {
		page, err := repo.ReadQueryContent(ctx, scope, *next)
		if err != nil || len(page.Text) > 8000 || len(page.Text) == 0 {
			t.Fatalf("read %v", err)
		}
		got.WriteString(page.Text)
		next = page.NextRead
	}
	if got.String() != text || len(repo.threadQuerySnapshots.snapshots) != 0 {
		t.Fatal("missing bytes or allocated snapshot")
	}
	for _, pair := range [][2]int64{{8000, 8000}, {0, 8000}, {0, int64(len(text)) + 1}, {2, 1}, {-1, 0}} {
		bad := in
		bad.StartByte, bad.EndByte = pair[0], pair[1]
		if _, err := repo.ReadQueryContent(ctx, scope, bad); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("bad range %v: %v", pair, err)
		}
	}
	in.StartByte = in.EndByte
	empty, err := repo.ReadQueryContent(ctx, scope, in)
	if err != nil || empty.Text != "" || empty.NextRead != nil {
		t.Fatal("empty range", err)
	}
	scope.ThreadIDs = nil
	if _, err := repo.ReadQueryContent(ctx, scope, in); err == nil {
		t.Fatal("revoked scope read data")
	}
}

func (backend *queryCandidateBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(queryCandidateTx{tx, backend}) })
}

type queryCandidateTx struct {
	spi.ReadTx
	backend *queryCandidateBackend
}

func (tx queryCandidateTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.backend.reads++
	return tx.ReadTx.Get(namespace, key)
}
func (tx queryCandidateTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace != queryDocumentNamespace {
		return spi.ScanPage{}, errors.New("query scanned a complete canonical journal")
	}
	return tx.ReadTx.Scan(request)
}

func TestV32QueryBrowsesMetadataAndMatchesCanonicalSourcesOnDemand(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		content := "unrelated retained history"
		if i == 250 {
			content = "unique matching needle"
		}
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: content}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	repo.threadMemory, repo.verifiedThreadLeaves = nil, nil
	counted := &queryCandidateBackend{Backend: base}
	repo.backend = counted
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"thread"}}
	browse, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5})
	if err != nil || browse.Total != 512 || len(browse.Records) != 5 {
		t.Fatalf("browse=%+v, %v", browse, err)
	}
	if counted.reads > 60 {
		t.Fatalf("metadata browse read %d canonical records", counted.reads)
	}
	counted.reads = 0
	filter := threadquery.Filter{Text: threadquery.TextMatch{Contains: "needle"}}
	page, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Filter: filter, Limit: 5})
	if err != nil || page.Total != 1 || len(page.Records) != 1 {
		t.Fatalf("query=%+v, %v", page, err)
	}
	if _, err := repo.Read(t.Context(), scope, threadquery.ReadInput{Ref: page.Records[0].Ref}); err != nil {
		t.Fatal(err)
	}
	aggregate, err := repo.Aggregate(t.Context(), scope, threadquery.AggregateInput{Object: threadquery.Messages, Filter: filter})
	if err != nil || aggregate.Total != 1 {
		t.Fatalf("aggregate=%+v, %v", aggregate, err)
	}
	if counted.reads < 512 || counted.reads > 16*512 {
		t.Fatalf("explicit text query read %d canonical records", counted.reads)
	}
	if repo.threadMemory != nil {
		t.Fatal("query retained a complete journal")
	}
}
