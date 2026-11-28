package sessiontree

import (
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestReleasedOutputDoesNotRecordItsOrderAgainstJournalEntries(t *testing.T) {
	base, repo, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "stream", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "stream")}
	if _, err := repo.BindHostWork(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Date(2027, 1, 10, 0, 0, 0, 0, time.UTC) }
	var sources []map[string]map[string][]byte
	for _, outputFirst := range []bool{false, true} {
		backend := &migrationTestBackend{records: cloneMigrationRecords(base.records)}
		var owner *BackendRepo
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			owner, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, clock, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		appendOutput := func() {
			_, err := owner.AppendHostWorkOutput(t.Context(), HostWorkOutputAppend{ThreadID: "thread", WorkID: work.ID, ExecutorID: work.ExecutorID, Text: "trace"})
			if err != nil {
				t.Fatal(err)
			}
		}
		if outputFirst {
			appendOutput()
		}
		request := canonicalEffectRequest(attempt)
		request.Now = clock()
		settled, err := owner.FinishEffectDispatch(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		readBytes := func(thread string) int64 {
			page, err := owner.ReadHostWorkOutputPage(t.Context(), HostWorkOutputRequest{ThreadID: thread, WorkID: work.ID, MaxBytes: 128})
			if err != nil {
				t.Fatal(err)
			}
			return page.Source.Bytes
		}
		atEntry := readBytes("thread")
		if !outputFirst {
			appendOutput()
		}
		sources = append(sources, cloneMigrationRecords(backend.records))
		_, err = owner.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", EntryID: settled.Result.ID, EntryIDPinned: true, Now: clock()})
		if err != nil {
			t.Fatal(err)
		}
		atFork := readBytes("fork")
		if atFork != 5 || (atEntry == 5) != outputFirst || (atEntry == 0) == outputFirst {
			t.Fatalf("outputFirst=%v bytes at entry=%d at fork=%d", outputFirst, atEntry, atFork)
		}
		t.Logf("outputFirst=%v bytes at entry=%d at fork=%d", outputFirst, atEntry, atFork)
	}
	if !reflect.DeepEqual(sources[0], sources[1]) {
		t.Fatal("different canonical records, histories were distinguishable")
	}
}
