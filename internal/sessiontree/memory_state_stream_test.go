package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

type snapshotRangeTx struct {
	spi.ReadTx
	raw            []byte
	reads, largest int
	cancel         context.CancelFunc
}

func (tx *snapshotRangeTx) NamespaceHasRecords(string) (bool, error) { return true, nil }
func (tx *snapshotRangeTx) RecordSize(string, []byte) (int64, error) { return int64(len(tx.raw)), nil }
func (tx *snapshotRangeTx) ReadRecordRange(_ string, _ []byte, offset int64, limit int) ([]byte, error) {
	tx.reads++
	tx.largest = max(tx.largest, limit)
	if tx.cancel != nil && tx.reads == 2 {
		tx.cancel()
	}
	return bytes.Clone(tx.raw[offset:min(int64(len(tx.raw)), offset+int64(limit))]), nil
}

func TestHistoricalSnapshotStreamPreservesExactMigration(t *testing.T) {
	for version := 2; version <= 5; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			payload := legacyEmptyMemoryState(t, version)
			if version == 2 {
				state, _, _ := legacyActiveSubAgentState(t)
				var err error
				payload, err = json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(payload, &fields); err != nil {
					t.Fatal(err)
				}
				fields["root_create_intents"] = json.RawMessage("{}")
				payload, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
			}
			var oracle memoryState
			if err := json.Unmarshal(payload, &oracle); err != nil {
				t.Fatal(err)
			}
			got, err := decodeMemoryStateValue(json.NewDecoder(bytes.NewReader(payload)))
			if err != nil || !reflect.DeepEqual(got, oracle) {
				t.Fatalf("stream changed exact snapshot: %v", err)
			}
			want, _, err := migrateDecodedMemoryState(oracle, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := storagecodec.EncodeEnvelope("sessiontree", payload)
			if err != nil {
				t.Fatal(err)
			}
			repo := &BackendRepo{now: time.Now}
			actual, found, _, err := repo.load(t.Context(), &snapshotRangeTx{raw: raw})
			if err != nil || !found {
				t.Fatalf("load = %v, %v", found, err)
			}
			wantBytes, _ := want.EncodeMemoryState()
			actualBytes, _ := actual.EncodeMemoryState()
			if !bytes.Equal(wantBytes, actualBytes) {
				t.Fatal("stream changed migrated state")
			}
		})
	}
}

func TestHistoricalSnapshotStreamUsesOnlyBoundedReadsAndCancels(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for i := range 200 {
		id := fmt.Sprintf("thread-%d", i)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: string(bytes.Repeat([]byte("retained fact"), 200))}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		t.Fatal(err)
	}
	tx := &snapshotRangeTx{raw: raw}
	repo := &BackendRepo{now: time.Now}
	got, found, _, err := repo.load(t.Context(), tx)
	if err != nil || !found || len(got.threads) != 200 || tx.largest > 64<<10 || tx.reads < 2 {
		t.Fatalf("bounded load: found=%v reads=%d largest=%d err=%v", found, tx.reads, tx.largest, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tx.reads, tx.cancel = 0, cancel
	if _, _, _, err := repo.load(ctx, tx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
}
