package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

func installSnapshotImportFixture(t *testing.T, payload []byte) (*migrationTestBackend, []byte) {
	t.Helper()
	backend := newMigrationTestBackend()
	raw, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, backendStateKey, raw) }); err != nil {
		t.Fatal(err)
	}
	return backend, raw
}

func TestSnapshotImportIndexesExactLegacySources(t *testing.T) {
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
			}
			backend, raw := installSnapshotImportFixture(t, payload)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				checkpoint, err := indexHistoricalSnapshotBatch(t.Context(), tx)
				if err == nil && (!checkpoint.Complete || checkpoint.Version != version) {
					t.Fatalf("checkpoint=%+v", checkpoint)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var state memoryState
				if err := decodeSnapshotSourceValue(t.Context(), tx, &state, "payload"); err != nil {
					return err
				}
				var expected memoryState
				if err := json.Unmarshal(payload, &expected); err != nil {
					return err
				}
				if !reflect.DeepEqual(state, expected) {
					t.Fatal("source index changed historical bytes")
				}
				stored, err := tx.Get(backendDomainNamespace, backendStateKey)
				if err == nil && !bytes.Equal(stored, raw) {
					t.Fatal("indexing modified canonical source")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotImportResumesInsideLargeBody(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	entry, err := memory.Append(t.Context(), Entry{ID: "large", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("preserved 中文🌸\n", 50000)}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, raw := installSnapshotImportFixture(t, payload)
	batches := 0
	var previous int64
	for {
		var checkpoint snapshotImportCheckpoint
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			checkpoint, err = indexHistoricalSnapshotBatch(t.Context(), tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		batches++
		if checkpoint.Scan.Offset-previous > backendThreadMigrationBatchBytes {
			t.Fatal("unbounded source scan")
		}
		previous = checkpoint.Scan.Offset
		if checkpoint.Complete {
			break
		}
		if batches > 100 {
			t.Fatal("source scan did not finish")
		}
		before := cloneMigrationRecords(backend.records)
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		if err := backend.Update(cancelled, func(tx spi.WriteTx) error { _, err := indexHistoricalSnapshotBatch(cancelled, tx); return err }); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel=%v", err)
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatal("cancelled scan committed progress")
		}
	}
	if batches < 3 {
		t.Fatal("large snapshot did not exercise checkpoint resume")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var got Entry
		if err := decodeSnapshotSourceValue(t.Context(), tx, &got, "payload", "entries", "history", "0"); err != nil {
			return err
		}
		wantJSON, _ := json.Marshal(entry)
		gotJSON, _ := json.Marshal(got)
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Fatal("entry span lost facts")
		}
		retained, err := tx.Get(backendDomainNamespace, backendStateKey)
		if err == nil && !bytes.Equal(retained, raw) {
			t.Fatal("source changed")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotImportRejectsAmbiguousEnvelopeWithoutMutation(t *testing.T) {
	payload := legacyEmptyMemoryState(t, 5)
	for _, change := range []func(string) string{
		func(raw string) string {
			return strings.Replace(raw, "\"kind\":\"sessiontree\"", "\"kind\":\"sessiontree\",\"kind\":\"sessiontree\"", 1)
		},
		func(raw string) string {
			return strings.Replace(raw, "\"threads\":{}", "\"threads\":{},\"threads\":{}", 1)
		},
		func(raw string) string { return raw + "{}" },
		func(raw string) string { return raw[:len(raw)-1] },
	} {
		backend, raw := installSnapshotImportFixture(t, payload)
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainNamespace, backendStateKey, []byte(change(string(raw))))
		}); err != nil {
			t.Fatal(err)
		}
		before := cloneMigrationRecords(backend.records)
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := indexHistoricalSnapshotBatch(t.Context(), tx); return err }); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("error=%v", err)
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatal("malformed source committed index")
		}
	}
}

type snapshotCheckpointFailureTx struct {
	spi.WriteTx
	mode   string
	cancel context.CancelFunc
}

func (tx snapshotCheckpointFailureTx) Put(namespace string, key, value []byte) error {
	if namespace == snapshotImportNamespace && bytes.Equal(key, snapshotImportCheckpointKey) {
		switch tx.mode {
		case "write":
			return errors.New("checkpoint write failed")
		case "panic":
			panic("checkpoint write panic")
		case "cancel":
			tx.cancel()
			return context.Canceled
		}
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestSnapshotImportRollsBackAllLocatorsWithCheckpoint(t *testing.T) {
	for _, mode := range []string{"write", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			backend, _ := installSnapshotImportFixture(t, legacyEmptyMemoryState(t, 5))
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				if mode == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("missing injected panic")
						}
					}()
				}
				err := backend.Update(ctx, func(tx spi.WriteTx) error {
					_, err := indexHistoricalSnapshotBatch(ctx, snapshotCheckpointFailureTx{WriteTx: tx, mode: mode, cancel: cancel})
					return err
				})
				if err == nil {
					t.Fatal("failed checkpoint committed")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed checkpoint retained locators")
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				checkpoint, err := indexHistoricalSnapshotBatch(t.Context(), tx)
				if err == nil && !checkpoint.Complete {
					t.Fatal("retry incomplete")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			completed := cloneMigrationRecords(backend.records)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := indexHistoricalSnapshotBatch(t.Context(), tx); return err }); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(completed, backend.records) {
				t.Fatal("completed scan rewrote state")
			}
		})
	}
}

func TestSnapshotImportRejectsCorruptCheckpointWithoutMutation(t *testing.T) {
	backend, raw := installSnapshotImportFixture(t, legacyEmptyMemoryState(t, 5))
	checkpoint := snapshotImportCheckpoint{SourceBytes: int64(len(raw)), Scan: storagecodec.JSONScanState{Offset: 1, Started: true, Token: 's', TokenKey: true}}
	encoded, _ := json.Marshal(checkpoint)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(snapshotImportNamespace, snapshotImportCheckpointKey, encoded)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := indexHistoricalSnapshotBatch(t.Context(), tx); return err }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("corrupt checkpoint changed source or index")
	}
}

func TestSnapshotImportRejectsChangedSpanningValueAfterResume(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "large", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("preserved fact ", 80000)}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, raw := installSnapshotImportFixture(t, payload)
	var partial snapshotImportCheckpoint
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		partial, err = indexHistoricalSnapshotBatch(t.Context(), tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if partial.Complete || len(partial.Digests) == 0 {
		t.Fatal("fixture did not checkpoint a spanning value")
	}
	// Keep the record size unchanged while changing bytes before the checkpoint.
	changed := bytes.Replace(raw, []byte("preserved fact"), []byte("replaced  fact"), 1)
	if len(changed) != len(raw) || bytes.Equal(changed, raw) {
		t.Fatal("invalid mutation fixture")
	}
	if offset := bytes.Index(raw, []byte("preserved fact")); int64(offset) >= partial.Scan.Offset {
		t.Fatal("mutation must precede the checkpoint")
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, backendStateKey, changed) }); err != nil {
		t.Fatal(err)
	}
	for attempts := 0; attempts < 100; attempts++ {
		before := cloneMigrationRecords(backend.records)
		var next snapshotImportCheckpoint
		err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			next, err = indexHistoricalSnapshotBatch(t.Context(), tx)
			return err
		})
		if err != nil {
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("source drift error = %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("source verification committed on mismatch")
			}
			return
		}
		if next.Complete {
			t.Fatal("changed source became publishable")
		}
	}
	t.Fatal("changed source verification did not finish")
}

func TestSnapshotImportRejectsInvalidDigestCheckpoint(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("body", 100000)}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, payload)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		checkpoint, err := indexHistoricalSnapshotBatch(t.Context(), tx)
		if err != nil {
			return err
		}
		if len(checkpoint.Digests) == 0 {
			t.Fatal("missing partial digests")
		}
		checkpoint.Digests[0].State = []byte("invalid")
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			return err
		}
		return tx.Put(snapshotImportNamespace, snapshotImportCheckpointKey, raw)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := indexHistoricalSnapshotBatch(t.Context(), tx); return err }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("checkpoint error = %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("invalid digest checkpoint committed")
	}
}

func TestSnapshotImportPreservesEnvelopeWhitespace(t *testing.T) {
	backend, raw := installSnapshotImportFixture(t, legacyEmptyMemoryState(t, 5))
	raw = append(append([]byte(" \n\t"), raw...), '\n', ' ')
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, backendStateKey, raw) }); err != nil {
		t.Fatal(err)
	}
	indexSnapshotForTest(t, backend)
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		checkpoint, found, err := readSnapshotImportCheckpoint(tx)
		if err == nil && (!found || !checkpoint.Complete || checkpoint.VerifyOffset != int64(len(raw))) {
			t.Fatal("whitespace lost source extent")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
