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

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

type windowCostTx struct {
	spi.ReadTx
	gets, batches, rows, bytes int
	failure                    error
}

func (tx *windowCostTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.gets++
	if tx.failure != nil {
		return nil, tx.failure
	}
	value, err := tx.ReadTx.Get(namespace, key)
	tx.bytes += len(value)
	tx.rows++
	return value, err
}
func (tx *windowCostTx) ReadFloretRecords(request storagebridge.RecordBatch) ([][]byte, error) {
	tx.batches++
	if tx.failure != nil {
		return nil, tx.failure
	}
	values, err := storagebridge.ReadRecords(tx.ReadTx, request)
	for _, value := range values {
		tx.rows++
		tx.bytes += len(value)
	}
	return values, err
}

func TestProjectionSourceBatchPreservesCapabilityOwnershipAndErrors(t *testing.T) {
	backend := newMigrationTestBackend()
	for _, namespace := range []string{activityFieldsNamespace, canonicalBodyNamespace} {
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			for _, key := range []string{"a", "b"} {
				if err := tx.Put(namespace, []byte(key), []byte(key)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := backend.View(t.Context(), func(raw spi.ReadTx) error {
			counted := &windowCostTx{ReadTx: raw}
			tx := newProjectionSourceTx(counted)
			if newProjectionSourceTx(tx) != tx {
				t.Fatal("nested projection discarded the shared transaction reader")
			}
			a, err := tx.Get(namespace, []byte("a"))
			if err != nil {
				return err
			}
			a[0] = 'x'
			values, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: namespace, Keys: [][]byte{[]byte("b"), []byte("a"), []byte("b")}})
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(values, [][]byte{[]byte("b"), []byte("a"), []byte("b")}) || counted.batches != 1 {
				t.Fatalf("batch/ordering changed: %q, batches=%d", values, counted.batches)
			}
			values[0][0] = 'z'
			if string(values[2]) != "b" {
				t.Fatal("duplicate batch results alias")
			}
			value, err := tx.Get(namespace, []byte("b"))
			if err != nil || string(value) != "b" {
				t.Fatalf("caller mutated memo: %q %v", value, err)
			}
			for _, request := range []storagebridge.RecordBatch{{}, {Namespace: namespace, Keys: [][]byte{nil}}, {Namespace: namespace, Keys: make([][]byte, storagebridge.MaxRecordBatch+1)}} {
				if _, err := tx.ReadFloretRecords(request); !errors.Is(err, spi.ErrInvalidArgument) {
					t.Fatalf("invalid batch: %v", err)
				}
			}
			counted.failure = context.Canceled
			if _, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: namespace, Keys: [][]byte{[]byte("missing")}}); !errors.Is(err, context.Canceled) {
				t.Fatalf("batch failure changed: %v", err)
			}
			before := counted.gets
			counted.failure = nil
			if _, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: namespace, Keys: [][]byte{[]byte("missing")}}); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("missing record: %v", err)
			}
			if counted.gets != before {
				t.Fatal("failed batch silently retried with Get")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProjectionSourceMemoIsBoundedAndTransactionLocal(t *testing.T) {
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for i := range 6 {
			if err := tx.Put(activityFieldsNamespace, []byte(fmt.Sprint(i)), bytes.Repeat([]byte("a"), projectionSourceReadBytes/3)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(raw spi.ReadTx) error {
		tx := newProjectionSourceTx(raw)
		for i := range 6 {
			if _, err := tx.Get(activityFieldsNamespace, []byte(fmt.Sprint(i))); err != nil {
				return err
			}
		}
		if tx.bytes > projectionSourceReadBytes || len(tx.values) >= 6 {
			t.Fatalf("unbounded memo: bytes=%d records=%d", tx.bytes, len(tx.values))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Delete(activityFieldsNamespace, []byte("0")) }); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(raw spi.ReadTx) error {
		_, err := newProjectionSourceTx(raw).Get(activityFieldsNamespace, []byte("0"))
		return err
	}); !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("previous transaction hid deletion: %v", err)
	}
}

func TestWindowProjectionReadCostAndMemoEquivalence(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 50)
	for i := range 100 {
		call := fmt.Sprint("cost-tool-", i)
		for _, result := range []bool{false, true} {
			entry := Entry{ID: fmt.Sprint(call, "-", result), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: call, ToolName: "inspect", ToolArgs: "{}", Activity: &tools.ActivityPresentation{Label: "Inspect", Description: "Read source", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: fmt.Sprint("Result ", i), Rows: []tools.StructuredActivityRow{{Title: "Hidden", Format: tools.StructuredActivityRowFormatCode, Content: strings.Repeat("never load hidden content 🌸", 2000)}}}}}}
			if result {
				entry.Type, entry.Message.Role, entry.Message.ToolResult = EntryToolResult, session.Tool, &session.ToolResultView{Status: "success"}
			}
			if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatalf("append %s result=%v: %v", call, result, err)
			}
		}
	}
	opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 150, ActivityFields: true}
	fields := func(PathMetadata, bool) []string { return nil }
	// Prepare with the normal owner before comparing one frozen snapshot.
	if _, err := repo.ReadWindowProjection(t.Context(), opts, fields); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	var counts [2]*windowCostTx
	var outputs [2]WindowProjection
	for mode := range 2 {
		if err := repo.canonicalThreadView(t.Context(), "thread", func(raw spi.ReadTx, meta ThreadMeta) error {
			counted := &windowCostTx{ReadTx: raw}
			counts[mode] = counted
			tx := newProjectionSourceTx(counted)
			if mode == 0 {
				tx.bytes = projectionSourceReadBytes
			} // Retention disabled; identical reducer and authority checks.
			var err error
			outputs[mode], err = readWindowProjection(t.Context(), tx, meta, opts, fields, false)
			return err
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := json.Marshal(outputs[0])
	b, _ := json.Marshal(outputs[1])
	if !bytes.Equal(a, b) || len(outputs[1].Membership.Members) != 150 || len(outputs[1].ActivitySummaries) != 100 {
		t.Fatal("memo changed membership, activity or canonical output")
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("read optimization changed durable records")
	}
	t.Logf("uncached gets=%d rows=%d bytes=%d; memo gets=%d rows=%d bytes=%d batches=%d", counts[0].gets, counts[0].rows, counts[0].bytes, counts[1].gets, counts[1].rows, counts[1].bytes, counts[1].batches)
	if counts[1].gets*100 > counts[0].gets*60 || counts[1].bytes >= counts[0].bytes {
		t.Fatal("window still repeats most source reads")
	}
}

func TestProjectionSourceKeepsBodyPrefetchBatched(t *testing.T) {
	backend := newMigrationTestBackend()
	var refs []canonicalBodyRef
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for i := range 150 {
			ref, err := retainCanonicalBody(t.Context(), tx, fmt.Sprint("exact body 🌸 ", i))
			if err != nil {
				return err
			}
			refs = append(refs, ref)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(raw spi.ReadTx) error {
		counted := &windowCostTx{ReadTx: raw}
		reader := newCanonicalBodyPageReader(t.Context(), newProjectionSourceTx(counted))
		if err := reader.prefetch(refs); err != nil {
			return err
		}
		for i, ref := range refs {
			value, err := reader.read(ref)
			if err != nil || value != fmt.Sprint("exact body 🌸 ", i) {
				t.Fatalf("body %d: %q %v", i, value, err)
			}
		}
		if counted.batches != 2 || counted.gets != 0 || counted.rows != 300 {
			t.Fatalf("wrapper lost physical batching: batches=%d gets=%d rows=%d", counted.batches, counted.gets, counted.rows)
		}
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		reader.ctx = cancelled
		if _, err := reader.read(refs[0]); !errors.Is(err, context.Canceled) {
			t.Fatalf("body memo ignored cancellation: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(refs[0].SHA256, 0))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		return newCanonicalBodyPageReader(t.Context(), newProjectionSourceTx(tx)).prefetch(refs)
	}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("batch hid missing source block: %v", err)
	}
}
