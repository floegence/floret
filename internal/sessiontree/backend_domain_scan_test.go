package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

type domainScanTestReader struct {
	spi.ReadTx
	scan func(spi.ScanRequest) (spi.ScanPage, error)
}

func (reader domainScanTestReader) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	return reader.scan(request)
}

func TestDomainVisitConsumesEachPageBeforeReadingNext(t *testing.T) {
	calls, visited := 0, 0
	reader := domainScanTestReader{scan: func(request spi.ScanRequest) (spi.ScanPage, error) {
		if visited != calls*2 {
			t.Fatalf("read ahead: calls=%d visited=%d", calls, visited)
		}
		if request.Namespace != backendDomainV28Namespace || request.Limit != 2 {
			t.Fatal("unbounded scan request")
		}
		calls++
		first := calls*2 - 1
		last := fmt.Sprintf("%04d", first+1)
		return spi.ScanPage{Records: []spi.Record{{Key: []byte(fmt.Sprintf("%04d", first))}, {Key: []byte(last)}}, Next: []byte(last), HasMore: calls < 3}, nil
	}}
	format := backendDomainV28Format
	format.scanLimit = 2
	found, err := visitBackendDomain(t.Context(), reader, format, func(record spi.Record) error {
		visited++
		if string(record.Key) != fmt.Sprintf("%04d", visited) {
			t.Fatal("record order changed")
		}
		return nil
	})
	if err != nil || !found || calls != 3 || visited != 6 {
		t.Fatalf("found=%t calls=%d visited=%d err=%v", found, calls, visited, err)
	}
}

func TestDomainVisitStopsOnCancellationFailureAndStalledCursor(t *testing.T) {
	injected := errors.New("injected scan failure")
	for _, mode := range []string{"cancel", "callback", "read", "stalled", "empty_cursor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, visited := 0, 0
			reader := domainScanTestReader{scan: func(spi.ScanRequest) (spi.ScanPage, error) {
				calls++
				if mode == "read" {
					return spi.ScanPage{}, injected
				}
				next := []byte("key")
				if mode == "empty_cursor" {
					next = nil
				}
				return spi.ScanPage{Records: []spi.Record{{Key: []byte("key")}, {Key: []byte("later")}}, Next: next, HasMore: true}, nil
			}}
			_, err := visitBackendDomain(ctx, reader, backendDomainV28Format, func(spi.Record) error {
				visited++
				if mode == "cancel" {
					cancel()
				}
				if mode == "callback" {
					return injected
				}
				return nil
			})
			switch mode {
			case "cancel":
				if !errors.Is(err, context.Canceled) || visited != 1 || calls != 1 {
					t.Fatalf("continued cancelled scan: %d %d %v", calls, visited, err)
				}
			case "callback", "read":
				if !errors.Is(err, injected) || calls != 1 {
					t.Fatalf("continued failed scan: %d %v", calls, err)
				}
			default:
				if !errors.Is(err, ErrAuthorityCorrupt) || calls > 2 {
					t.Fatalf("stalled scan not rejected: %d %v", calls, err)
				}
			}
		})
	}
}

func TestDomainLoaderRejectsCorruptionOnLaterPage(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		appendPathTestEntry(t, memory, fmt.Sprint(i), "")
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV28(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		calls := 0
		reader := domainScanTestReader{ReadTx: tx, scan: func(request spi.ScanRequest) (spi.ScanPage, error) {
			page, err := tx.Scan(request)
			calls++
			if calls == 3 && len(page.Records) > 0 {
				page.Records[0].Value = []byte("corrupt")
			}
			return page, err
		}}
		format := backendDomainV28Format
		format.scanLimit = 2
		if _, _, err := loadBackendDomain(t.Context(), reader, time.Now, format); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("adopted later corrupt page: %v", err)
		}
		if calls != 3 {
			t.Fatalf("continued after corrupt page: %d", calls)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("read changed canonical bytes")
	}
}

func TestDomainScanCollectorDetachesReusedPages(t *testing.T) {
	key, value := []byte("a"), []byte("first")
	calls := 0
	reader := domainScanTestReader{scan: func(spi.ScanRequest) (spi.ScanPage, error) {
		calls++
		if calls == 2 {
			key[0] = 'b'
			copy(value, "later")
		}
		return spi.ScanPage{Records: []spi.Record{{Key: key, Value: value}}, Next: key, HasMore: calls == 1}, nil
	}}
	records, err := scanBackendDomain(t.Context(), reader, backendDomainV28Format)
	if err != nil || len(records) != 2 || !bytes.Equal(records[0].Value, []byte("first")) || string(records[0].Key) != "a" || string(records[1].Key) != "b" {
		t.Fatalf("collector reused page memory: %+v %v", records, err)
	}
}
