package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
)

func TestExecutionSegmentReadsVisitOnlyNewRecords(t *testing.T) {
	ctx := t.Context()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &requestReadBackend{Backend: inner}
	kernel, err := NewBackendKernel(ctx, backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := cache.ProviderRequestRecord{ID: "request", PromptScopeID: "scope"}
	var segments []cache.Segment
	for index := range 600 {
		segments = append(segments, cache.Segment{PromptScopeID: "scope", ID: fmt.Sprint(index), Provider: "test", Model: "model", Message: cache.MessageSnapshot{Attachments: []session.MessageAttachment{{Name: "original"}}}})
	}
	if err := kernel.CheckpointProviderRequest(ctx, segments, nil, request); err != nil {
		t.Fatal(err)
	}
	store := cache.NewExecutionStore(kernel, "scope", "turn")
	read := func(count int) []cache.Segment {
		t.Helper()
		got, err := store.Segments(ctx, "scope", "test", "model")
		if err != nil || len(got) != count {
			t.Fatalf("got %d segments, want %d: %v", len(got), count, err)
		}
		return got
	}
	read(600)[0].Message.Attachments[0].Name = "mutated"
	if err := kernel.AppendSegment(ctx, segments[0]); err != nil {
		t.Fatal(err)
	}
	backend.scans = 0
	if got := read(601); got[0].Message.Attachments[0].Name != "original" || backend.scans > 1 {
		t.Fatalf("repeat read: name=%q scans=%d", got[0].Message.Attachments[0].Name, backend.scans)
	}
	if err := kernel.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	read(601)
	if err := kernel.DeletePromptScopes(ctx, "scope"); err != nil {
		t.Fatal(err)
	}
	if err := kernel.CheckpointProviderRequest(ctx, segments[2:3], nil, request); err != nil {
		t.Fatal(err)
	}
	if got := read(1); got[0].ID != "2" {
		t.Fatal(got)
	}
}

func TestSegmentPrefixSurvivesFailedWritesButNeverHidesTail(t *testing.T) {
	ctx := t.Context()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &promptCommitFailureBackend{Backend: inner}
	kernel, err := NewBackendKernel(ctx, backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	reader := &executionSegmentReader{kernel: kernel, scope: "scope", budget: 1800}
	request := cache.ProviderRequestRecord{ID: "initial", PromptScopeID: "scope"}
	var segments []cache.Segment
	for index := range 10 {
		segments = append(segments, cache.Segment{ID: fmt.Sprint(index), PromptScopeID: "scope", Provider: "test", Model: "model"})
	}
	if err := kernel.CheckpointProviderRequest(ctx, segments, nil, request); err != nil {
		t.Fatal(err)
	}
	read := func(provider string, want int) {
		t.Helper()
		got, err := reader.Segments(ctx, "scope", provider, "model")
		if err != nil || len(got) != want {
			t.Fatalf("read: %d, %v", len(got), err)
		}
		for index, item := range got {
			if item.ID != fmt.Sprint(index) {
				t.Fatalf("segment %d: %s", index, item.ID)
			}
		}
	}
	read("test", 10)
	if len(reader.prefix) == 0 || len(reader.prefix) >= 10 || reader.bytes > reader.budget {
		t.Fatalf("unbounded or empty prefix: %d, %d", len(reader.prefix), reader.bytes)
	}
	read("other", 0)
	prepared := cache.NewPreparedStore(kernel)
	if err := prepared.AppendSegment(ctx, segments[0]); err != nil {
		t.Fatal(err)
	}
	backend.fail = true
	if err := prepared.AppendProviderRequest(ctx, request); err == nil {
		t.Fatal("checkpoint succeeded")
	}
	if err := kernel.DeletePromptScopes(ctx, "scope"); err == nil {
		t.Fatal("delete succeeded")
	}
	read("test", 10)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reader.Segments(cancelled, "scope", "test", "model"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	read("test", 10)
}

func BenchmarkExecutionSegmentReads(b *testing.B) {
	ctx := b.Context()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/segments.sqlite")))
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	kernel, err := NewBackendKernel(ctx, inner, time.Now)
	if err != nil {
		b.Fatal(err)
	}
	var segments []cache.Segment
	for index := range 2400 {
		segments = append(segments, cache.Segment{ID: fmt.Sprint(index), PromptScopeID: "scope", Provider: "test", Model: "model"})
	}
	if err := kernel.CheckpointProviderRequest(ctx, segments, nil, cache.ProviderRequestRecord{ID: "request", PromptScopeID: "scope"}); err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"fresh", "execution"} {
		b.Run(name, func(b *testing.B) {
			var reader cache.SegmentReader = kernel
			if name == "execution" {
				reader = cache.NewExecutionStore(kernel, "scope", "turn")
			}
			if _, err := reader.Segments(ctx, "scope", "test", "model"); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				got, err := reader.Segments(ctx, "scope", "test", "model")
				if err != nil || len(got) != len(segments) {
					b.Fatalf("read: %d %v", len(got), err)
				}
			}
		})
	}
}
