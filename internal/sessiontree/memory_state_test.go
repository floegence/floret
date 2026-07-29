package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestDecodeMemoryStateRejectsUnknownTrailingAndCorruptAuthority(t *testing.T) {
	now := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &shape); err != nil {
		t.Fatal(err)
	}
	shape["unknown"] = json.RawMessage(`true`)
	unknown, err := json.Marshal(shape)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeMemoryState(unknown, time.Now); err == nil {
		t.Fatal("unknown session-tree field passed validation")
	}
	if _, err := DecodeMemoryState(append(encoded, []byte(` {}`)...), time.Now); err == nil {
		t.Fatal("trailing session-tree data passed validation")
	}

	delete(shape, "unknown")
	shape["threads"] = json.RawMessage(`{"child":{"id":"child","parent_thread_id":"missing","parent_turn_id":"turn","task_name":"task","agent_path":"task","lifecycle":"open"}}`)
	corrupt, err := json.Marshal(shape)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeMemoryState(corrupt, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt authority error = %v, want ErrAuthorityCorrupt", err)
	}
}
