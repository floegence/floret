package sessiontree

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/observation"
)

func TestThreadContextEntriesKeepExecutionIdentityOnlyOnCanonicalEntry(t *testing.T) {
	statusEntry, err := NewThreadContextStatusEntry(observation.ContextStatus{
		RunID: "run", ThreadID: "thread", TurnID: "turn",
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
		RequestEstimate: config.RequestEstimate{Breakdown: config.ContextBreakdown{Source: "semantic_input_bpe_v1", ToolResultTokens: 750}},
		Provider:        "test", Model: "scripted", ObservedAt: time.Unix(1_725_273_600, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	compactionEntry, err := NewThreadContextCompactionEntry(ThreadContextCompaction{
		RunID: "run", ThreadID: "thread", TurnID: "turn", OperationID: "compact", RequestID: "request",
		Phase: string(observation.CompactionPhaseNoop), Status: string(observation.CompactionStatusNoop),
	})
	if err != nil {
		t.Fatal(err)
	}
	for label, raw := range map[string]string{
		"status":     statusEntry.Metadata[threadContextStatusKey],
		"compaction": compactionEntry.Metadata[threadContextCompactionKey],
	} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"thread_id", "turn_id", "run_id"} {
			if _, found := fields[field]; found {
				t.Fatalf("%s payload retained %s: %s", label, field, raw)
			}
		}
	}

	statusEntry.ThreadID = "fork"
	status, err := DecodeThreadContextStatusEntry(statusEntry)
	if err != nil {
		t.Fatal(err)
	}
	if status.ThreadID != identity.ThreadID("fork") || status.TurnID != identity.TurnID("turn") || status.RunID != identity.RunID("run") {
		t.Fatalf("decoded status identity=%#v", status)
	}
	if status.RequestEstimate.Breakdown.ToolResultTokens != 750 {
		t.Fatalf("lost input composition: %+v", status.RequestEstimate)
	}
	compactionEntry.ThreadID = "fork"
	compaction, err := DecodeThreadContextCompactionEntry(compactionEntry)
	if err != nil {
		t.Fatal(err)
	}
	if compaction.ThreadID != "fork" || compaction.TurnID != "turn" || compaction.RunID != "run" {
		t.Fatalf("decoded compaction identity=%#v", compaction)
	}
}

func TestThreadContextKeyScanPreservesExactIdentityRules(t *testing.T) {
	type value struct {
		RunID string `json:"run_id"`
		Label string
		Raw   json.RawMessage
	}
	for _, raw := range []string{
		`{"Label":"🌸","Raw":{"run_id":"nested"}}`,
		`{"run_id":null}`, `{"run\u005fid":"hidden"}`, `{"RUN_ID":"case"}`,
		`{"Label":"first","Label":"last"}`, `{"Raw":{"a":1,"a":2}}`,
		`{"unknown":true}`, `{"Raw":[1,2,]}`, `{}`, `null`, `[]`, `{} {}`, ``,
	} {
		var got, want value
		var fields map[string]json.RawMessage
		expected := json.Unmarshal([]byte(raw), &fields)
		_, forbidden := fields["run_id"]
		if expected == nil && !forbidden {
			expected = storagecodec.DecodeJSON([]byte(raw), &want)
		}
		actual := decodeStrictThreadContextJSON(raw, &got, map[string]struct{}{"run_id": {}})
		valid := expected == nil && !forbidden
		if (actual == nil) != valid || valid && !reflect.DeepEqual(got, want) {
			t.Fatalf("raw=%q got=%+v err=%v want=%+v valid=%v", raw, got, actual, want, valid)
		}
	}
}
