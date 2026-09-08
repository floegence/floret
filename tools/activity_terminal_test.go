package tools_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestTerminalReadCursorSnapshotReplacesHasMore(t *testing.T) {
	initial := &tools.ActivityPresentation{Label: "Check diagnostic output", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: "read", Command: "gpu diagnostic", FirstSeq: 1, LastSeq: 2, LatestSeq: 4, HasMore: true, Output: "first page"}}
	update := &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: "read", FirstSeq: 3, LastSeq: 4, LatestSeq: 4, Output: "last page"}}
	merged := tools.MergeActivityPresentations(initial, update)
	got := merged.Payload.(tools.TerminalActivityPayload)
	if got.HasMore || got.FirstSeq != 3 || got.LastSeq != 4 || got.Command != "gpu diagnostic" {
		t.Fatalf("stale cursor snapshot: %#v", got)
	}
	statusOnly := tools.MergeActivityPresentations(initial, &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Status: "success"}})
	if !statusOnly.Payload.(tools.TerminalActivityPayload).HasMore {
		t.Fatal("status-only update erased output facts")
	}
}

func TestTerminalActivityPublicContract(t *testing.T) {
	exit := 0
	initial := &tools.ActivityPresentation{Label: "Submit SSH login input", Description: "Provide the requested credential", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: "write", Command: "ssh host", ProcessID: "process-1", InputBytes: 12, FirstSeq: 1, LastSeq: 2, LatestSeq: 3, HasMore: true, TotalBytes: 128, ExecutionLocation: "local", TimedOut: true, ExitCode: &exit, PendingResult: "running", Error: &tools.ActivityError{Message: "deadline exceeded"}}}
	raw, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	var decoded tools.ActivityPresentation
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(initial, &decoded) {
		t.Fatalf("round trip mismatch: %s", raw)
	}
	clone := tools.CloneActivityPresentation(initial)
	cp := clone.Payload.(tools.TerminalActivityPayload)
	*cp.ExitCode = 5
	cp.Error.Message = "changed"
	if *initial.Payload.(tools.TerminalActivityPayload).ExitCode != 0 || initial.Payload.(tools.TerminalActivityPayload).Error.Message != "deadline exceeded" {
		t.Fatal("clone aliases pointers")
	}
	final := tools.FinalizeActivityPresentation(initial, "completed")
	fp := final.Payload.(tools.TerminalActivityPayload)
	if fp.Status != "completed" || fp.PendingResult != "" || fp.InputBytes != 12 || !fp.HasMore || !fp.TimedOut || fp.ExecutionLocation != "local" {
		t.Fatalf("finalization lost facts: %#v", fp)
	}
	for _, payload := range []string{`{"operation":"poll"}`, `{"input_bytes":-1}`, `{"first_seq":-1}`, `{"last_seq":-1}`, `{"latest_seq":-1}`, `{"total_bytes":-1}`, `{"first_seq":3,"last_seq":2}`, `{"last_seq":3,"latest_seq":2}`, `{"stdin":"secret"}`, `{"input":"secret"}`, `{"password":"secret"}`, `{"execution_location":42}`} {
		var invalid tools.ActivityPresentation
		err := json.Unmarshal([]byte(`{"renderer":"terminal","payload":`+payload+`}`), &invalid)
		if err == nil {
			err = invalid.Validate()
		}
		if err == nil {
			t.Errorf("accepted invalid payload: %s", payload)
		}
	}
	for _, op := range []string{"", "exec", "read", "write", "terminate"} {
		valid := tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: op}}
		if err := valid.Validate(); err != nil {
			t.Errorf("operation %q: %v", op, err)
		}
	}
	if strings.Contains(string(raw), "stdin") || strings.Contains(string(raw), `"input"`) {
		t.Fatal("raw input entered display contract")
	}
}
