package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func canonicalReferenceFixture() []MessageReference {
	return []MessageReference{
		{
			ReferenceID: "context:turn-1:0",
			Kind:        MessageReferenceText,
			Label:       "Selected output",
			Text:        "service ready",
		},
		{
			ReferenceID: "context:turn-1:1",
			Kind:        MessageReferenceFile,
			Label:       "config.yaml",
			Text:        "/workspace/config.yaml",
			ResourceRef: "host-resource:v1:config",
		},
		{
			ReferenceID: "context:turn-1:2",
			Kind:        MessageReferenceDirectory,
			Label:       "workspace",
			Text:        "/workspace",
			ResourceRef: "host-resource:v1:workspace",
		},
		{
			ReferenceID: "context:turn-1:3",
			Kind:        MessageReferenceTerminal,
			Label:       "Terminal selection",
			Text:        "service ready\nlistening on :8080",
		},
		{
			ReferenceID: "context:turn-1:4",
			Kind:        MessageReferenceProcess,
			Label:       "Process snapshot",
			Text:        "server pid 42 running",
		},
	}
}

func renderableSupplementalFixture() []TurnSupplementalContextItem {
	return []TurnSupplementalContextItem{{
		Kind:  "linked_context",
		Title: "Selected output",
		Text:  "service ready",
		Metadata: map[string]string{
			"path": "/workspace/config.yaml",
		},
	}}
}

func TestTurnInputValidatesClosedMessageReferenceUnionAndLimits(t *testing.T) {
	valid := canonicalReferenceFixture()
	if err := (TurnInput{References: valid}).Validate(); err != nil {
		t.Fatalf("valid references rejected: %v", err)
	}

	cases := []struct {
		name string
		refs []MessageReference
	}{
		{name: "invalid kind", refs: []MessageReference{{ReferenceID: "ref", Kind: "other", Label: "label", Text: "text"}}},
		{name: "blank id", refs: []MessageReference{{Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "unstable id", refs: []MessageReference{{ReferenceID: " ref ", Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "multiline id", refs: []MessageReference{{ReferenceID: "ref\nnext", Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "duplicate id", refs: []MessageReference{
			{ReferenceID: "ref", Kind: MessageReferenceText, Label: "one", Text: "one"},
			{ReferenceID: "ref", Kind: MessageReferenceText, Label: "two", Text: "two"},
		}},
		{name: "blank label", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Text: "text"}}},
		{name: "text with resource", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "label", Text: "text", ResourceRef: "resource"}}},
		{name: "terminal with resource", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceTerminal, Label: "label", Text: "text", ResourceRef: "resource"}}},
		{name: "process with resource", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceProcess, Label: "label", Text: "text", ResourceRef: "resource"}}},
		{name: "file without ref", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceFile, Label: "label"}}},
		{name: "directory without ref", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceDirectory, Label: "label"}}},
		{name: "truncated without text", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceFile, Label: "label", ResourceRef: "resource", Truncated: true}}},
		{name: "invalid utf8", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "label", Text: string([]byte{0xff})}}},
		{name: "too many", refs: func() []MessageReference {
			out := make([]MessageReference, MaxMessageReferencesPerTurn+1)
			for index := range out {
				out[index] = MessageReference{ReferenceID: "ref-" + strings.Repeat("x", index%3) + string(rune('a'+index%26)), Kind: MessageReferenceText, Label: "label", Text: "text"}
			}
			return out
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := (TurnInput{References: tc.refs}).Validate(); err == nil {
				t.Fatalf("references %#v succeeded", tc.refs)
			}
		})
	}

	duplicates := []MessageReference{
		{ReferenceID: "ref-1", Kind: MessageReferenceFile, Label: "first", ResourceRef: "same"},
		{ReferenceID: "ref-2", Kind: MessageReferenceFile, Label: "second", ResourceRef: "same"},
	}
	if err := (TurnInput{References: duplicates}).Validate(); err != nil {
		t.Fatalf("duplicate opaque resource refs should preserve user order: %v", err)
	}
}

func TestMessageReferenceResourceRefSupportsSelfContainedLocatorBoundary(t *testing.T) {
	const locatorLimit = 8 * 1024
	if MaxMessageReferenceResourceRefBytes != locatorLimit {
		t.Fatalf("resource ref limit = %d, want %d", MaxMessageReferenceResourceRefBytes, locatorLimit)
	}
	valid := MessageReference{
		ReferenceID: "context:0", Kind: MessageReferenceFile, Label: "max path",
		ResourceRef: strings.Repeat("x", locatorLimit),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("boundary resource ref rejected: %v", err)
	}
	valid.ResourceRef += "x"
	if err := valid.Validate(); err == nil {
		t.Fatal("oversized resource ref succeeded")
	}
	nonASCII := MessageReference{
		ReferenceID: "context:1", Kind: MessageReferenceFile, Label: "multibyte locator",
		ResourceRef: strings.Repeat("界", 2730) + "é",
	}
	if got := len([]byte(nonASCII.ResourceRef)); got != locatorLimit {
		t.Fatalf("multibyte resource ref = %d bytes, want %d", got, locatorLimit)
	}
	if err := nonASCII.Validate(); err != nil {
		t.Fatalf("boundary multibyte resource ref rejected: %v", err)
	}
	nonASCII.ResourceRef += "x"
	if err := nonASCII.Validate(); err == nil {
		t.Fatal("oversized multibyte resource ref succeeded")
	}
}

func TestMessageReferenceFieldAndPayloadBoundaries(t *testing.T) {
	valid := MessageReference{
		ReferenceID: strings.Repeat("i", MaxMessageReferenceIDBytes),
		Kind:        MessageReferenceText,
		Label:       strings.Repeat("界", MaxMessageReferenceLabelRunes),
		Text:        strings.Repeat("文", MaxMessageReferenceTextRunes),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("boundary reference rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*MessageReference)
	}{
		{name: "overlong id", mutate: func(reference *MessageReference) { reference.ReferenceID += "i" }},
		{name: "trim-unstable label", mutate: func(reference *MessageReference) { reference.Label = " label " }},
		{name: "multiline label", mutate: func(reference *MessageReference) { reference.Label = "label\u2028next" }},
		{name: "overlong label", mutate: func(reference *MessageReference) { reference.Label += "界" }},
		{name: "overlong text", mutate: func(reference *MessageReference) { reference.Text += "文" }},
		{name: "invalid id utf8", mutate: func(reference *MessageReference) { reference.ReferenceID = string([]byte{0xff}) }},
		{name: "invalid label utf8", mutate: func(reference *MessageReference) { reference.Label = string([]byte{0xff}) }},
		{name: "invalid text utf8", mutate: func(reference *MessageReference) { reference.Text = string([]byte{0xff}) }},
		{name: "invalid resource utf8", mutate: func(reference *MessageReference) {
			reference.Kind = MessageReferenceFile
			reference.Text = ""
			reference.ResourceRef = string([]byte{0xff})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reference := valid
			tc.mutate(&reference)
			if err := reference.Validate(); err == nil {
				t.Fatalf("invalid reference succeeded: %#v", reference)
			}
		})
	}

	atLimit := messageReferencesAtPayloadLimit(t)
	raw, err := json.Marshal(atLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != MaxMessageReferencesPayloadBytes {
		t.Fatalf("reference payload = %d bytes, want %d", len(raw), MaxMessageReferencesPayloadBytes)
	}
	if err := (TurnInput{References: atLimit}).Validate(); err != nil {
		t.Fatalf("boundary reference payload rejected: %v", err)
	}
	overLimit := append([]MessageReference(nil), atLimit...)
	overLimit[len(overLimit)-1].Text += "x"
	if err := (TurnInput{References: overLimit}).Validate(); err == nil {
		t.Fatal("oversized reference payload succeeded")
	}
}

func messageReferencesAtPayloadLimit(t *testing.T) []MessageReference {
	t.Helper()
	references := make([]MessageReference, 0, MaxMessageReferencesPerTurn)
	for index := 0; index < MaxMessageReferencesPerTurn; index++ {
		candidate := append(append([]MessageReference(nil), references...), MessageReference{
			ReferenceID: fmt.Sprintf("context:%03d", index),
			Kind:        MessageReferenceFile,
			Label:       "linked file",
			ResourceRef: strings.Repeat("r", MaxMessageReferenceResourceRefBytes),
		})
		raw, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > MaxMessageReferencesPayloadBytes {
			break
		}
		references = candidate
	}
	if len(references) == 0 {
		t.Fatal("could not construct reference payload")
	}
	raw, err := json.Marshal(references)
	if err != nil {
		t.Fatal(err)
	}
	remaining := MaxMessageReferencesPayloadBytes - len(raw)
	last := &references[len(references)-1]
	const nonEmptyTextJSONOverhead = len(`,"text":""`)
	if remaining < nonEmptyTextJSONOverhead+1 {
		shrink := nonEmptyTextJSONOverhead + 1 - remaining
		last.ResourceRef = last.ResourceRef[:len(last.ResourceRef)-shrink]
		remaining += shrink
	}
	last.Text = strings.Repeat("x", remaining-nonEmptyTextJSONOverhead)
	if len(last.Text) > MaxMessageReferenceTextRunes {
		t.Fatalf("payload boundary requires %d text characters", len(last.Text))
	}
	return references
}

func TestHostRejectsInvalidReferenceBeforeAdmissionWithoutMutation(t *testing.T) {
	cases := []struct {
		name      string
		reference MessageReference
	}{
		{name: "overlong id", reference: MessageReference{ReferenceID: strings.Repeat("i", MaxMessageReferenceIDBytes+1), Kind: MessageReferenceText, Label: "label", Text: "text"}},
		{name: "overlong label", reference: MessageReference{ReferenceID: "ref", Kind: MessageReferenceText, Label: strings.Repeat("界", MaxMessageReferenceLabelRunes+1), Text: "text"}},
		{name: "overlong text", reference: MessageReference{ReferenceID: "ref", Kind: MessageReferenceText, Label: "label", Text: strings.Repeat("文", MaxMessageReferenceTextRunes+1)}},
		{name: "invalid resource utf8", reference: MessageReference{ReferenceID: "ref", Kind: MessageReferenceFile, Label: "label", ResourceRef: string([]byte{0xff})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryStore()
			host, err := newTestHost(t, providerHostOptions{
				Config: runtimeGatewayConfig("invalid reference"),
				ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
					return runtimeGatewayEvents("unexpected"), nil
				}),
				ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
				Store:                store,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run",
				Input: TurnInput{Text: "inspect", References: []MessageReference{tc.reference}},
			}); err == nil {
				t.Fatal("RunTurn succeeded")
			}
			entries, err := store.repo.Entries(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			meta, err := store.repo.Thread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			overview, err := host.ReadThreadOverview(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 || meta.LeafID != "" || overview.Thread.ThroughOrdinal != 0 || overview.LatestTurn != nil {
				t.Fatalf("invalid reference mutated authority: entries=%#v meta=%#v overview=%#v", entries, meta, overview)
			}
		})
	}
}

func TestHostReferenceOnlyTurnUsesCurrentSupplementalWithoutHistoryLeak(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		index := len(requests)
		mu.Unlock()
		return runtimeGatewayEvents([]string{"first answer", "second answer"}[index-1]), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("reference-only contract"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	references := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1",
		Input: TurnInput{References: references}, SupplementalContext: renderableSupplementalFixture(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn-2", RunID: "run-2", Input: TurnInput{Text: "continue"},
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	firstTexts := modelRequestTexts(requests[0])
	if slicesContainBlank(firstTexts) || !strings.Contains(strings.Join(firstTexts, "\n"), "Host-provided supplemental context") {
		t.Fatalf("reference-only provider request = %#v", requests[0].Messages)
	}
	secondText := strings.Join(modelRequestTexts(requests[1]), "\n")
	if strings.Contains(secondText, "Host-provided supplemental context") || strings.Contains(secondText, "service ready") || strings.Contains(secondText, "config.yaml") {
		t.Fatalf("later provider history leaked references or supplemental context: %#v", requests[1].Messages)
	}
	if !strings.Contains(secondText, "continue") {
		t.Fatalf("later provider request lost current input: %#v", requests[1].Messages)
	}
}

func TestHostReferenceOnlyTurnUsesSQLiteAdmissionAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "reference-only.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("reference-only sqlite"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("done"), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run",
		Input: TurnInput{References: want}, SupplementalContext: renderableSupplementalFixture(),
	}); err != nil {
		t.Fatal(err)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].UserReferences, want) {
		t.Fatalf("sqlite reference-only page=%#v err=%v", page, err)
	}
}

func TestHostRejectsReferenceOnlyInvalidSupplementalBeforeAdmission(t *testing.T) {
	cases := []struct {
		name  string
		items []TurnSupplementalContextItem
	}{
		{name: "missing"},
		{name: "blank", items: []TurnSupplementalContextItem{{Kind: " ", Title: " ", Text: " ", Metadata: map[string]string{" ": " "}}}},
		{name: "flags only", items: []TurnSupplementalContextItem{{Sensitive: true, Truncated: true}}},
		{name: "invalid kind utf8", items: []TurnSupplementalContextItem{{Kind: string([]byte{0xff})}}},
		{name: "invalid title utf8", items: []TurnSupplementalContextItem{{Title: string([]byte{0xff})}}},
		{name: "invalid text utf8", items: []TurnSupplementalContextItem{{Text: string([]byte{0xff})}}},
		{name: "overlong kind", items: []TurnSupplementalContextItem{{Kind: strings.Repeat("类", MaxTurnSupplementalContextKindRunes+1)}}},
		{name: "overlong title", items: []TurnSupplementalContextItem{{Title: strings.Repeat("题", MaxTurnSupplementalContextTitleRunes+1)}}},
		{name: "overlong text", items: []TurnSupplementalContextItem{{Text: strings.Repeat("文", MaxTurnSupplementalContextTextRunes+1)}}},
		{name: "too many metadata pairs", items: []TurnSupplementalContextItem{{Metadata: runtimeSupplementalMetadataPairs(MaxTurnSupplementalMetadataPairs + 1)}}},
		{name: "overlong metadata key", items: []TurnSupplementalContextItem{{Metadata: map[string]string{strings.Repeat("k", MaxTurnSupplementalMetadataKeyBytes+1): "value"}}}},
		{name: "overlong metadata value", items: []TurnSupplementalContextItem{{Metadata: map[string]string{"key": strings.Repeat("值", MaxTurnSupplementalMetadataValueRunes+1)}}}},
		{name: "invalid metadata key utf8", items: []TurnSupplementalContextItem{{Metadata: map[string]string{string([]byte{0xff}): "value"}}}},
		{name: "invalid metadata value utf8", items: []TurnSupplementalContextItem{{Metadata: map[string]string{"key": string([]byte{0xff})}}}},
		{name: "oversized rendered payload", items: func() []TurnSupplementalContextItem {
			out := make([]TurnSupplementalContextItem, 17)
			for index := range out {
				out[index] = TurnSupplementalContextItem{Kind: "context", Text: strings.Repeat("x", MaxTurnSupplementalContextTextRunes)}
			}
			return out
		}()},
		{name: "too many", items: func() []TurnSupplementalContextItem {
			out := make([]TurnSupplementalContextItem, MaxTurnSupplementalContextItems+1)
			for index := range out {
				out[index] = TurnSupplementalContextItem{Kind: "context", Text: "value"}
			}
			return out
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			host, err := newTestHost(t, providerHostOptions{Config: runtimeGatewayConfig("test"), ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
				return runtimeGatewayEvents("unexpected"), nil
			}), ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), Store: NewMemoryStore()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{References: canonicalReferenceFixture()}, SupplementalContext: tc.items}); err == nil {
				t.Fatal("RunTurn succeeded")
			}
			overview, err := host.ReadThreadOverview(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if overview.LatestTurn != nil || overview.Thread.ThroughOrdinal != 0 {
				t.Fatalf("invalid supplemental context mutated thread: %#v", overview)
			}
		})
	}
}

func runtimeSupplementalMetadataPairs(count int) map[string]string {
	metadata := make(map[string]string, count)
	for index := 0; index < count; index++ {
		metadata[fmt.Sprintf("key-%03d", index)] = "value"
	}
	return metadata
}

func TestHostRunTurnReplayStartsProviderOnce(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	gateway := runtimeModelGateway(func(ctx context.Context, _ ModelRequest) (<-chan ModelEvent, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return runtimeGatewayEvents("done"), nil
	})
	newHost := func() *testProviderFacade {
		host, err := newTestHost(t, providerHostOptions{Config: runtimeGatewayConfig("test"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), Store: store})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	firstHost := newHost()
	if _, err := firstHost.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	request := RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{References: canonicalReferenceFixture()}, SupplementalContext: renderableSupplementalFixture()}
	firstDone := make(chan error, 1)
	go func() {
		_, err := firstHost.RunTurn(ctx, request)
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first provider did not start")
	}

	replayedRunning, err := newHost().RunTurn(ctx, request)
	if err != nil {
		t.Fatalf("running replay: %v", err)
	}
	if !replayedRunning.Replayed || replayedRunning.Status != TurnStatusRunning || calls.Load() != 1 {
		t.Fatalf("running replay=%#v provider calls=%d", replayedRunning, calls.Load())
	}
	for name, supplemental := range map[string][]TurnSupplementalContextItem{
		"missing": nil,
		"invalid": {{Kind: "invalid", Text: string([]byte{0xff})}},
	} {
		t.Run("running "+name, func(t *testing.T) {
			replay := request
			replay.SupplementalContext = supplemental
			result, err := newHost().RunTurn(ctx, replay)
			if err != nil || !result.Replayed || result.Status != TurnStatusRunning || calls.Load() != 1 {
				t.Fatalf("running replay=%#v err=%v provider calls=%d", result, err, calls.Load())
			}
		})
	}
	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first turn did not finish")
	}

	retry := request
	retry.SupplementalContext = []TurnSupplementalContextItem{{Kind: "different", Text: "must be ignored"}}
	replayedTerminal, err := newHost().RunTurn(ctx, retry)
	if err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	if !replayedTerminal.Replayed || replayedTerminal.Status != TurnStatusCompleted || replayedTerminal.Output != "done" || calls.Load() != 1 {
		t.Fatalf("terminal replay=%#v provider calls=%d", replayedTerminal, calls.Load())
	}
	for name, supplemental := range map[string][]TurnSupplementalContextItem{
		"missing": nil,
		"invalid": {{Kind: "invalid", Text: string([]byte{0xff})}},
	} {
		t.Run("terminal "+name, func(t *testing.T) {
			replay := request
			replay.SupplementalContext = supplemental
			result, err := newHost().RunTurn(ctx, replay)
			if err != nil || !result.Replayed || result.Status != TurnStatusCompleted || result.Output != "done" || calls.Load() != 1 {
				t.Fatalf("terminal replay=%#v err=%v provider calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestHostReferenceReplayAfterRetryUsesExactTerminalBranch(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int64
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("exact reference replay"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			index := calls.Add(1)
			return runtimeGatewayEvents(map[int64]string{1: "original answer", 2: "retry answer"}[index]), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	request := RunTurnRequest{
		ThreadID: "thread", TurnID: "turn-original", RunID: "run-original",
		Input: TurnInput{Text: "inspect", References: canonicalReferenceFixture()}, SupplementalContext: renderableSupplementalFixture(),
	}
	original, err := host.RunTurn(ctx, request)
	if err != nil || original.Status != TurnStatusCompleted || original.Output != "original answer" {
		t.Fatalf("original=%#v err=%v", original, err)
	}
	retried, err := host.RetryTurn(ctx, RetryTurnRequest{ThreadID: "thread", Reason: "verify"})
	if err != nil || retried.Status != TurnStatusCompleted || retried.Output != "retry answer" {
		t.Fatalf("retry=%#v err=%v", retried, err)
	}
	replayRequest := request
	replayRequest.SupplementalContext = []TurnSupplementalContextItem{{Kind: "different", Text: "ignored replay context"}}
	replayed, err := host.RunTurn(ctx, replayRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Status != TurnStatusCompleted || replayed.Output != "original answer" || calls.Load() != 2 {
		t.Fatalf("replay=%#v provider calls=%d", replayed, calls.Load())
	}
	if replayed.ProjectionAvailability != TurnProjectionAvailabilityReady || replayed.Projection == nil || replayed.Projection.Status != TurnStatusCompleted {
		t.Fatalf("exact replay projection=%#v availability=%q", replayed.Projection, replayed.ProjectionAvailability)
	}
	readProjection, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-original", RunID: "run-original"})
	if err != nil || readProjection.Status != TurnStatusCompleted {
		t.Fatalf("exact read projection=%#v err=%v", readProjection, err)
	}
	conflict := request
	conflict.Input.References = append([]MessageReference(nil), request.Input.References...)
	conflict.Input.References[0].Text = "changed canonical reference"
	if _, err := host.RunTurn(ctx, conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("changed references replay error=%v, want request conflict", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("conflicting replay called provider %d times", calls.Load())
	}
}

func TestHostReferenceOnlyFailureHasTypedNoRetryTarget(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int64
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("reference-only no retry"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			calls.Add(1)
			return nil, errors.New("provider failed")
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run",
		Input: TurnInput{References: canonicalReferenceFixture()}, SupplementalContext: renderableSupplementalFixture(),
	}); err == nil {
		t.Fatal("reference-only turn should retain its provider failure")
	}
	before, err := host.ReadThread(ctx, "thread")
	if err != nil || before.CanRetry {
		t.Fatalf("reference-only snapshot=%#v err=%v", before, err)
	}
	if _, err := host.RetryTurn(ctx, RetryTurnRequest{ThreadID: "thread", Reason: "provider recovered"}); !errors.Is(err, ErrNoRetryTarget) {
		t.Fatalf("RetryTurn error=%v, want ErrNoRetryTarget", err)
	}
	after, err := host.ReadThread(ctx, "thread")
	if err != nil || !reflect.DeepEqual(after, before) || calls.Load() != 1 {
		t.Fatalf("rejected RetryTurn mutated snapshot or called provider: before=%#v after=%#v err=%v calls=%d", before, after, err, calls.Load())
	}
}

func TestHostMessageReferencesSurviveSQLiteReopenAndFork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("test"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("done"), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "source", TurnID: "turn", RunID: "run", Input: TurnInput{Text: "inspect", References: want}, SupplementalContext: renderableSupplementalFixture()}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.repo.Entries(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryUserMessage && reflect.DeepEqual(entry.Message.References, sessionMessageReferences(want)) && entry.RawHash == sessiontree.StableHash(entry.Raw)
	}) {
		t.Fatalf("canonical entry missing references/raw hash: %#v", entries)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	maintenance, err := newTestMaintenanceHost(t, reopened)
	if err != nil {
		t.Fatal(err)
	}
	page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "source", Tail: 1})
	if err != nil || len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].UserReferences, want) {
		t.Fatalf("reopened references page=%#v err=%v", page, err)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-op", SourceThreadID: "source", DestinationThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	forked, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 1})
	if err != nil || len(forked.Turns) != 1 || !reflect.DeepEqual(forked.Turns[0].UserReferences, want) {
		t.Fatalf("forked references page=%#v err=%v", forked, err)
	}
}

func TestHostSubAgentReferencesProjectInDetailAndRejectReferenceOnly(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "child done", SystemPrompt: "test"},
		Store:  NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication", ParentThreadID: "parent", ThreadID: "child", TaskName: "worker", Message: "inspect", References: want, ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	detail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(detail.Events, func(event ThreadDetailEvent) bool {
		return event.Kind == ThreadDetailEventUserMessage && event.Message != nil && reflect.DeepEqual(event.Message.References, want)
	}) {
		t.Fatalf("subagent detail missing references: %#v", detail.Events)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "reference-only", ParentThreadID: "parent", ThreadID: "child-2", TaskName: "worker", References: want, ForkMode: SubAgentForkNone}); err == nil || !strings.Contains(err.Error(), "reference-only") {
		t.Fatalf("reference-only subagent spawn error=%v", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		CompletionRequestID: "completion", ContinuationTurnID: "next-turn", ContinuationRunID: "next-run", Status: PendingToolCompletionCompleted,
		Target: PendingToolSettlementTarget{ThreadID: "parent", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", Handle: "handle"},
		Input:  TurnInput{References: want},
	}); err == nil || !strings.Contains(err.Error(), "reference-only") {
		t.Fatalf("reference-only pending completion error=%v", err)
	}
}

func modelRequestTexts(req ModelRequest) []string {
	out := make([]string, 0, len(req.Messages))
	for _, message := range req.Messages {
		if message.Role == ModelMessageRoleUser {
			out = append(out, message.Text)
		}
	}
	return out
}

func slicesContainBlank(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return true
		}
	}
	return false
}

func TestMessageReferencesPreserveValueOrder(t *testing.T) {
	want := canonicalReferenceFixture()
	got := append([]MessageReference(nil), want...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("references=%#v want=%#v", got, want)
	}
}

func TestPublicMessageReferenceAndSupplementalLimitsMatchInternalValidation(t *testing.T) {
	if MaxMessageReferencesPerTurn != session.MaxMessageReferencesPerTurn ||
		MaxMessageReferenceIDBytes != session.MaxMessageReferenceIDBytes ||
		MaxMessageReferenceLabelRunes != session.MaxMessageReferenceLabelRunes ||
		MaxMessageReferenceTextRunes != session.MaxMessageReferenceTextRunes ||
		MaxMessageReferenceResourceRefBytes != session.MaxMessageReferenceResourceRefBytes ||
		MaxMessageReferencesPayloadBytes != session.MaxMessageReferencesPayloadBytes {
		t.Fatal("public message reference limits diverged from durable validation")
	}
	if MaxTurnSupplementalContextItems != engine.MaxTurnSupplementalContextItems ||
		MaxTurnSupplementalContextKindRunes != engine.MaxTurnSupplementalContextKindRunes ||
		MaxTurnSupplementalContextTitleRunes != engine.MaxTurnSupplementalContextTitleRunes ||
		MaxTurnSupplementalContextTextRunes != engine.MaxTurnSupplementalContextTextRunes ||
		MaxTurnSupplementalMetadataPairs != engine.MaxTurnSupplementalMetadataPairs ||
		MaxTurnSupplementalMetadataKeyBytes != engine.MaxTurnSupplementalMetadataKeyBytes ||
		MaxTurnSupplementalMetadataValueRunes != engine.MaxTurnSupplementalMetadataValueRunes ||
		MaxTurnSupplementalPayloadBytes != engine.MaxTurnSupplementalPayloadBytes {
		t.Fatal("public supplemental context limits diverged from provider validation")
	}
}
