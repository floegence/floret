package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestResidentActivityFrozenFieldsSurviveGrowthReplacementAndCompletion(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	created, err := service.CreateAck(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), created.ThreadID); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	const path = "/payload/rows/0/content"
	original := strings.Repeat("中文🌸\n", 4000)
	item := ThreadItem{ID: "tool:turn:tool", TurnID: "turn", RunID: "run", Kind: ThreadItemTool, Live: true}
	install := func(text string, terminal bool) {
		t.Helper()
		if err := actor.apply(t.Context(), func() error {
			activity := &observation.ActivityItem{ToolID: "tool", ToolName: "inspect", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusRunning, Presentation: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: text, Format: tools.StructuredActivityRowFormatCode}}}}}
			if terminal {
				activity.Status = observation.ActivityStatusSuccess
			}
			item.Activity, item.Live = activity, !terminal
			actor.state.view.Items = []ThreadItem{item}
			actor.state.view.ViewVersion++
			actor.invalidateItemIndex()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	install(original, false)
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	structure := ThreadActivityStructureInput{ThreadID: created.ThreadID, ItemID: item.ID, Path: path}
	frozen, err := service.ActivityStructure(t.Context(), scope, structure)
	if err != nil {
		t.Fatal(err)
	}
	input := ThreadActivityFieldInput{ThreadID: created.ThreadID, ItemID: item.ID, RunID: item.RunID, SourceID: frozen.Field.SourceID, Path: path}
	if input.SourceID == "" {
		t.Fatal("no frozen source")
	}
	for i := 0; i < 100; i++ {
		install(original+fmt.Sprint(i), false)
		structure.Version = frozen.Version
		current, err := service.ActivityStructure(t.Context(), scope, structure)
		if err != nil {
			t.Fatalf("growth invalidated structure: %v", err)
		}
		if current.Field.SourceID == frozen.Field.SourceID {
			t.Fatal("changed field reused old source")
		}
		part, err := service.ReadActivityField(t.Context(), scope, input)
		if err != nil || part.Bytes != len(original) || !strings.HasPrefix(original, part.Text) {
			t.Fatalf("frozen source changed: %v", err)
		}
	}
	install("replaced", true)
	var got strings.Builder
	for got.Len() < len(original) {
		input.OffsetBytes = got.Len()
		part, err := service.ReadActivityField(t.Context(), scope, input)
		if err != nil || part.Text == "" {
			t.Fatal("completion lost frozen field", err)
		}
		got.WriteString(part.Text)
	}
	if got.String() != original {
		t.Fatal("mixed different source versions")
	}
	if _, err := service.ReadActivityField(t.Context(), threadquery.Scope{Authority: "revoked"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("revoked scope accepted", err)
	}
	input.RunID = "wrong"
	if _, err := service.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("wrong run accepted", err)
	}
	input.RunID = item.RunID
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ReadActivityField(cancelCtx, scope, input); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read", err)
	}
	if err := actor.apply(t.Context(), func() error { actor.state.view.Items = nil; actor.invalidateItemIndex(); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("removed item retained authority", err)
	}
}

func TestResidentActivityCacheBoundsAndNoSiblingRetention(t *testing.T) {
	fields := residentActivityFields{}
	item := ThreadItem{ID: "tool", RunID: "run"}
	first, err := fields.retain(item, "/content", strings.Repeat("a", 65536))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		_, err := fields.retain(item, fmt.Sprint(i), strings.Repeat("b", 65536))
		if err != nil {
			t.Fatal(err)
		}
		if fields.bytes > residentActivityFieldBytes || len(fields.entries) > residentActivityFieldCount {
			t.Fatal("unbounded cache")
		}
	}
	if _, _, ok, _ := fields.read(t.Context(), item, &ThreadActivityFieldInput{SourceID: first, Path: "/content"}, 4096); ok {
		t.Fatal("old source not evicted")
	}
}

func TestResidentActivityAlignedReadsPreserveUnicode(t *testing.T) {
	for _, repeats := range []int{1000, 900000} {
		t.Run(fmt.Sprint(repeats), func(t *testing.T) {
			fields := residentActivityFields{}
			t.Cleanup(func() {
				if err := fields.close(); err != nil {
					t.Error(err)
				}
			})
			item := ThreadItem{ID: "unicode", RunID: "run"}
			text := strings.Repeat("a中文🌸\n", repeats)
			id, err := fields.retain(item, "/output", text)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 100 {
				offset := (len(text) - 1) * (100 - i) / 100
				input := ThreadActivityFieldInput{SourceID: id, Path: "/output", OffsetBytes: offset, AlignStart: true}
				chunk, extent, found, err := fields.read(t.Context(), item, &input, 4096)
				if err != nil || !found || extent != len(text) || input.OffsetBytes > offset || offset-input.OffsetBytes > 3 || len(chunk) == 0 || len(chunk) > 4096 || !utf8.ValidString(chunk) || text[input.OffsetBytes:input.OffsetBytes+len(chunk)] != chunk {
					t.Fatalf("invalid aligned range at %d: offset=%d bytes=%d err=%v", offset, input.OffsetBytes, len(chunk), err)
				}
				if input.OffsetBytes != offset {
					input.OffsetBytes, input.AlignStart = offset, false
					if _, _, _, err := fields.read(t.Context(), item, &input, 4096); err == nil {
						t.Fatal("strict offset accepted partial rune")
					}
				}
			}
		})
	}
}

func TestResidentActivityLargeFieldKeepsBoundedRanges(t *testing.T) {
	fields := residentActivityFields{}
	t.Cleanup(func() {
		if err := fields.close(); err != nil {
			t.Error(err)
		}
	})
	item := ThreadItem{ID: "large", RunID: "run"}
	text := strings.Repeat("large 中文🌸\n", 800000)
	id, err := fields.retain(item, "/output", text)
	if err != nil {
		t.Fatalf("large valid field cannot be opened: %v", err)
	}
	for _, offset := range []int{0, len(text) / 2, len(text) - 16384} {
		for offset > 0 && !utf8.RuneStart(text[offset]) {
			offset--
		}
		chunk, size, found, err := fields.read(t.Context(), item, &ThreadActivityFieldInput{SourceID: id, Path: "/output", OffsetBytes: offset}, 16384)
		if err != nil || !found || size != len(text) || len(chunk) > 16384 || !strings.HasPrefix(text[offset:], chunk) || !utf8.ValidString(chunk) {
			t.Fatalf("invalid large range: %v", err)
		}
	}
	if id == "" || fields.bytes > residentActivityFieldBytes {
		t.Fatal("large field exceeded resident budget")
	}
	body := fields.entries[0].body
	if body == nil || fields.spillBytes != len(text) {
		t.Fatal("large field did not leave resident memory")
	}
	for i := 0; i < residentActivityFieldCount; i++ {
		if _, err := fields.retain(item, fmt.Sprint("new-", i), "small"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, found, err := fields.read(t.Context(), item, &ThreadActivityFieldInput{SourceID: id, Path: "/output"}, 16384); found || err != nil {
		t.Fatal("evicted large field remained readable", err)
	}
	if fields.spillBytes != 0 {
		t.Fatal("evicted large field retained spill budget")
	}
	if _, err := body.Read(t.Context(), 0, 4096, len(text)); !errors.Is(err, os.ErrClosed) {
		t.Fatal("spill owner did not close", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fields.retainContext(ctx, item, "cancelled", text); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read retained source", err)
	}
	if err := fields.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fields.retain(item, "closed", "text"); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("closed owner retained source", err)
	}
}

// Running tools have a canonical call already, but their resident presentation
// contains newer output. A detail read must not select the older call snapshot.
func TestResidentActivityWithCanonicalCallReadsCurrentField(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	created, err := service.CreateAck(t.Context(), CreateThreadInput{RequestKey: "resident-canonical"})
	if err != nil {
		t.Fatal(err)
	}
	presentation := func(text string) *tools.ActivityPresentation {
		return &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: text}}
	}
	_, err = host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "call", ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: presentation("old")}}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), created.ThreadID); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	if err := actor.apply(t.Context(), func() error {
		actor.state.view.Items = []ThreadItem{{ID: "tool:turn:tool", TurnID: "turn", RunID: "run", Kind: ThreadItemTool, activityEntryIDs: []string{"call"}, Activity: &observation.ActivityItem{Status: observation.ActivityStatusRunning, Presentation: presentation("new live output")}}}
		actor.invalidateItemIndex()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	page, err := service.ActivityStructure(t.Context(), scope, ThreadActivityStructureInput{ThreadID: created.ThreadID, ItemID: "tool:turn:tool", Path: "/payload/output"})
	if err != nil {
		t.Fatal(err)
	}
	if page.Field.SourceID == "" || page.Field.Bytes != len("new live output") {
		t.Fatalf("selected old call instead of live output: %+v", page.Field)
	}
	part, err := service.ReadActivityField(t.Context(), scope, ThreadActivityFieldInput{ThreadID: created.ThreadID, ItemID: page.ItemID, RunID: page.RunID, SourceID: page.Field.SourceID, Path: page.Field.Path})
	if err != nil || part.Text != "new live output" {
		t.Fatalf("wrong live field: %+v %v", part, err)
	}
}

func TestResidentActivityFrozenFieldSurvivesCanonicalToolCompletion(t *testing.T) {
	started, releaseTool, releaseProvider := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishTool := sync.OnceFunc(func() { close(releaseTool) })
	finishProvider := sync.OnceFunc(func() { close(releaseProvider) })
	t.Cleanup(finishTool)
	t.Cleanup(finishProvider)
	original := strings.Repeat("frozen 中文🌸", 700)
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "inspect", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{BlockUntil: releaseProvider, Events: []provider.Event{{Type: provider.EventDelta, Text: "Complete."}, {Type: provider.EventDone, Reason: "stop"}}})
	tool := tools.Define[map[string]string](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil),
		Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
			return &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "inspect"}}, nil
		}}, nil, nil,
		func(ctx context.Context, inv tools.Invocation[map[string]string]) (tools.Result, error) {
			inv.UpdateActivity(tools.ActivityUpdate{Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: original}}})
			close(started)
			select {
			case <-releaseTool:
			case <-ctx.Done():
				return tools.Result{}, ctx.Err()
			}
			return tools.Result{Text: "done", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "final replacement", Status: "success"}}}, nil
		})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-hash", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, public := testThreadServiceWithAgent(t, agent)
	service := public.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "frozen-completion"})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := service.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "read", Input: UserInput{Text: "inspect"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not start")
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	input := ThreadActivityStructureInput{ThreadID: created.ThreadID, ItemID: "tool:" + ack.TurnID.String() + ":read", Path: "/payload/output"}
	frozen, err := service.ActivityStructure(t.Context(), scope, input)
	if err != nil || frozen.Field.SourceID == "" || frozen.Field.Bytes != len(original) {
		t.Fatalf("missing running field: %+v %v", frozen.Field, err)
	}
	read := ThreadActivityFieldInput{ThreadID: created.ThreadID, ItemID: input.ItemID, RunID: frozen.RunID, SourceID: frozen.Field.SourceID, Path: input.Path}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ReadActivityField(cancelled, scope, read); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation: %v", err)
	}
	finishTool()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	var received strings.Builder
	for received.Len() < len(original) {
		read.OffsetBytes = received.Len()
		chunk, err := service.ReadActivityField(t.Context(), scope, read)
		if err != nil || chunk.Text == "" {
			t.Fatalf("canonical handoff lost accepted source: %v", err)
		}
		received.WriteString(chunk.Text)
	}
	if received.String() != original {
		t.Fatal("canonical handoff mixed source versions")
	}
	current, err := service.ActivityStructure(t.Context(), scope, input)
	if err != nil {
		t.Fatal(err)
	}
	if current.Field.Bytes != len("final replacement") {
		t.Fatalf("current source did not advance: %+v", current.Field)
	}
	finishProvider()
	waitThreadView(t, public, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if _, err := service.ReadActivityField(t.Context(), scope, read); err != nil {
		t.Fatalf("settlement lost source: %v", err)
	}
}

func TestResidentActivityGrowingLargeFieldSharesFrozenPrefix(t *testing.T) {
	fields := residentActivityFields{}
	t.Cleanup(func() {
		if err := fields.close(); err != nil {
			t.Error(err)
		}
	})
	item := ThreadItem{ID: "growing", RunID: "run"}
	original := strings.Repeat("中文🌸\n", 1000000)
	first, err := fields.retain(item, "/output", original)
	if err != nil {
		t.Fatal(err)
	}
	text := original
	for i := range 40 {
		text += fmt.Sprintf("chunk-%d\n", i)
		current, err := fields.retain(item, "/output", text)
		if err != nil {
			t.Fatal(err)
		}
		if current == first {
			t.Fatal("growing source changed frozen extent")
		}
		chunk, size, found, err := fields.read(t.Context(), item, &ThreadActivityFieldInput{SourceID: first, Path: "/output", OffsetBytes: len(original) - 11}, 4096)
		if err != nil || !found || size != len(original) || chunk != original[len(original)-11:] {
			t.Fatalf("old extent lost during growth: size=%d found=%v error=%v", size, found, err)
		}
		if fields.spillBytes != len(text) {
			t.Fatalf("growth copied immutable prefix: retained=%d latest=%d", fields.spillBytes, len(text))
		}
	}
	replacement := strings.Repeat("replacement\n", 1000000)
	replaced, err := fields.retain(item, "/output", replacement)
	if err != nil || replaced == first {
		t.Fatal("replacement did not select independent bytes", err)
	}
	chunk, _, found, err := fields.read(t.Context(), item, &ThreadActivityFieldInput{SourceID: first, Path: "/output"}, 4096)
	if err != nil || !found || !strings.HasPrefix(original, chunk) {
		t.Fatal("replacement corrupted frozen prefix", err)
	}
	if fields.spillBytes != len(text)+len(replacement) {
		t.Fatal("replacement storage accounting differs")
	}
}
