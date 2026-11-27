package runtime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

type isolatedFileReader struct{ threadsqlite.LocalRecords }

func (source isolatedFileReader) Open(context.Context) (spi.Backend, error) { return source, nil }
func (source isolatedFileReader) Close() error                              { return nil }

type testFileSourceID struct{ randomIDSource }

func (testFileSourceID) NewThreadID() (identity.ThreadID, error) { return "source", nil }

func TestPublishedFileForkSurvivesRemovalAtProviderBoundary(t *testing.T) {
	for _, reference := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "reference"}[reference], func(t *testing.T) {
			ctx := t.Context()
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			catalogPath := filepath.Join(root, "catalog.sqlite")
			catalog, err := threadsqlite.CreateCatalog(ctx, catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = catalog.Close() })
			openFiles := func() *threadsqlite.Files {
				files, err := threadsqlite.NewFiles(root, catalog.Store, 2, 1, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := files.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				return files
			}
			files := openFiles()
			allocate := func(id identity.ThreadID) threadsqlite.Reference {
				ref, err := threadsqlite.Allocate(id)
				if err != nil {
					t.Fatal(err)
				}
				if err = files.Create(ctx, ref); err != nil {
					t.Fatal(err)
				}
				return ref
			}
			source, fork, stale := allocate("source"), allocate("fork"), allocate("stale")
			if err := catalog.Publish(ctx, files, "create", []threadsqlite.Replacement{{Thread: source.Thread, Next: source.File}}); err != nil {
				t.Fatal(err)
			}
			answer := func(text string) florettest.Step {
				return florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: text}, {Type: provider.EventDone, Reason: "stop"}}}
			}
			call := florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "read_sample", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "detached", StateCompatibilityKey: "detached:1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, call, answer("retained-answer"), answer("later-answer"), answer("continued-answer"))
			var calls atomic.Int32
			full := "begin\n" + strings.Repeat("retained evidence\n", 12000) + "end\n"
			tool := tools.Define[map[string]any](tools.Definition{Name: "read_sample", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
				calls.Add(1)
				return tools.Result{Text: `{"preview":"read-sample-complete"}`, FullOutput: &tools.FullOutputPlan{Text: full}}, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			openPublished := func(id identity.ThreadID) (*Host, ThreadService) {
				ref, err := catalog.Resolve(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				host, err := Open(ctx, Options{Storage: storage.NewSource(isolatedFileReader{threadsqlite.LocalRecords{Files: files, Reference: ref}}), IDSource: testFileSourceID{}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := host.Shutdown(context.Background()); err != nil {
						t.Error(err)
					}
				})
				svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, svc
			}
			send := func(svc ThreadService, id identity.ThreadID, text string) {
				receipt, err := svc.Send(ctx, SendInput{ThreadID: id, RequestKey: RequestKey(text), Input: UserInput{Text: text}})
				if err != nil {
					t.Fatal(err)
				}
				view := waitThreadView(t, svc, id, func(view ThreadView) bool {
					return view.TurnID == receipt.TurnID && view.Activity == ThreadActivityIdle && view.LastOutcome != nil
				})
				if *view.LastOutcome != TurnOutcomeCompleted {
					t.Fatalf("turn failed: %v", view.Failure)
				}
			}
			host, svc := openPublished(source.Thread)
			created, err := svc.Create(ctx, CreateThreadInput{RequestKey: "fixture"})
			if err != nil || created.ThreadID != source.Thread {
				t.Fatalf("create %s: %v", created.ThreadID, err)
			}
			send(svc, source.Thread, "retained-question")
			retainedRef := func(messages []provider.Message) string {
				for _, message := range messages {
					if message.Role != provider.RoleTool || message.ToolResult == nil {
						continue
					}
					var envelope struct {
						Retained struct {
							Ref string `json:"ref"`
						} `json:"retained_output"`
					}
					if err := json.Unmarshal([]byte(message.ToolResult.Text), &envelope); err != nil {
						t.Fatal(err)
					}
					return envelope.Retained.Ref
				}
				return ""
			}
			outputRef := retainedRef(gateway.Requests()[1].Messages)
			if outputRef == "" || calls.Load() != 1 {
				t.Fatal("source did not persist the confirmed tool output")
			}
			meta, err := host.store.repo.Thread(ctx, string(source.Thread))
			if err != nil {
				t.Fatal(err)
			}
			send(svc, source.Thread, "after-anchor")
			if err := host.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			opts := sessiontree.ForkOptions{SourceThreadID: string(source.Thread), NewThreadID: string(fork.Thread), EntryID: meta.LeafID, EntryIDPinned: true, ReferenceHistory: reference, OriginRequestKey: "fork-key", OriginFingerprint: "fixture-input", Now: time.Now().UTC()}
			request := reservedFileFork{source, fork, opts}
			if err := request.prepare(ctx, files); err != nil {
				t.Fatal(err)
			}
			if _, err := catalog.Resolve(ctx, fork.Thread); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("staged fork became visible: %v", err)
			}
			if err := request.prepare(ctx, files); err != nil {
				t.Fatal(err)
			}
			wrong := request
			wrong.Options.ReferenceHistory = !reference
			if err := wrong.publish(ctx, catalog, files, "fork"); !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("published wrong fingerprint: %v", err)
			}
			if err := request.publish(ctx, catalog, files, "fork"); err != nil {
				t.Fatal(err)
			}
			other := request
			other.Target = stale
			other.Options.NewThreadID = string(stale.Thread)
			if err := other.prepare(ctx, files); err != nil {
				t.Fatal(err)
			}
			if err := catalog.Publish(ctx, files, "delete-source", []threadsqlite.Replacement{{Thread: source.Thread, Expected: source.File}}); err != nil {
				t.Fatal(err)
			}
			if err := other.publish(ctx, catalog, files, "stale-fork"); !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("published after source deletion: %v", err)
			}
			if err := files.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := catalog.Close(); err != nil {
				t.Fatal(err)
			}
			name := hex.EncodeToString(source.File[:])
			if err := os.Remove(filepath.Join(root, name[:2], name+".sqlite")); err != nil {
				t.Fatal(err)
			}
			catalog, err = threadsqlite.OpenCatalog(ctx, catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			files = openFiles()
			if err := request.publish(ctx, catalog, files, "fork"); err != nil {
				t.Fatal(err)
			}
			host, svc = openPublished(fork.Thread)
			if len(gateway.Requests()) != 3 || calls.Load() != 1 {
				t.Fatal("restart dispatched a model request")
			}
			send(svc, fork.Thread, "new-question")
			requests := gateway.Requests()
			if len(requests) != 4 || calls.Load() != 1 {
				t.Fatalf("expected four provider requests and one tool execution, got %d and %d", len(requests), calls.Load())
			}
			if !reference {
				var contents strings.Builder
				if _, err := svc.(ThreadOutputService).ReadToolOutput(ctx, ToolOutputReadInput{ThreadID: fork.Thread, Ref: outputRef}); !errors.Is(err, ErrThreadAuthorityInvariant) {
					t.Fatalf("source-scoped reference: %v", err)
				}
				localRef := retainedRef(requests[3].Messages)
				if localRef == "" || localRef == outputRef {
					t.Fatal("model did not receive a destination-scoped output reference")
				}
				in := ToolOutputReadInput{ThreadID: fork.Thread, Ref: localRef, MaxBytes: 8000}
				for {
					page, err := svc.(ThreadOutputService).ReadToolOutput(ctx, in)
					if err != nil {
						t.Fatal(err)
					}
					contents.WriteString(page.Text)
					if !page.HasMore {
						break
					}
					if page.NextOffset <= in.Offset {
						t.Fatal("output cursor stalled")
					}
					in.Offset = page.NextOffset
				}
				if contents.String() != full {
					t.Fatalf("tool output changed: got %d bytes, want %d", contents.Len(), len(full))
				}
			}
			encoded, err := json.Marshal(requests[3].Messages)
			if err != nil {
				t.Fatal(err)
			}
			model := string(encoded)
			for _, want := range []string{"retained-question", "retained-answer", "new-question"} {
				if !strings.Contains(model, want) {
					t.Fatalf("missing %s", want)
				}
			}
			if strings.Contains(model, "after-anchor") || strings.Contains(model, "later-answer") || strings.Contains(model, "Reference history") != reference {
				t.Fatal("fork included wrong model history")
			}
			page, err := svc.History(ctx, fork.Thread, "", 20)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(page.Items)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), "retained-question") == reference || !strings.Contains(string(body), "new-question") {
				t.Fatalf("wrong direct history: %s", body)
			}
			if err := host.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
