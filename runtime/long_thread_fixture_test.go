package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

// This writer belongs to the canonical owner. Downstream qualification invokes
// this test from an exact published module; it never writes Floret records.
// The profile reproduces density and renderer counts, not private source data.
// Nine user messages and 41 assistant messages replace the reference's eight
// users, 33 assistants, eight Goals and one interaction. Interactive lifecycle
// correctness is exercised separately, not inferred from this completed history.
type longThreadFixtureManifest struct {
	Profile        string         `json:"profile"`
	ThreadID       string         `json:"thread_id"`
	Scale          int            `json:"scale"`
	Turns          int            `json:"turns"`
	Items          int            `json:"items"`
	Tools          int            `json:"tools"`
	LargestTools   int            `json:"largest_turn_tools"`
	Renderers      map[string]int `json:"renderers"`
	JournalSHA256  string         `json:"journal_sha256"`
	ItemsSHA256    string         `json:"items_sha256"`
	DatabaseSHA256 string         `json:"database_sha256"`
}

func writeLongThreadFixture(t *testing.T, directory string, scale int, singleTurn bool) longThreadFixtureManifest {
	t.Helper()
	if scale != 1 && scale != 10 && scale != 100 {
		t.Fatal("fixture scale must be 1, 10 or 100")
	}
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
		t.Fatalf("fixture requires a fresh empty directory: %v", err)
	}
	path := filepath.Join(directory, "floret_threads.sqlite")
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	const id = "thread-synthetic-long-history"
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	writer := host.store.repo.(sessiontree.RuntimeJournalRepo)
	turnsRepo := host.store.repo.(sessiontree.RuntimeTurnRepo)
	manifest := longThreadFixtureManifest{Profile: "dense-tools-v1", ThreadID: id, Scale: scale, Renderers: make(map[string]int)}
	toolSerial := 0
	turnSerial := 0
	turns := 9 * scale
	if singleTurn {
		turns = 1
	}
	for turnIndex := range turns {
		turnSerial++
		turnID, runID := fmt.Sprintf("turn-%06d", turnSerial), fmt.Sprintf("run-%06d", turnSerial)
		if _, err := turnsRepo.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{
			ThreadID: id, TurnID: turnID, RunID: runID,
			LogicalRequestID: turnID, RequestFingerprint: turnID, InputRequestFingerprint: turnID + "-input",
			Input: session.Message{Role: session.User, Content: fmt.Sprintf("Synthetic request %d. Preserve 中文 and UTF-8 🌸.\r\n", turnSerial)}, Now: now.Add(time.Duration(turnIndex) * time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		toolCount, assistantCount := 48, 4
		if turnIndex%9 == 7 {
			toolCount = 49
		}
		if turnIndex%9 == 8 {
			toolCount, assistantCount = 661, 9
		}
		if singleTurn {
			toolCount, assistantCount = 1046*scale, 50*scale-1
		}
		manifest.LargestTools = max(manifest.LargestTools, toolCount)
		batch := make([]sessiontree.Entry, 0, 256)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if _, err := writer.AppendRuntimeFacts(t.Context(), id, batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
		appendFact := func(entry sessiontree.Entry) {
			entry.ThreadID, entry.TurnID, entry.RunID = id, turnID, runID
			entry.CreatedAt = now.Add(time.Duration(turnIndex)*time.Hour + time.Duration(manifest.Items)*time.Millisecond)
			batch = append(batch, entry)
			if len(batch) == cap(batch) {
				flush()
			}
		}
		for range toolCount {
			serial := toolSerial
			toolSerial++
			callID := fmt.Sprintf("inspect-%08d", serial)
			presentation := longThreadFixtureActivity(serial % 1046)
			manifest.Renderers[string(presentation.Renderer)]++
			appendFact(sessiontree.Entry{ID: callID + "-call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "qualification_inspect", ToolCallID: callID, ToolArgs: "{}"}})
			appendFact(sessiontree.Entry{ID: callID + "-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "qualification_inspect", ToolCallID: callID, Content: "Synthetic inspection complete.", Activity: presentation}})
			manifest.Items++
		}
		finalID := ""
		for assistant := range assistantCount {
			finalID = fmt.Sprintf("answer-%06d-%03d", turnSerial, assistant)
			appendFact(sessiontree.Entry{ID: finalID + "-entry", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: finalID, Content: "## Synthetic evidence\r\n\r\n" + strings.Repeat("Verified **Markdown**, 中文🌸 and [source](https://example.com).\r\n", 10)}})
			manifest.Items++
		}
		flush()
		finalEntries, _ := json.Marshal([]string{finalID + "-entry"})
		if _, err := turnsRepo.FinishTurn(t.Context(), sessiontree.FinishTurnRequest{ThreadID: id, TurnID: turnID, RunID: runID, TerminalEntryID: turnID + "-done", Status: sessiontree.TurnCompleted, OutcomeFingerprint: turnID + "-completed", Metadata: map[string]string{sessiontree.FinalResponseEntriesMetadataKey: string(finalEntries)}, Now: now.Add(time.Duration(turnIndex+1)*time.Hour - time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
		manifest.Items++ // admitted user
		manifest.Turns++
	}
	manifest.Tools = toolSerial
	// Hash canonical facts through real bounded storage reads. Do not keep a
	// second transcript in the generator or use Path followed by slicing.
	digest := sha256.New()
	encoder := json.NewEncoder(digest)
	reader := host.store.repo.(sessiontree.PathEntriesWindowReader)
	after := ""
	for {
		page, err := reader.PathEntriesWindow(t.Context(), sessiontree.PathWindowOptions{ThreadID: id, AnchorID: after, Direction: sessiontree.PathWindowAfter, Limit: 256})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Entries {
			if err := encoder.Encode(entry); err != nil {
				t.Fatal(err)
			}
			after = entry.ID
		}
		if !page.HasAfter {
			break
		}
	}
	manifest.JournalSHA256 = hex.EncodeToString(digest.Sum(nil))
	// Explicit complete-projection qualification is outside all performance
	// intervals. Its digest lets consumers independently verify the copied store.
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		return nil, fmt.Errorf("fixture writer must not execute providers")
	}))
	if err != nil {
		t.Fatal(err)
	}
	view, err := public.View(t.Context(), identity.ThreadID(id))
	if err != nil || len(view.Items) != manifest.Items || len(view.Turns) != manifest.Turns {
		t.Fatalf("fixture projection: items=%d turns=%d err=%v", len(view.Items), len(view.Turns), err)
	}
	digest.Reset()
	if err := json.NewEncoder(digest).Encode(view.Items); err != nil {
		t.Fatal(err)
	}
	manifest.ItemsSHA256 = hex.EncodeToString(digest.Sum(nil))
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	digest.Reset()
	_, err = io.Copy(digest, file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	manifest.DatabaseSHA256 = hex.EncodeToString(digest.Sum(nil))
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func longThreadFixtureActivity(index int) *tools.ActivityPresentation {
	text := strings.Repeat("Synthetic output 中文🌸\r\n", 100)
	presentation := &tools.ActivityPresentation{Label: "Inspect evidence"}
	switch {
	case index < 872:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererTerminal, tools.TerminalActivityPayload{Operation: "exec", Command: "inspect synthetic evidence", Status: "success", Output: text}
	case index < 964:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererFile, tools.FileActivityPayload{Operation: "read", Path: "synthetic/evidence.txt", Content: text, Status: "success"}
	case index < 993:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererPatch, tools.PatchActivityPayload{Path: "synthetic/evidence.txt", Status: "success", Diff: "--- a/evidence.txt\n+++ b/evidence.txt\n@@ -1 +1 @@\n-old\n+new\n", Summary: text}
	case index < 1012:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererStructured, tools.StructuredActivityPayload{Operation: "inspect", Status: "success", RowsProvided: true, Rows: []tools.StructuredActivityRow{{Title: "Evidence", Content: text}}}
	case index < 1029:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererWebSearch, tools.WebSearchActivityPayload{Operation: "search", Query: "synthetic evidence", Status: "success", ResultsProvided: true, Results: []tools.WebSearchActivityResult{{URL: "https://example.com/evidence", Title: "Synthetic evidence", Snippet: text}}}
	case index < 1045:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererSubAgentOperation, tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationList, Status: "success"}
	default:
		presentation.Renderer, presentation.Payload = tools.ActivityRendererWebFetch, tools.WebFetchActivityPayload{URL: "https://example.com/evidence", Status: "success", StatusCode: 200, ContentPreview: "Synthetic evidence 中文🌸", BytesRead: 32}
	}
	return presentation
}

func TestWriteLongThreadQualificationFixture(t *testing.T) {
	directory := os.Getenv("FLORET_LONG_THREAD_FIXTURE_DIR")
	if directory == "" {
		t.Skip("explicit canonical SQLite qualification fixture writer")
	}
	scale, err := strconv.Atoi(os.Getenv("FLORET_LONG_THREAD_FIXTURE_SCALE"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := writeLongThreadFixture(t, directory, scale, os.Getenv("FLORET_LONG_THREAD_FIXTURE_SINGLE_TURN") == "1")
	t.Logf("wrote %d items, %d tools, %d turns; canonical SHA256 %s", manifest.Items, manifest.Tools, manifest.Turns, manifest.JournalSHA256)
}

func TestLongThreadQualificationFixtureRestartsAndContinues(t *testing.T) {
	directory := t.TempDir()
	manifest := writeLongThreadFixture(t, directory, 1, false)
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(filepath.Join(directory, "floret_threads.sqlite"))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	agent, err := testAgent(&liveBodyGateway{unit: "continued", count: 1, release: closedFixtureChannel()})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), identity.ThreadID(manifest.ThreadID))
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Items) != manifest.Items || len(view.Turns) != 9 || manifest.Items != 1096 || manifest.Tools != 1046 || manifest.LargestTools != 661 {
		t.Fatalf("fixture profile differs: items=%d turns=%d manifest=%+v", len(view.Items), len(view.Turns), manifest)
	}
	for _, turn := range view.Turns {
		if turn.Outcome == nil || *turn.Outcome != TurnOutcomeCompleted || !turn.FinalBoundaryComplete {
			t.Fatalf("incomplete historical turn: %+v", turn)
		}
	}
	admitted, err := service.Send(t.Context(), SendInput{ThreadID: view.ThreadID, RequestKey: "continue-fixture", Input: UserInput{Text: "Continue synthetic history."}})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, admitted.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if completed.Failure != nil || *completed.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("fixture could not continue: %+v", completed.Failure)
	}
}

func closedFixtureChannel() chan struct{} {
	channel := make(chan struct{})
	close(channel)
	return channel
}
