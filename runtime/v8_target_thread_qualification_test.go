package runtime_test

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestV8TargetThreadCopyAcceptsNewTurnAfterProjectionMigration(t *testing.T) {
	sourcePath := os.Getenv("FLORET_V8_SOURCE_DB")
	threadID := os.Getenv("FLORET_V8_THREAD_ID")
	if sourcePath == "" || threadID == "" {
		t.Skip("set FLORET_V8_SOURCE_DB and FLORET_V8_THREAD_ID for an opt-in migration qualification")
	}
	sourceHash, err := qualificationFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "floret-v8-runtime-qualification.sqlite")
	if err := copyQualificationFile(sourcePath, copyPath); err != nil {
		t.Fatal(err)
	}

	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "qualification", Model: "v7", StateCompatibilityKey: "qualification:v7"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "target thread continued"},
			{Type: provider.EventDone, Reason: "stop"},
		}},
	)
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(copyPath)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return runtime.NewAgent(config.AgentConfig{
			Profile:      config.AgentProfile{ID: "v8-runtime-qualification", Name: "V8 Runtime Qualification"},
			SystemPrompt: "Continue the migrated thread using the current execution surface.",
			Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
		}, gateway)
	}))
	if err != nil {
		t.Fatal(err)
	}

	typedThreadID := identity.ThreadID(threadID)
	before, err := service.View(t.Context(), typedThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Items) == 0 {
		t.Fatal("migrated target thread has no historical items")
	}
	prefixIDs := make([]string, len(before.Items))
	for index, item := range before.Items {
		prefixIDs[index] = item.ID
	}
	reader := service.(runtime.ThreadContextReader)
	beforeContext, err := reader.Context(t.Context(), typedThreadID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Send(t.Context(), runtime.SendInput{
		ThreadID:   typedThreadID,
		Input:      runtime.UserInput{Text: "Continue this migrated thread with the current version."},
		RequestKey: "v8-runtime-qualification-new-turn",
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		after, viewErr := service.View(t.Context(), typedThreadID)
		if viewErr != nil {
			t.Fatal(viewErr)
		}
		if after.Activity == runtime.ThreadActivityIdle && len(after.Items) >= len(prefixIDs)+2 {
			for index, id := range prefixIDs {
				if after.Items[index].ID != id {
					t.Fatalf("historical item prefix changed at %d: got %q want %q", index, after.Items[index].ID, id)
				}
			}
			last := after.Items[len(after.Items)-1]
			if last.Kind != runtime.ThreadItemAssistant || last.Text != "target thread continued" {
				t.Fatalf("new turn result=%#v", last)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target thread did not complete a new turn: activity=%q items=%d failure=%#v", after.Activity, len(after.Items), after.Failure)
		}
		time.Sleep(10 * time.Millisecond)
	}

	afterContext, err := reader.Context(t.Context(), typedThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterContext.Compactions) != len(beforeContext.Compactions) {
		t.Fatalf("projection migration created a compaction: before=%d after=%d", len(beforeContext.Compactions), len(afterContext.Compactions))
	}
	requests := gateway.Requests()
	if len(requests) != 1 || requests[0].PreviousState != nil {
		t.Fatalf("projection migration request=%#v", requests)
	}
	askCalls := make(map[string]struct{})
	askResults := make(map[string]struct{})
	for _, message := range requests[0].Messages {
		for _, call := range message.ToolCalls {
			if call.Name == "ask_user" {
				askCalls[call.ID] = struct{}{}
			}
		}
		if message.ToolResult != nil && message.ToolResult.ToolName == "ask_user" {
			askResults[message.ToolResult.CallID] = struct{}{}
		}
		if message.Role == provider.RoleUser && (strings.Contains(message.Text, "interaction_response") || strings.Contains(message.Text, "Agent requested user input")) {
			t.Fatalf("target thread contains textified Ask User history: %#v", message)
		}
	}
	if len(askCalls) != len(askResults) {
		t.Fatalf("target thread Ask User calls=%d results=%d", len(askCalls), len(askResults))
	}
	for callID := range askCalls {
		if _, ok := askResults[callID]; !ok {
			t.Fatalf("target thread Ask User call %q has no paired result", callID)
		}
	}
	if rawExpected := os.Getenv("FLORET_V8_EXPECT_ASK_USER_PAIRS"); rawExpected != "" {
		expected, parseErr := strconv.Atoi(rawExpected)
		if parseErr != nil || expected < 0 {
			t.Fatalf("invalid FLORET_V8_EXPECT_ASK_USER_PAIRS=%q", rawExpected)
		}
		if len(askCalls) != expected {
			t.Fatalf("target thread Ask User pairs=%d, want %d", len(askCalls), expected)
		}
	}
	afterSourceHash, err := qualificationFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if sourceHash != afterSourceHash {
		t.Fatal("source database changed while qualifying its copy")
	}
}

func TestLargeV7RuntimeStartupQualification(t *testing.T) {
	sourcePath := os.Getenv("FLORET_V7_LARGE_SOURCE_DB")
	if sourcePath == "" {
		t.Skip("set FLORET_V7_LARGE_SOURCE_DB for the opt-in large-store runtime qualification")
	}
	sourceHash, err := qualificationFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "floret-v7-large-runtime-qualification.sqlite")
	if err := copyQualificationFile(sourcePath, copyPath); err != nil {
		t.Fatal(err)
	}

	var phases []runtime.StartupPhase
	started := time.Now()
	host, err := runtime.Open(t.Context(), runtime.Options{
		Storage: storage.SQLite(copyPath),
		StartupProgress: runtime.StartupProgressFunc(func(phase runtime.StartupPhase) {
			phases = append(phases, phase)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if elapsed > 15*time.Second {
		t.Fatalf("large v7 runtime startup took %s, want at most 15s", elapsed)
	}
	if len(phases) != 2 || phases[0] != runtime.StartupPhaseMigrating || phases[1] != runtime.StartupPhaseVerifying {
		t.Fatalf("startup phases = %v, want [%s %s]", phases, runtime.StartupPhaseMigrating, runtime.StartupPhaseVerifying)
	}
	afterSourceHash, err := qualificationFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if sourceHash != afterSourceHash {
		t.Fatal("source database changed while qualifying its copy")
	}
	t.Logf("large v7 runtime startup completed in %s", elapsed)
}

func copyQualificationFile(sourcePath, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}

func qualificationFileSHA256(path string) ([sha256.Size]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [sha256.Size]byte{}, err
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum, nil
}
