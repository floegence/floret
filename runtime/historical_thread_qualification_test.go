package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

// Real incident data stays outside the repository and is never opened for writing.
// The ordinary released-writer fixture and adoption gates do not require this test.
func TestHistoricalThreadCopyMigratesAndAcceptsNewTurn(t *testing.T) {
	sourcePath := os.Getenv("FLORET_MIGRATION_DB")
	target := identity.ThreadID(os.Getenv("FLORET_MIGRATION_THREAD_ID"))
	if sourcePath == "" || target == "" {
		t.Skip("set FLORET_MIGRATION_DB and FLORET_MIGRATION_THREAD_ID for isolated incident qualification")
	}
	sourceHash := qualificationFileHash(t, sourcePath)
	t.Cleanup(func() {
		if qualificationFileHash(t, sourcePath) != sourceHash {
			t.Error("qualification changed source database")
		}
	})
	copyPath := filepath.Join(t.TempDir(), "incident-copy.sqlite")
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var host *Host
	var service ThreadService
	open := func(steps ...florettest.Step) *florettest.ScriptedGateway {
		t.Helper()
		started := time.Now()
		var err error
		host, err = Open(t.Context(), Options{Storage: storage.SQLite(copyPath)})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("opened qualified copy in %s", time.Since(started))
		fake := florettest.NewScriptedGateway(provider.Identity{Provider: "qualification", Model: "fake", StateCompatibilityKey: "qualification:v11"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
		service, err = host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			return NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "qualification", Name: "Qualification"}, SystemPrompt: "Continue the migrated conversation.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, fake, WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}))
		}))
		if err != nil {
			t.Fatal(err)
		}
		return fake
	}
	closeHost := func() {
		t.Helper()
		if err := host.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if host != nil {
			_ = host.Shutdown(context.Background())
		}
	})
	step := func() florettest.Step {
		return florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Qualified historical continuation."}, {Type: provider.EventDone, Reason: "stop"}}}
	}
	steps := make([]florettest.Step, 20)
	for i := range steps {
		steps[i] = step()
	}
	fake := open(steps...)
	metas, err := sessiontree.ListThreads(t.Context(), host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	checkpointThreads := map[string]bool{}
	checkpointCount := 0
	for _, meta := range metas {
		entries, err := host.store.repo.Entries(t.Context(), meta.ID)
		if err != nil {
			t.Fatal(err)
		}
		path, err := host.store.repo.Path(t.Context(), meta.ID, "")
		if err != nil {
			t.Fatal(err)
		}
		messages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
		if err != nil {
			t.Fatalf("thread %s: %v", meta.ID, err)
		}
		hasCheckpoint := false
		for _, entry := range entries {
			if entry.Type == sessiontree.EntryCompaction {
				hasCheckpoint = true
			}
		}
		if err := session.ValidateToolHistory(messages); err != nil && hasCheckpoint {
			t.Fatalf("checkpoint thread %s: %v", meta.ID, err)
		}

		for _, entry := range entries {
			if entry.Type != sessiontree.EntryCompaction {
				continue
			}
			checkpointCount++
			checkpointThreads[meta.ID] = true
			path, err := host.store.repo.Path(t.Context(), meta.ID, entry.ID)
			if err != nil {
				t.Fatal(err)
			}
			messages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := session.ValidateToolHistory(messages); err != nil {
				t.Fatal(err)
			}
			if meta.ID == target.String() && entry.CompactionGeneration == 5 {
				calls, results := 0, 0
				for _, message := range messages[1:] {
					if message.Role == session.Assistant && message.ToolCallID != "" {
						calls++
					}
					if message.Role == session.Tool {
						results++
					}
				}
				if len(messages)-1 != 40 || calls != 20 || results != 20 {
					t.Fatalf("fifth checkpoint tail=%d calls=%d results=%d", len(messages)-1, calls, results)
				}
				t.Log("verified fifth checkpoint: 40 messages / 20 complete exchanges")
			}
		}
	}
	if checkpointCount != 9 {
		t.Fatalf("checkpoints=%d, want 9", checkpointCount)
	}
	t.Logf("projected all %d threads and strictly validated %d checkpoints", len(metas), checkpointCount)
	ids := make([]string, 0, len(checkpointThreads))
	for id := range checkpointThreads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	complete := func(threadID identity.ThreadID, accepted ThreadView) ThreadView {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			view, err := service.View(t.Context(), threadID)
			if err != nil {
				t.Fatal(err)
			}
			if view.Activity == ThreadActivityIdle && view.TurnID == accepted.TurnID {
				if view.Failure != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCompleted {
					t.Fatalf("qualification failed: thread=%s failure=%+v", threadID, view.Failure)
				}
				return view
			}
			if time.Now().After(deadline) {
				t.Fatalf("thread %s remained busy", threadID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	send := func(threadID identity.ThreadID, key string) {
		t.Helper()
		before, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := service.Send(t.Context(), SendInput{ThreadID: threadID, Input: UserInput{Text: "Continue with preserved historical results."}, RequestKey: RequestKey(key)})
		if err != nil {
			t.Fatal(err)
		}
		after := complete(threadID, accepted)
		if len(after.Items) <= len(before.Items) {
			t.Fatal("new output missing")
		}
		for i, item := range before.Items {
			if after.Items[i].ID != item.ID {
				t.Fatal("historical item identity changed")
			}
		}
	}
	for _, id := range ids {
		send(identity.ThreadID(id), "qualification-"+id)
	}
	if len(fake.Requests()) < len(ids) {
		t.Fatalf("checkpoint threads=%d provider requests=%d", len(ids), len(fake.Requests()))
	}
	forked, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: target, RequestKey: "qualification-fork"})
	if err != nil {
		t.Fatal(err)
	}
	send(forked.ThreadID, "qualification-fork-send")
	closeHost()
	open(florettest.Step{ReturnError: errors.New("injected qualification failure")}, step(), step(), step())
	accepted, err := service.Send(t.Context(), SendInput{ThreadID: target, Input: UserInput{Text: "Exercise safe canonical Retry."}, RequestKey: "qualification-failure"})
	if err != nil {
		t.Fatal(err)
	}
	var failed ThreadView
	deadline := time.Now().Add(60 * time.Second)
	for {
		failed, err = service.View(t.Context(), target)
		if err != nil {
			t.Fatal(err)
		}
		if failed.TurnID == accepted.TurnID && failed.Activity == ThreadActivityIdle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("injected failure remained busy: activity=%s", failed.Activity)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failed.Failure == nil {
		t.Fatal("injected provider failure was not preserved")
	}
	retried, err := service.Retry(t.Context(), RetryInput{ThreadID: target, SourceTurnID: failed.TurnID, RequestKey: "qualification-retry"})
	if err != nil {
		t.Fatal(err)
	}
	complete(target, retried)
	compacted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: target, RequestKey: "qualification-recompact"})
	if err != nil {
		t.Fatal(err)
	}
	complete(target, compacted)
	closeHost()
	fake = open(step(), step())
	send(target, "qualification-after-recompact")
	nested, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: forked.ThreadID, RequestKey: "qualification-nested"})
	if err != nil {
		t.Fatal(err)
	}
	send(nested.ThreadID, "qualification-nested-send")
	closeHost()
	t.Logf("qualified explicit Send for %d checkpoint threads, restart, Retry, recompaction, Fork and nested Fork; source unchanged", len(ids))
}

func qualificationFileHash(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}
