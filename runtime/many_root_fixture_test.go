package runtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
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
)

// The ordered ID file is for setup only. A reader can open the oldest link
// from the manifest without listing threads or inspecting SQLite records.
type manyRootFixtureManifest struct {
	Profile        string `json:"profile"`
	Roots          int    `json:"roots"`
	OldestID       string `json:"oldest_thread_id"`
	NewestID       string `json:"newest_thread_id"`
	OldestTitle    string `json:"oldest_title"`
	OldestInput    string `json:"oldest_input"`
	IDsSHA256      string `json:"root_ids_sha256"`
	DatabaseSHA256 string `json:"database_sha256"`
}

func fixtureFileDigest(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func fixtureStoreDigest(t *testing.T, fence string) string {
	t.Helper()
	digest := sha256.New()
	fmt.Fprintln(digest, fixtureFileDigest(t, fence))
	root := os.DirFS(fence + ".d")
	err := fs.WalkDir(root, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected fixture file %q", name)
		}
		_, err = fmt.Fprintf(digest, "%q %s\n", name, fixtureFileDigest(t, filepath.Join(fence+".d", name)))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func writeManyRootFixture(t *testing.T, directory string, count int) {
	t.Helper()
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 || count < 1 || count > 100000 {
		t.Fatalf("fixture needs a fresh empty directory and 1..100000 roots: count=%d err=%v", count, err)
	}
	ctx := t.Context()
	database := filepath.Join(directory, "floret_threads.sqlite")
	host, err := Open(ctx, Options{Storage: storage.ThreadSQLite(database), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	idsPath := filepath.Join(directory, "root_ids.txt")
	file, err := os.OpenFile(idsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ids := bufio.NewWriter(file)
	m := manyRootFixtureManifest{Profile: "short-root-pairs-v1", Roots: count, OldestInput: "早期笔记正文", OldestTitle: "Notebook 000000"}
	turns := host.store.repo.(sessiontree.RuntimeTurnRepo)
	journal := host.store.repo.(sessiontree.RuntimeJournalRepo)
	titles := host.store.repo.(sessiontree.ThreadTitleAuthorityRepo)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range count {
		suffix := fmt.Sprintf("%06d", i)
		id, turn, run := "thread-notebook-"+suffix, "turn-notebook-"+suffix, "run-notebook-"+suffix
		now := start.Add(time.Duration(i) * time.Second)
		_, err := host.store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: id, CreatedAt: now})
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if _, err := titles.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{ThreadID: id, Title: "Notebook " + suffix, Now: now}); err != nil {
			t.Fatalf("title %s: %v", id, err)
		}
		input := "Synthetic note " + suffix
		if i == 0 {
			input, m.OldestID = m.OldestInput, id
		}
		_, err = turns.AcceptTurn(ctx, sessiontree.AcceptTurnRequest{ThreadID: id, TurnID: turn, RunID: run, LogicalRequestID: turn, RequestFingerprint: turn, InputRequestFingerprint: turn + "-input", Input: session.Message{Role: session.User, Content: input}, Now: now})
		if err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
		answer := "answer-" + suffix
		_, err = journal.AppendRuntimeFacts(ctx, id, []sessiontree.Entry{{ID: answer, ThreadID: id, TurnID: turn, RunID: run, Type: sessiontree.EntryAssistantMessage, CreatedAt: now.Add(time.Millisecond), Message: session.Message{Role: session.Assistant, ContentSourceID: answer, Content: "Recorded note " + suffix}}})
		if err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
		final, err := json.Marshal([]string{answer})
		if err != nil {
			t.Fatal(err)
		}
		_, err = turns.FinishTurn(ctx, sessiontree.FinishTurnRequest{ThreadID: id, TurnID: turn, RunID: run, Status: sessiontree.TurnCompleted, TerminalEntryID: turn + "-done", OutcomeFingerprint: turn + "-completed", Metadata: map[string]string{sessiontree.FinalResponseEntriesMetadataKey: string(final)}, Now: now.Add(2 * time.Millisecond)})
		if err != nil {
			t.Fatalf("finish %s: %v", id, err)
		}
		if _, err := fmt.Fprintln(ids, id); err != nil {
			t.Fatal(err)
		}
		m.NewestID = id
	}
	if err := ids.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	m.IDsSHA256, m.DatabaseSHA256 = fixtureFileDigest(t, idsPath), fixtureStoreDigest(t, database)
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWriteManyRootQualificationFixture(t *testing.T) {
	directory := os.Getenv("FLORET_MANY_ROOT_FIXTURE_DIR")
	if directory == "" {
		t.Skip("explicit canonical SQLite qualification fixture writer")
	}
	count, err := strconv.Atoi(os.Getenv("FLORET_MANY_ROOT_FIXTURE_ROOTS"))
	if err != nil || (count != 1000 && count != 10000 && count != 100000) {
		t.Fatal("expected 1000, 10000 or 100000 roots")
	}
	writeManyRootFixture(t, directory, count)
}

func TestManyRootQualificationFixtureRestartsAndContinues(t *testing.T) {
	dir := t.TempDir()
	writeManyRootFixture(t, dir, 3)
	database := filepath.Join(dir, "floret_threads.sqlite")
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manyRootFixtureManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m.Profile != "short-root-pairs-v1" || m.Roots != 3 || fixtureStoreDigest(t, database) != m.DatabaseSHA256 {
		t.Fatal("invalid fixture manifest")
	}
	ids, err := os.ReadFile(filepath.Join(dir, "root_ids.txt"))
	if err != nil {
		t.Fatal(err)
	}
	ordered := strings.Fields(string(ids))
	if fmt.Sprintf("%x", sha256.Sum256(ids)) != m.IDsSHA256 || len(ordered) != 3 || ordered[0] != m.OldestID || ordered[2] != m.NewestID {
		t.Fatal("incorrect ordered roots")
	}
	host, err := Open(t.Context(), Options{Storage: storage.ThreadSQLite(database)})
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
	var previous time.Time
	for i, raw := range ordered {
		id := identity.ThreadID(raw)
		s, err := service.(ThreadSummaryReader).Summary(t.Context(), id)
		if err != nil || s.ID != id || s.ParentThreadID != "" || !s.CreatedAt.After(previous) || s.Title != fmt.Sprintf("Notebook %06d", i) || (i == 0 && s.Title != m.OldestTitle) {
			t.Fatalf("root %d: %+v err=%v", i, s, err)
		}
		previous = s.CreatedAt
		v, err := service.View(t.Context(), id)
		if err != nil || len(v.Turns) != 1 || !v.Turns[0].FinalBoundaryComplete || v.Turns[0].Outcome == nil || *v.Turns[0].Outcome != TurnOutcomeCompleted || len(v.Items) != 2 || v.Items[1].Text != fmt.Sprintf("Recorded note %06d", i) || (v.Items[0].Text == m.OldestInput) != (i == 0) {
			t.Fatalf("root %d incomplete or wrong history: %+v err=%v", i, v, err)
		}
	}
	continued := continueQualificationFixture(t, service, identity.ThreadID(m.OldestID))
	if len(continued.Turns) != 2 || len(continued.Items) != 4 || continued.Items[0].Text != m.OldestInput || continued.Items[3].Text != "continued" {
		t.Fatal("continued fixture differs")
	}
}
