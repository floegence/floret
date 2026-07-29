package runtime_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	floretruntime "github.com/floegence/floret/v2/runtime"
)

func TestPublicThreadTitleVocabularyRejectsUnknownValues(t *testing.T) {
	for _, status := range []floretruntime.ThreadTitleStatus{
		floretruntime.ThreadTitleStatusUnset,
		floretruntime.ThreadTitleStatusPending,
		floretruntime.ThreadTitleStatusReady,
		floretruntime.ThreadTitleStatusFailed,
	} {
		if parsed, err := floretruntime.ParseThreadTitleStatus(string(status)); err != nil || parsed != status {
			t.Fatalf("ParseThreadTitleStatus(%q)=(%q, %v)", status, parsed, err)
		}
	}
	for _, raw := range []string{"complete", " READY", "ready "} {
		if _, err := floretruntime.ParseThreadTitleStatus(raw); err == nil {
			t.Fatalf("unknown thread title status %q was accepted", raw)
		}
	}
	for _, source := range []floretruntime.ThreadTitleSource{
		floretruntime.ThreadTitleSourceUnset,
		floretruntime.ThreadTitleSourceHost,
		floretruntime.ThreadTitleSourceProvider,
	} {
		if parsed, err := floretruntime.ParseThreadTitleSource(string(source)); err != nil || parsed != source {
			t.Fatalf("ParseThreadTitleSource(%q)=(%q, %v)", source, parsed, err)
		}
	}
	for _, raw := range []string{"model", "HOST", "host "} {
		if _, err := floretruntime.ParseThreadTitleSource(raw); err == nil {
			t.Fatalf("unknown thread title source %q was accepted", raw)
		}
	}
}

func TestPublicThreadSnapshotValidatesCanonicalStates(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	base := publicIdleThreadSnapshot(now)
	valid := []floretruntime.ThreadSnapshot{
		base,
		withPublicThreadTitle(base, "", "pending", "", "", 1, now),
		withPublicThreadTitle(base, "Host title", "ready", "host", "", 1, now),
		withPublicThreadTitle(base, "Provider title", "ready", "provider", "", 1, now),
		withPublicThreadTitle(base, "", "failed", "", "provider title failed", 1, now),
		func() floretruntime.ThreadSnapshot {
			snapshot := base
			snapshot.Status = floretruntime.ThreadStatusCompleted
			snapshot.LatestTurnID = "turn-1"
			snapshot.LatestRunID = "run-1"
			snapshot.ThroughOrdinal = 1
			return snapshot
		}(),
		func() floretruntime.ThreadSnapshot {
			snapshot := base
			snapshot.Phase = floretruntime.ThreadPhaseTurn
			snapshot.Status = floretruntime.ThreadStatusRunning
			snapshot.LatestTurnID = "turn-1"
			snapshot.LatestRunID = "run-1"
			snapshot.ThroughOrdinal = 1
			snapshot.CanAppendMessage = false
			return snapshot
		}(),
		func() floretruntime.ThreadSnapshot {
			snapshot := base
			snapshot.Status = floretruntime.ThreadStatusWaiting
			snapshot.LatestTurnID = "turn-1"
			snapshot.LatestRunID = "run-1"
			snapshot.ThroughOrdinal = 1
			return snapshot
		}(),
		func() floretruntime.ThreadSnapshot {
			snapshot := base
			snapshot.CanRetry = true
			return snapshot
		}(),
		func() floretruntime.ThreadSnapshot {
			snapshot := withPublicThreadTitle(base, "Later title", "ready", "host", "", 1, now.Add(time.Hour))
			snapshot.CanRetry = true
			return snapshot
		}(),
	}
	for index, snapshot := range valid {
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("valid snapshot %d: %v", index, err)
		}
	}
}

func TestPublicThreadSnapshotRejectsConflictingStates(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	base := publicIdleThreadSnapshot(now)
	cases := map[string]floretruntime.ThreadSnapshot{
		"blank thread identity":      func() floretruntime.ThreadSnapshot { s := base; s.ID = " "; return s }(),
		"unstable thread identity":   func() floretruntime.ThreadSnapshot { s := base; s.ID = " thread-1"; return s }(),
		"zero created time":          func() floretruntime.ThreadSnapshot { s := base; s.CreatedAt = time.Time{}; return s }(),
		"zero updated time":          func() floretruntime.ThreadSnapshot { s := base; s.UpdatedAt = time.Time{}; return s }(),
		"reversed update time":       func() floretruntime.ThreadSnapshot { s := base; s.UpdatedAt = now.Add(-time.Second); return s }(),
		"unknown phase":              func() floretruntime.ThreadSnapshot { s := base; s.Phase = "unknown"; return s }(),
		"unknown status":             func() floretruntime.ThreadSnapshot { s := base; s.Status = "unknown"; return s }(),
		"unknown title status":       withPublicThreadTitle(base, "", "unknown", "", "", 0, time.Time{}),
		"ready title without source": withPublicThreadTitle(base, "Title", "ready", "", "", 1, now),
		"ready multiline title":      withPublicThreadTitle(base, "First\nSecond", "ready", "host", "", 1, now),
		"failed title without error": withPublicThreadTitle(base, "", "failed", "", "", 1, now),
		"running without turn phase": func() floretruntime.ThreadSnapshot {
			s := base
			s.Status, s.LatestTurnID, s.CanAppendMessage = floretruntime.ThreadStatusRunning, "turn-1", false
			return s
		}(),
		"run without turn": func() floretruntime.ThreadSnapshot {
			s := base
			s.LatestRunID = "run-1"
			return s
		}(),
		"turn without run": func() floretruntime.ThreadSnapshot {
			s := base
			s.Status, s.LatestTurnID, s.CanAppendMessage = floretruntime.ThreadStatusCompleted, "turn-1", true
			s.ThroughOrdinal = 1
			return s
		}(),
		"latest execution without ordinal": func() floretruntime.ThreadSnapshot {
			s := base
			s.Status, s.LatestTurnID, s.LatestRunID, s.CanAppendMessage = floretruntime.ThreadStatusCompleted, "turn-1", "run-1", true
			return s
		}(),
		"unstable latest turn": func() floretruntime.ThreadSnapshot {
			s := base
			s.Status, s.LatestTurnID, s.LatestRunID, s.ThroughOrdinal, s.CanAppendMessage = floretruntime.ThreadStatusCompleted, " turn-1", "run-1", 1, true
			return s
		}(),
		"negative ordinal": func() floretruntime.ThreadSnapshot {
			s := base
			s.ThroughOrdinal = -1
			return s
		}(),
	}
	for name, snapshot := range cases {
		t.Run(name, func(t *testing.T) {
			if err := snapshot.Validate(); err == nil {
				t.Fatal("conflicting public snapshot was accepted")
			}
		})
	}
}

func TestPublicThreadSummaryUsesSameStateContract(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	summary := floretruntime.ThreadSummary{
		ID: "thread-1", CreatedAt: now, UpdatedAt: now,
		Phase: floretruntime.ThreadPhaseIdle, Status: floretruntime.ThreadStatusIdle,
		CanAppendMessage: true,
	}
	if err := summary.Validate(); err != nil {
		t.Fatal(err)
	}
	valid := []floretruntime.ThreadSummary{
		summary,
		func() floretruntime.ThreadSummary { s := summary; s.CanRetry = true; return s }(),
		func() floretruntime.ThreadSummary {
			s := summary
			s.Status, s.LatestTurnID, s.CanAppendMessage = floretruntime.ThreadStatusWaiting, "turn-1", true
			return s
		}(),
		func() floretruntime.ThreadSummary {
			s := summary
			s.Title, s.TitleStatus, s.TitleSource = "Later title", "ready", "provider"
			s.TitleUpdatedAt, s.TitleGeneration = now.Add(time.Hour), 1
			return s
		}(),
	}
	for index, candidate := range valid {
		if err := candidate.Validate(); err != nil {
			t.Fatalf("valid summary %d: %v", index, err)
		}
	}
	invalid := map[string]floretruntime.ThreadSummary{
		"blank identity":       func() floretruntime.ThreadSummary { s := summary; s.ID = " "; return s }(),
		"unstable identity":    func() floretruntime.ThreadSummary { s := summary; s.ID = "thread-1 "; return s }(),
		"zero created time":    func() floretruntime.ThreadSummary { s := summary; s.CreatedAt = time.Time{}; return s }(),
		"reversed update time": func() floretruntime.ThreadSummary { s := summary; s.UpdatedAt = now.Add(-time.Second); return s }(),
		"unknown phase":        func() floretruntime.ThreadSummary { s := summary; s.Phase = "unknown"; return s }(),
		"unknown status":       func() floretruntime.ThreadSummary { s := summary; s.Status = "unknown"; return s }(),
		"unstable latest turn": func() floretruntime.ThreadSummary {
			s := summary
			s.Status, s.LatestTurnID, s.CanAppendMessage = floretruntime.ThreadStatusCompleted, "turn-1 ", true
			return s
		}(),
		"incomplete ready title": func() floretruntime.ThreadSummary { s := summary; s.TitleStatus = "ready"; return s }(),
	}
	for name, candidate := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid public summary was accepted")
			}
		})
	}
}

func TestPublicThreadSnapshotJSONRoundTripPreservesValidatedVocabulary(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	want := withPublicThreadTitle(publicIdleThreadSnapshot(now), "Canonical title", "ready", "provider", "", 2, now)
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got floretruntime.ThreadSnapshot
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped snapshot: %v", err)
	}
	if got.TitleStatus != want.TitleStatus || got.TitleSource != want.TitleSource || got.Title != want.Title {
		t.Fatalf("round-tripped title state=%#v, want %#v", got, want)
	}
}

func TestPublicThreadSnapshotRejectsOverlongTitleAndUnstableRunIdentity(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	overlong := withPublicThreadTitle(
		publicIdleThreadSnapshot(now), strings.Repeat("x", 201), "ready", "host", "", 1, now,
	)
	if err := overlong.Validate(); err == nil {
		t.Fatal("overlong public title was accepted")
	}
	running := publicIdleThreadSnapshot(now)
	running.Phase = floretruntime.ThreadPhaseTurn
	running.Status = floretruntime.ThreadStatusRunning
	running.LatestTurnID = "turn-1"
	running.LatestRunID = " run-1"
	running.ThroughOrdinal = 1
	running.CanAppendMessage = false
	if err := running.Validate(); err == nil {
		t.Fatal("non-trim-stable latest run id was accepted")
	}
}

func publicIdleThreadSnapshot(now time.Time) floretruntime.ThreadSnapshot {
	return floretruntime.ThreadSnapshot{
		ID: "thread-1", CreatedAt: now, UpdatedAt: now,
		Phase: floretruntime.ThreadPhaseIdle, Status: floretruntime.ThreadStatusIdle,
		CanAppendMessage: true,
	}
}

func withPublicThreadTitle(snapshot floretruntime.ThreadSnapshot, title string, status floretruntime.ThreadTitleStatus, source floretruntime.ThreadTitleSource, titleError string, generation int64, updatedAt time.Time) floretruntime.ThreadSnapshot {
	snapshot.Title = title
	snapshot.TitleStatus = status
	snapshot.TitleSource = source
	snapshot.TitleError = titleError
	snapshot.TitleGeneration = generation
	snapshot.TitleUpdatedAt = updatedAt
	return snapshot
}
