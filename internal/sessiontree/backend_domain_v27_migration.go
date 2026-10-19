package sessiontree

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
)

// Version 27 is the only reader of historical Engine message identities. Normal
// execution accepts canonical journal projections exclusively.
func migrateBackendDomainV26ToV27(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV26Memory(memory); err != nil {
		return err
	}
	original := cloneMemoryRepoForStartup(memory)
	for id, entries := range original.entries {
		original.entries[id] = cloneEntries(entries)
	}
	done, visiting := map[string]bool{}, map[string]bool{}
	var migrate func(string) error
	migrate = func(threadID string) error {
		if done[threadID] {
			return nil
		}
		if visiting[threadID] {
			return ErrAuthorityCorrupt
		}
		visiting[threadID] = true
		meta := memory.threads[threadID]
		if meta.ForkedFromThreadID != "" {
			if _, found := memory.threads[meta.ForkedFromThreadID]; found {
				if err := migrate(meta.ForkedFromThreadID); err != nil {
					return err
				}
			}
		}
		entries := memory.entries[threadID]
		for index := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if entries[index].Type != EntryCompaction {
				continue
			}
			checkpoint := cloneEntry(entries[index])
			path, err := pathLocked(memory.threads, memory.entries, threadID, checkpoint.ParentID)
			if err != nil {
				return err
			}
			repaired, err := migrateCompactionBoundaryV27(path, checkpoint)
			if err != nil && meta.ForkedFromThreadID != "" {
				repaired, err = migrateForkCompactionV27(original, memory, path, checkpoint)
			}
			if err != nil {
				return fmt.Errorf("%w: migrate compaction %q: %v", ErrAuthorityCorrupt, checkpoint.ID, err)
			}
			if repaired.FirstKeptEntryID != checkpoint.FirstKeptEntryID || repaired.CompactedThroughEntryID != checkpoint.CompactedThroughEntryID || !slices.Equal(repaired.KeptUserEntryIDs, checkpoint.KeptUserEntryIDs) {
				repaired.Raw = rawForEntry(repaired)
				repaired.RawHash = stableHash(repaired.Raw)
				entries[index] = repaired
				memory.entries[threadID] = entries
			}
		}
		visiting[threadID], done[threadID] = false, true
		return nil
	}
	ids := make([]string, 0, len(memory.threads))
	for id := range memory.threads {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if err := migrate(id); err != nil {
			return err
		}
	}
	return validateBackendDomainV27Memory(memory)
}

func migrateCompactionBoundaryV27(path []Entry, checkpoint Entry) (Entry, error) {
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		return Entry{}, err
	}
	eligible := make([]session.Message, 0, len(history))
	summaryOffset := 0
	for _, message := range history {
		if message.Kind == session.MessageKindCompactionSummary {
			summaryOffset = 1
			continue
		}
		eligible = append(eligible, message)
	}
	// Published writers counted the eligible projection after removing the active
	// summary, but assigned new Engine hashes before removing that summary.
	head, headErr := strconv.Atoi(checkpoint.Metadata["compacted_messages"])
	total, totalErr := strconv.Atoi(checkpoint.Metadata["history_messages"])
	tail, tailErr := strconv.Atoi(checkpoint.Metadata["retained_tail_messages"])
	counts := headErr == nil && totalErr == nil && tailErr == nil && head > 0 && tail >= 0 && head+tail == total
	if !counts && (checkpoint.Metadata["compacted_messages"] != "" || checkpoint.Metadata["history_messages"] != "" || checkpoint.Metadata["retained_tail_messages"] != "") {
		return Entry{}, fmt.Errorf("invalid historical projection counts")
	}
	sources := map[string]session.Message{}
	aliases := map[string][]string{}
	for _, message := range eligible {
		sources[message.EntryID] = message
	}
	controlSources, err := canonicalControlResultSources(path)
	if err != nil {
		return Entry{}, err
	}
	for _, entry := range path {
		if source := controlSources[entry.ID]; source != "" {
			aliases[entry.ID] = append(aliases[entry.ID], source)
			if entry.Message.EntryID != "" {
				aliases[entry.Message.EntryID] = append(aliases[entry.Message.EntryID], source)
			}
			continue
		}
		if entry.Message.EntryID != "" {
			aliases[entry.Message.EntryID] = append(aliases[entry.Message.EntryID], entry.ID)
		}
	}
	canonicalRun, runErr := CurrentRunID(path, checkpoint.TurnID)
	if checkpoint.RunID != "" && runErr == nil && checkpoint.RunID != canonicalRun {
		return Entry{}, fmt.Errorf("checkpoint run conflicts with canonical lifecycle")
	}
	resolve := func(ref string) (string, error) {
		matches := map[string]bool{}
		if _, found := sources[ref]; found {
			matches[ref] = true
		}
		for _, id := range aliases[ref] {
			if _, found := sources[id]; found {
				matches[id] = true
			}
		}
		if len(matches) == 0 && counts {
			runID, _, ok := strings.Cut(ref, ":entry:")
			if ok && runID != "" && runErr == nil && runID == canonicalRun {
				for index, message := range eligible {
					if historicalEngineMessageIDV27(runID, index+summaryOffset, message) == ref {
						matches[message.EntryID] = true
					}
				}
			}
		}
		if len(matches) != 1 {
			return "", fmt.Errorf("boundary %q has %d validated sources", ref, len(matches))
		}
		for id := range matches {
			return id, nil
		}
		panic("unreachable")
	}
	cut, err := resolve(checkpoint.CompactedThroughEntryID)
	if err != nil {
		return Entry{}, err
	}
	first := ""
	if checkpoint.FirstKeptEntryID != "" {
		first, err = resolve(checkpoint.FirstKeptEntryID)
		if err != nil {
			return Entry{}, err
		}
	}
	// Counts corroborate identity evidence. They never select an entry.
	cutIndex := slices.IndexFunc(eligible, func(message session.Message) bool { return message.EntryID == cut })
	if cutIndex < 0 {
		return Entry{}, ErrAuthorityCorrupt
	}
	if counts && (cutIndex+1 != head || len(eligible) != total || len(eligible)-cutIndex-1 != tail) {
		return Entry{}, fmt.Errorf("historical projection counts disagree: head=%d/%d total=%d/%d tail=%d/%d", cutIndex+1, head, len(eligible), total, len(eligible)-cutIndex-1, tail)
	}
	checkpoint.CompactedThroughEntryID, checkpoint.FirstKeptEntryID = cut, first
	for i, ref := range checkpoint.KeptUserEntryIDs {
		// Retained user identities were assigned before execution. Only exact
		// canonical identities or a unique stored alias can identify them.
		if _, found := sources[ref]; found {
			continue
		}
		if ids := aliases[ref]; len(ids) == 1 {
			checkpoint.KeptUserEntryIDs[i] = ids[0]
		}
	}
	if _, err := projectCheckpoint(history, checkpoint, path); err != nil {
		return Entry{}, err
	}
	return checkpoint, nil
}

// This is the exact encoding/json and SHA256 contract of the released Engine
// writer. Keep it frozen here; it is not an alternative runtime identity.
func historicalEngineMessageIDV27(runID string, index int, message session.Message) string {
	raw, err := json.Marshal(map[string]any{"index": index, "run_id": runID, "role": message.Role, "content": message.Content, "reasoning": message.Reasoning, "tool_call_id": message.ToolCallID, "tool_name": message.ToolName, "tool_args": message.ToolArgs, "kind": message.Kind, "compaction": message.CompactionID})
	if err != nil {
		return ""
	}
	return runID + ":entry:" + stableHash(string(raw))[:16]
}

func validateCompactionBoundariesV27(memory *MemoryRepo) error {
	for threadID, entries := range memory.entries {
		for _, entry := range entries {
			if entry.Type != EntryCompaction {
				continue
			}
			path, err := pathLocked(memory.threads, memory.entries, threadID, entry.ID)
			if err != nil {
				return err
			}
			if _, err := BuildContextChecked(path, ContextOptions{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Reproduce the released Fork writer over its retained source path. Position is
// used only to replay that exact writer; every copied fact and parent reference
// must match before its identity can serve as migration authority.
func historicalForkSourcesV27(original *MemoryRepo, threadID string) (map[string]string, error) {
	meta := original.threads[threadID]
	source, err := pathLocked(original.threads, original.entries, meta.ForkedFromThreadID, meta.ForkedFromEntryID)
	if err != nil {
		return nil, err
	}
	source, removed, err := removeEffectAuthorityEntries(source)
	if err != nil {
		return nil, err
	}
	copies := original.entries[threadID]
	if len(copies) < len(source) {
		return nil, ErrAuthorityCorrupt
	}
	mapping := map[string]string{"": ""}
	for i, entry := range source {
		next := cloneEntry(entry)
		copy := copies[i]
		next.ID, next.ThreadID, next.ParentID, next.PathDepth = copy.ID, threadID, mapping[entry.ParentID], int64(i+1)
		next.FirstKeptEntryID, next.CompactedThroughEntryID = mapping[entry.FirstKeptEntryID], mapping[entry.CompactedThroughEntryID]
		next.KeptUserEntryIDs = historicalRewriteEntryIDsV27(entry.KeptUserEntryIDs, mapping)
		next.Metadata = historicalRewriteForkMetadataV27(next.Metadata, mapping)
		if entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return nil, err
			}
			resolution.SteeringID = mapping[resolution.SteeringID]
			if resolution.SteeringID == "" {
				return nil, ErrAuthorityCorrupt
			}
			next.Payload, _ = json.Marshal(resolution)
		}
		if entry.Type == EntryGoal {
			var change GoalChange
			if json.Unmarshal(next.Payload, &change) != nil {
				return nil, ErrAuthorityCorrupt
			}
			if id := mapping[change.State.SuppressedAtInput]; id != "" {
				change.State.SuppressedAtInput = id
			}
			next.Payload, _ = json.Marshal(change)
		}
		if entry.Type == EntryGoalReview {
			var review GoalReview
			if json.Unmarshal(next.Payload, &review) != nil {
				return nil, ErrAuthorityCorrupt
			}
			review.Boundary = mapping[review.Boundary]
			next.Payload, _ = json.Marshal(review)
		}
		if id := next.Metadata[GoalReviewMetadataKey]; id != "" {
			next.Metadata[GoalReviewMetadataKey] = mapping[id]
		}
		refs, known, err := FinalResponseEntryIDs(entry)
		if err != nil {
			return nil, err
		}
		if known {
			for j, ref := range refs {
				if mapping[ref] == "" {
					return nil, ErrAuthorityCorrupt
				}
				refs[j] = mapping[ref]
			}
			encoded, _ := json.Marshal(refs)
			next.Metadata[FinalResponseEntriesMetadataKey] = string(encoded)
		}
		next.Raw, next.RawHash = rawForEntry(next), ""
		next.RawHash = stableHash(next.Raw)
		if !reflect.DeepEqual(next, copy) {
			return nil, fmt.Errorf("fork %q copy %q differs from released writer", threadID, copy.ID)
		}
		mapping[entry.ID] = copy.ID
		for removedID := range removed {
			retainedID, err := resolveRemovedEffectEntryID(removedID, removed)
			if err != nil {
				return nil, err
			}
			if retainedID == entry.ID {
				mapping[removedID] = copy.ID
			}
		}
	}
	return mapping, nil
}

func migrateForkCompactionV27(original, migrated *MemoryRepo, path []Entry, checkpoint Entry) (Entry, error) {
	mapping, err := historicalForkSourcesV27(original, checkpoint.ThreadID)
	if err != nil {
		return Entry{}, err
	}
	meta := original.threads[checkpoint.ThreadID]
	var source Entry
	for _, entry := range migrated.entries[meta.ForkedFromThreadID] {
		if mapping[entry.ID] == checkpoint.ID {
			source = entry
			break
		}
	}
	if source.Type != EntryCompaction {
		return Entry{}, ErrAuthorityCorrupt
	}
	rewrite := func(ref string) (string, error) {
		if ref == "" {
			return "", nil
		}
		if mapped := mapping[ref]; mapped != "" {
			return mapped, nil
		}
		if strings.HasSuffix(ref, ":result") && mapping[strings.TrimSuffix(ref, ":result")] != "" {
			return mapping[strings.TrimSuffix(ref, ":result")] + ":result", nil
		}
		return "", ErrAuthorityCorrupt
	}
	checkpoint.CompactedThroughEntryID, err = rewrite(source.CompactedThroughEntryID)
	if err != nil {
		return Entry{}, err
	}
	checkpoint.FirstKeptEntryID, err = rewrite(source.FirstKeptEntryID)
	if err != nil {
		return Entry{}, err
	}
	checkpoint.KeptUserEntryIDs = slices.Clone(source.KeptUserEntryIDs)
	for i, ref := range checkpoint.KeptUserEntryIDs {
		checkpoint.KeptUserEntryIDs[i], err = rewrite(ref)
		if err != nil {
			return Entry{}, err
		}
	}
	return migrateCompactionBoundaryV27(path, checkpoint)
}

func historicalRewriteEntryIDsV27(ids []string, oldToNew map[string]string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if next := oldToNew[id]; next != "" {
			out = append(out, next)
		}
	}
	return out
}

func historicalRewriteForkMetadataV27(metadata map[string]string, entryIDs map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		next := value
		switch strings.TrimSpace(key) {
		case "entry_id", "parent_entry_id", "input_entry_id", "subagent_input_id":
			if rewritten := strings.TrimSpace(entryIDs[strings.TrimSpace(value)]); rewritten != "" {
				next = rewritten
			}
		case RetrySourceEntryIDMetadataKey:
			if rewritten := strings.TrimSpace(entryIDs[strings.TrimSpace(value)]); rewritten != "" {
				next = rewritten
			}
		}
		out[key] = next
	}
	return out
}
