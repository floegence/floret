package sessiontree

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/floegence/floret/v4/internal/session"
)

func fallbackThreadTitle(message session.Message) string {
	candidates := make([]string, 0, 1+len(message.Attachments)+len(message.References))
	candidates = append(candidates, message.Content)
	for _, attachment := range message.Attachments {
		candidates = append(candidates, attachment.Name)
	}
	for _, reference := range message.References {
		candidates = append(candidates, reference.Label)
	}
	for _, candidate := range candidates {
		title := strings.Join(strings.Fields(candidate), " ")
		if title == "" {
			continue
		}
		runes := []rune(title)
		if len(runes) > MaxThreadTitleRunes {
			title = strings.TrimSpace(string(runes[:MaxThreadTitleRunes]))
		}
		return title
	}
	return ""
}

func installFallbackThreadTitle(meta ThreadMeta, message session.Message, now time.Time) (ThreadMeta, bool, error) {
	if meta.Title != "" {
		return meta, false, nil
	}
	title := fallbackThreadTitle(message)
	if title == "" {
		return meta, false, errors.New("canonical user message cannot produce a fallback title")
	}
	meta.Title = title
	meta.TitleSource = ThreadTitleSourceFallback
	switch meta.TitleStatus {
	case "":
		if meta.TitleGeneration == math.MaxInt64 {
			return meta, false, ErrAuthorityCorrupt
		}
		meta.TitleStatus = ThreadTitleReady
		meta.TitleGeneration++
		meta.TitleToken = ""
		meta.TitleError = ""
	case ThreadTitlePending, ThreadTitleFailed:
	case ThreadTitleReady:
		meta.TitleToken = ""
		meta.TitleError = ""
	default:
		return meta, false, ErrAuthorityCorrupt
	}
	if meta.TitleUpdatedAt.IsZero() {
		meta.TitleUpdatedAt = now.UTC()
	}
	if err := ValidateThreadTitleState(meta); err != nil {
		return meta, false, err
	}
	return meta, true, nil
}

func repairLegacyFallbackThreadTitles(repo *MemoryRepo) (bool, error) {
	repaired := false
	for threadID, meta := range repo.threads {
		if meta.Title != "" {
			continue
		}
		for _, entry := range repo.entries[threadID] {
			if entry.Type != EntryUserMessage || entry.Message.Role != session.User {
				continue
			}
			updated, changed, err := installFallbackThreadTitle(meta, entry.Message, entry.CreatedAt)
			if err != nil {
				return false, err
			}
			if changed {
				repo.threads[threadID] = updated
				repaired = true
			}
			break
		}
	}
	return repaired, nil
}
