package sessiontree

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func fallbackThreadTitle(message session.Message) string {
	if title := fallbackTitleCandidate(message.Content); title != "" {
		return title
	}
	for _, attachment := range message.Attachments {
		if title := fallbackTitleCandidate(attachment.Name); title != "" {
			return title
		}
	}
	for _, reference := range message.References {
		if title := fallbackTitleCandidate(reference.Label); title != "" {
			return title
		}
	}
	for _, item := range message.Context {
		if title := fallbackTitleCandidate(item.Title); title != "" {
			return title
		}
	}
	return ""
}

func fallbackTitleCandidate(candidate string) string {
	title := strings.Join(strings.Fields(candidate), " ")
	runes := []rune(title)
	if len(runes) > MaxThreadTitleRunes {
		title = strings.TrimSpace(string(runes[:MaxThreadTitleRunes]))
	}
	return title
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
