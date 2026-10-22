package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// MediaReference identifies host-owned media retained by canonical conversation
// state. It is an opaque retention fact, not authorization to read the resource.
type MediaReference struct {
	ThreadID   identity.ThreadID
	Attachment MessageAttachment
}

// MediaInventory is a canonical retention snapshot. Idle is false while any
// retained execution has not reached a terminal outcome, including waits.
type MediaInventory struct {
	References []MediaReference
	Idle       bool
}

// MediaInventory returns canonical media retention and whether every retained
// execution is terminal. Archived threads, inactive paths, and queue facts are
// included; deleted threads are excluded. Descriptors grant no resource access.
// Hosts must protect not-yet-admitted resources. In particular, a tool-media
// store must hold its write gate before reading the inventory and collect only
// while Idle is true, or while Host execution remains deferred.
func (host *Host) MediaInventory(ctx context.Context) (MediaInventory, error) {
	if ctx == nil {
		return MediaInventory{}, errors.New("media inventory context is required")
	}
	if err := ctx.Err(); err != nil {
		return MediaInventory{}, err
	}
	if err := host.available(); err != nil {
		return MediaInventory{}, err
	}
	reader, ok := host.store.repo.(sessiontree.CanonicalMediaReader)
	if !ok {
		return MediaInventory{}, errors.New("canonical media inventory is unavailable")
	}
	entries, err := reader.CanonicalMediaEntries(ctx)
	if err != nil {
		return MediaInventory{}, runtimeHostError(err)
	}
	return mediaInventoryFromEntries(ctx, entries)
}

// ThreadMediaInventory returns retained media facts for one canonical thread.
// It is intended for bounded host maintenance that processes threads in pages.
func (host *Host) ThreadMediaInventory(ctx context.Context, threadID identity.ThreadID) (MediaInventory, error) {
	if ctx == nil {
		return MediaInventory{}, errors.New("thread media inventory context is required")
	}
	if err := ctx.Err(); err != nil {
		return MediaInventory{}, err
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return MediaInventory{}, err
	}
	if err := host.available(); err != nil {
		return MediaInventory{}, err
	}
	reader, ok := host.store.repo.(sessiontree.CanonicalThreadMediaReader)
	if !ok {
		return MediaInventory{}, errors.New("canonical thread media inventory is unavailable")
	}
	entries, err := reader.CanonicalMediaEntriesForThread(ctx, threadID.String())
	if err != nil {
		return MediaInventory{}, runtimeHostError(err)
	}
	return mediaInventoryFromEntries(ctx, entries)
}

func mediaInventoryFromEntries(ctx context.Context, entries []sessiontree.Entry) (MediaInventory, error) {
	result := MediaInventory{Idle: true}
	type turn struct{ threadID, turnID string }
	unfinished := make(map[turn]bool)
	appendAttachments := func(threadID string, attachments []session.MessageAttachment) {
		for _, attachment := range runtimeMessageAttachments(attachments) {
			result.References = append(result.References, MediaReference{ThreadID: identity.ThreadID(threadID), Attachment: attachment})
		}
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return MediaInventory{}, err
		}
		appendAttachments(entry.ThreadID, entry.Message.Attachments)
		if entry.Message.ToolResult != nil {
			appendAttachments(entry.ThreadID, entry.Message.ToolResult.Attachments)
		}
		if entry.Type == sessiontree.EntryQueueAdded {
			var queued QueuedInput
			if err := json.Unmarshal(entry.Payload, &queued); err != nil {
				return MediaInventory{}, fmt.Errorf("decode canonical media queue %s: %w", entry.ID, err)
			}
			for _, attachment := range cloneMessageAttachments(queued.Input.Attachments) {
				result.References = append(result.References, MediaReference{ThreadID: identity.ThreadID(entry.ThreadID), Attachment: attachment})
			}
		}
		if entry.Type == sessiontree.EntryTurnMarker {
			key := turn{entry.ThreadID, entry.TurnID}
			switch entry.TurnStatus {
			case sessiontree.TurnStarted:
				unfinished[key] = true
			case sessiontree.TurnCompleted, sessiontree.TurnFailed, sessiontree.TurnAborted:
				delete(unfinished, key)
			}
		}
	}
	result.Idle = len(unfinished) == 0
	return result, nil
}
