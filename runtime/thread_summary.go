package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"strings"
	"unicode"
)

// ThreadSummaryReader reads canonical summaries without constructing ThreadView
// history or creating a thread actor. Hosts authorize the thread before reading.
type ThreadSummaryReader interface {
	Summary(context.Context, identity.ThreadID) (ThreadSummary, error)
}

var _ ThreadSummaryReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) Summary(ctx context.Context, id identity.ThreadID) (ThreadSummary, error) {
	if ctx == nil {
		return ThreadSummary{}, errors.New("thread summary requires a context and valid thread ID")
	}
	if err := ctx.Err(); err != nil {
		return ThreadSummary{}, err
	}
	if _, err := identity.ParseThreadID(id.String()); err != nil {
		return ThreadSummary{}, errors.New("thread summary requires a context and valid thread ID")
	}
	if err := service.host.available(); err != nil {
		return ThreadSummary{}, err
	}
	meta, err := service.host.store.repo.Thread(ctx, id.String())
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	if summary, found, err := service.activeThreadSummary(ctx, meta); err != nil {
		return ThreadSummary{}, err
	} else if found {
		return summary, nil
	}
	return service.coldThreadSummary(ctx, meta)
}

// Summary positions are disposable indexes over the actor's accepted items.
// Ordinary growth visits only changed rows; explicit reorder/removal may rebuild.
func (actor *threadRuntimeState) refreshSummaryPositions() {
	if actor.mutations.order {
		actor.summaryIndexed = false
	}
	items := actor.state.view.Items
	if !actor.summaryIndexed {
		actor.summaryPreviewIndex, actor.summaryTimeIndex = -1, -1
		for index := len(items) - 1; index >= 0; index-- {
			if actor.summaryTimeIndex < 0 && !items[index].CreatedAt.IsZero() {
				actor.summaryTimeIndex = index
			}
			if summaryItemText(items[index]) != "" {
				actor.summaryPreviewIndex = index
				break
			}
		}
		actor.summaryIndexed = true
		return
	}
	for _, change := range actor.mutations.items {
		index := actor.itemIndexByID(change.ItemID)
		if index < 0 || change.Kind == ThreadItemRemoved || index == actor.summaryPreviewIndex && summaryItemText(items[index]) == "" || index == actor.summaryTimeIndex && items[index].CreatedAt.IsZero() {
			actor.summaryIndexed = false
			actor.refreshSummaryPositions()
			return
		}
		if !items[index].CreatedAt.IsZero() && index > actor.summaryTimeIndex {
			actor.summaryTimeIndex = index
		}
		if index >= actor.summaryPreviewIndex && summaryItemText(items[index]) != "" {
			actor.summaryPreviewIndex = index
		}
	}
}

func summaryItemText(item ThreadItem) string {
	if !item.textOmitted {
		return strings.TrimSpace(item.Text)
	}
	if item.textSummaryTail {
		return item.textSummary
	}
	return strings.TrimRightFunc(item.textSummary, unicode.IsSpace)
}

// A suffix containing only tools cannot prove that history has no preview.
// Source windows carry the exact latest canonical preview alongside their
// selected interval; custom repositories retain the complete-history reader.
func (actor *threadRuntimeState) hasCompleteSummaryPreview() bool {
	actor.refreshSummaryPositions()
	return actor.sourceWindow == nil || actor.sourceWindow.summary != nil || !actor.sourceWindow.hasAfter && (!actor.sourceWindow.hasBefore || actor.summaryPreviewIndex >= 0)
}

func (actor *threadRuntimeState) threadSummary(meta sessiontree.ThreadMeta) ThreadSummary {
	actor.refreshSummaryPositions()
	view := actor.state.view
	view.Items, view.Interactions = nil, nil
	summary := threadSummaryFromView(meta, view)
	if index := actor.summaryTimeIndex; index >= 0 && index >= actor.summaryPreviewIndex {
		summary.LastItemAt = actor.state.view.Items[index].CreatedAt
	}
	if index := actor.summaryPreviewIndex; index >= 0 {
		item := actor.state.view.Items[index]
		summary.LastItemPreview = truncateThreadSummaryPreview(summaryItemText(item), 160)
		if !item.CreatedAt.IsZero() {
			summary.LastItemAt = item.CreatedAt
		}
	}
	actor.ensureInteractionIndex()
	last := -1
	for id := range actor.pendingInteractionIDs {
		index := actor.interactionIndex[id]
		interaction := actor.state.view.Interactions[index]
		if index > last && interaction.Kind == ThreadInteractionInput && interaction.Input != nil {
			last = index
		}
	}
	if last >= 0 {
		summary.PendingInput = cloneInputPresentation(actor.state.view.Interactions[last].Input)
	}
	if actor.sourceWindow != nil && actor.sourceWindow.summary != nil && (actor.sourceWindow.hasAfter || actor.summaryPreviewIndex < 0) {
		summary.LastItemPreview = actor.sourceWindow.summary.Text
		summary.LastItemAt = actor.sourceWindow.summary.CreatedAt
	}
	return summary
}
