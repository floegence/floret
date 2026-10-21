package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

// retainCanonicalItem drops reconstructible text from the actor. The locator
// and exact extent are disposable metadata; canonical storage remains the only
// owner of accepted content. Live drafts and transformed control labels stay
// with their existing owner until their own representation can be reconstructed.
func retainCanonicalItem(item ThreadItem, leafID string) ThreadItem {
	if !item.canonicalText || item.CanonicalOrdinal == 0 {
		return item
	}
	if !item.textOmitted {
		item.textBytes, item.textNonSpace = len(item.Text), threadItemHasText(item)
		item.textSummary, item.textSummaryTail = canonicalTextSummary("", item.Text, false)
	}
	// Complete adapters need source membership for turn/Fork assembly. The
	// retained actor resolves text through the canonical source manifest and
	// must not clone this growing private list on every accepted segment.
	item.sourceEntryIDs = nil
	item.Text, item.textOmitted, item.textLeafID = "", true, leafID
	return item
}

func canonicalTextSummary(prefix, appended string, hasTail bool) (string, bool) {
	if hasTail {
		return prefix, true
	}
	if prefix == "" {
		appended = strings.TrimLeftFunc(appended, unicode.IsSpace)
	}
	remaining := 160 - utf8.RuneCountInString(prefix)
	part := truncateThreadSummaryPreview(appended, remaining)
	return strings.Clone(prefix + part), strings.TrimSpace(appended[len(part):]) != ""
}

// readItemText reconstructs only the requested prefix, or the exact frozen
// extent for an explicit full adapter. It never reads a complete source entry.
func (service *threadRuntimeService) readItemText(ctx context.Context, threadID string, item ThreadItem, limit int) (string, error) {
	if !item.textOmitted {
		if limit >= 0 {
			text, _ := boundedThreadText(item.Text, limit)
			return text, nil
		}
		return item.Text, nil
	}
	want := item.textBytes
	if limit >= 0 {
		want = min(want, limit)
	}
	if want == 0 {
		return "", nil
	}
	if item.liveBody != nil {
		var result strings.Builder
		for result.Len() < want {
			part, err := item.liveBody.Read(ctx, result.Len(), max(4, min(16<<10, want-result.Len())), item.textBytes)
			if err != nil {
				return "", err
			}
			if part == "" {
				return "", ErrAuthorityCorrupt
			}
			text, partial := boundedThreadText(part, want-result.Len())
			result.WriteString(text)
			if partial {
				break
			}
		}
		return result.String(), nil
	}
	reader, ok := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	if !ok {
		return "", ErrUnsupportedStoreCapability
	}
	var result strings.Builder
	finished := false
	field := "content"
	if item.Kind == ThreadItemThinking {
		field = "reasoning"
	}
	read := func(entryID string, bytes int) error {
		for offset := 0; offset < bytes && result.Len() < want; {
			chunk, err := reader.ReadCanonicalBody(ctx, threadID, entryID, field, offset, max(4, min(16<<10, want-result.Len())))
			if err != nil {
				return runtimeHostError(err)
			}
			if chunk.Text == "" {
				return ErrAuthorityCorrupt
			}
			text, partial := boundedThreadText(chunk.Text, want-result.Len())
			result.WriteString(text)
			offset += len(chunk.Text)
			if partial {
				finished = true
				return nil
			}
		}
		return nil
	}
	if item.Kind == ThreadItemUser || item.Kind == ThreadItemSteering {
		if err := read(item.ID, item.textBytes); err != nil {
			return "", err
		}
	} else {
		manifest, ok := service.host.store.repo.(sessiontree.SourceBodyPageReader)
		if !ok {
			return "", ErrUnsupportedStoreCapability
		}
		options := sessiontree.SourceBodyPageOptions{ThreadID: threadID, LeafID: item.textLeafID, SourceID: item.ID, Field: field, Limit: 150}
		for {
			page, err := manifest.SourceBodyPage(ctx, options)
			if err != nil {
				return "", runtimeHostError(err)
			}
			for _, part := range page.Bodies {
				if err := read(part.EntryID, part.Bytes); err != nil {
					return "", err
				}
				if result.Len() >= want || finished {
					break
				}
			}
			if result.Len() >= want || finished || !page.HasMore {
				break
			}
			options.LeafID, options.AfterEntryID = page.LeafID, page.NextEntryID
		}
	}
	if limit < 0 && result.Len() != item.textBytes {
		return "", fmt.Errorf("%w: canonical text extent is unavailable", ErrAuthorityCorrupt)
	}
	return result.String(), nil
}

// Custom repositories implementing only JournalRepo keep complete reads. Body
// eviction requires both exact source readers; all built-in stores provide them.
func (actor *threadRuntimeState) retainItem(item ThreadItem, leafID string) ThreadItem {
	if actor.service == nil {
		return item
	}
	if _, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyReader); !ok {
		return item
	}
	if _, ok := actor.service.host.store.repo.(sessiontree.SourceBodyPageReader); !ok {
		return item
	}
	item = retainCanonicalItem(item, leafID)
	if _, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader); ok {
		item = retainCanonicalActivity(item)
	}
	return item
}

func (actor *threadRuntimeState) retainItems() {
	if actor.canonical == nil {
		return
	}
	if _, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyReader); !ok {
		return
	}
	if _, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader); ok {
		actor.canonical.items.restoreActivity = func(item ThreadItem) (ThreadItem, error) {
			return actor.service.completeItemActivity(context.Background(), actor.threadID, item)
		}
	}
	for index, item := range actor.state.view.Items {
		actor.state.view.Items[index] = actor.retainItem(item, actor.canonical.leafID)
	}
	actor.canonical.items.releaseText()
}

func (service *threadRuntimeService) completeViewResult(ctx context.Context, view ThreadView, err error) (ThreadView, error) {
	if err != nil {
		return ThreadView{}, err
	}
	for index, item := range view.Items {
		item, err = service.completeItemActivity(ctx, view.ThreadID.String(), item)
		if err != nil {
			return ThreadView{}, err
		}
		view.Items[index] = item
		if !item.textOmitted {
			continue
		}
		text, err := service.readItemText(ctx, view.ThreadID.String(), item, -1)
		if err != nil {
			return ThreadView{}, err
		}
		view.Items[index].Text = text
		view.Items[index].textOmitted = false
		view.Items[index].liveBody = nil
	}
	return view, nil
}

func (actor *threadRuntimeState) windowItem(ctx context.Context, item ThreadItem, limit int) (ThreadItem, error) {
	// Text and reconstructible activity share one per-item source allowance.
	// Pending controls remain in their existing owner and are never truncated.
	activityLimit := limit
	if item.activityOmitted && (item.textBytes > 0 || item.Text != "") {
		activityLimit, limit = limit/2, limit-limit/2
	}
	var err error
	item, err = actor.windowActivity(ctx, item, activityLimit)
	if err != nil {
		return ThreadItem{}, err
	}
	if item.textOmitted {
		item = boundedWindowItem(item, limit)
		text, err := actor.service.readItemText(ctx, actor.threadID, item, limit)
		if err != nil {
			return ThreadItem{}, err
		}
		item.Text, item.textOmitted = text, false
		item.liveBody = nil
		if len(text) == item.textBytes {
			item.Body = nil
		}
	}
	return boundedWindowItem(item, limit), nil
}

// Completed ordinary tool presentations are reconstructible from their exact
// call/result fields. Activity identity, status and attention remain resident;
// pending/running presentations and Goal's derived presentation keep ownership.
func retainCanonicalActivity(item ThreadItem) ThreadItem {
	if item.Activity == nil || item.Activity.Status == observation.ActivityStatusRunning || len(item.activityEntryIDs) == 0 || item.activityOmitted {
		return item
	}
	activity := *item.Activity
	activity.Presentation = nil
	item.Activity, item.activityOmitted = &activity, true
	return item
}

func (service *threadRuntimeService) completeItemActivity(ctx context.Context, threadID string, item ThreadItem) (ThreadItem, error) {
	if !item.activityOmitted {
		return item, nil
	}
	if item.Activity == nil || len(item.activityEntryIDs) == 0 {
		return ThreadItem{}, ErrAuthorityCorrupt
	}
	reader, ok := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	if !ok {
		return ThreadItem{}, ErrUnsupportedStoreCapability
	}
	var presentation *tools.ActivityPresentation
	for index, entryID := range item.activityEntryIDs {
		var raw strings.Builder
		for offset := 0; ; {
			part, err := reader.ReadCanonicalBody(ctx, threadID, entryID, "activity_json", offset, 16<<10)
			if err != nil {
				return ThreadItem{}, runtimeHostError(err)
			}
			if part.Bytes == 0 {
				break
			}
			if part.Text == "" || offset+len(part.Text) > part.Bytes {
				return ThreadItem{}, ErrAuthorityCorrupt
			}
			raw.WriteString(part.Text)
			offset += len(part.Text)
			if offset == part.Bytes {
				break
			}
		}
		var next *tools.ActivityPresentation
		if raw.Len() > 0 {
			next = &tools.ActivityPresentation{}
			if err := json.Unmarshal([]byte(raw.String()), next); err != nil {
				return ThreadItem{}, fmt.Errorf("%w: activity body: %v", ErrAuthorityCorrupt, err)
			}
		} else {
			status := observation.ActivityStatusRunning
			if index == len(item.activityEntryIDs)-1 {
				status = item.Activity.Status
			}
			label := strings.TrimSpace(item.Activity.ToolName)
			if label == "" {
				label = "Tool activity"
			}
			next = &tools.ActivityPresentation{Label: label, Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: string(status), Operation: "tool", DisplayName: label, Summary: "Historical tool activity"}}
		}
		presentation = tools.MergeActivityPresentations(presentation, next)
	}
	activity := *item.Activity
	activity.Presentation = presentation
	item.Activity, item.activityOmitted = &activity, false
	return item, nil
}

func (actor *threadRuntimeState) windowActivity(ctx context.Context, item ThreadItem, limit int) (ThreadItem, error) {
	if !item.activityOmitted {
		return item, nil
	}
	metadata, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
	if !ok {
		return ThreadItem{}, ErrUnsupportedStoreCapability
	}
	bodies := make([]ThreadActivityBody, 0, len(item.activityEntryIDs))
	bytes := 0
	for _, entryID := range item.activityEntryIDs {
		body, err := metadata.CanonicalBodyMetadata(ctx, actor.threadID, entryID, "activity_json")
		if err != nil {
			return ThreadItem{}, runtimeHostError(err)
		}
		bytes += body.Bytes
		bodies = append(bodies, ThreadActivityBody{EntryID: entryID, Bytes: body.Bytes, SHA256: body.SHA256})
	}
	if bytes <= limit {
		return actor.service.completeItemActivity(ctx, actor.threadID, item)
	}
	item.ActivityBodies = bodies
	item.activityEntryIDs = nil
	return item, nil
}
