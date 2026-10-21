package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadFactsDoNotHydrateOrRetainPresentation(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	sub, err := service.SubscribeFacts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	emitText(sink, "source", strings.Repeat("中🌸", 100000))
	first := nextUpdate(t, sub)
	if first.ThreadID.String() != actor.threadID || first.ViewVersion == 0 {
		t.Fatal("missing actor facts")
	}
	if first.Window != nil || first.Items != nil || first.ItemOrder != nil || first.Context != nil || first.Goal != nil || first.ParentEvents != nil || first.Turns != nil || first.Queue != nil || first.Interactions != nil || first.ToolCallPreviews != nil || first.RestoredInputs != nil {
		t.Fatal("facts retained presentation")
	}
	raw, err := json.Marshal(first)
	if err != nil || len(raw) > 2048 {
		t.Fatal("unbounded facts", err)
	}
	emitText(sink, "source", "next")
	next := nextUpdate(t, sub)
	if next.BaseVersion != first.ViewVersion {
		t.Fatal("fact continuity lost")
	}
	if err := sub.SetWindow(t.Context(), threadquery.Scope{}, ThreadWindowInput{}); err == nil {
		t.Fatal("facts observer accepted detail interest")
	}
	sub.Close()
	if sub.queuedBytes != 0 || len(sub.updates) != 0 {
		t.Fatal("closed facts retained queue")
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrHostClosed) {
		t.Fatal(err)
	}
}
