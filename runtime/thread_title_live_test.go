package runtime

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

// TestLiveDeepSeekAutomaticTitleLanguage sends only title requests to the real
// configured provider. Ordinary agent execution is inert and storage is isolated.
func TestLiveDeepSeekAutomaticTitleLanguage(t *testing.T) {
	if os.Getenv("FLORET_LIVE_TITLE") != "1" {
		t.Skip("set FLORET_LIVE_TITLE=1 with DEEPSEEK_BASE_URL, DEEPSEEK_MODEL and DEEPSEEK_API_KEY")
	}
	model := os.Getenv("DEEPSEEK_MODEL")
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{
		BaseURL: os.Getenv("DEEPSEEK_BASE_URL"), Model: model, APIKey: os.Getenv("DEEPSEEK_API_KEY"),
		StateCompatibilityKey: "live-title-qualification",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, input string
		han         bool
	}{
		{"directory", "Look at the current working directory and explain in everyday language what is here, what it is for, and how I can use it.\n\nFirst inspect the top-level contents and any necessary introductory documents to determine whether this is a software project, an application, a collection of reference materials, or another kind of folder.\n\nIf it is a software project, focus on the problem it solves, its main features, who it is for, and where to start. Based on the actual documentation and current environment, give me the shortest getting-started steps and one concrete usage example. Explain which prerequisites are already available and which are missing.\n\nIf it contains documents or reference materials, summarize the main content, identify the best files to read first, and suggest a reading order.\n\nDeliver a concise usage guide. Clearly state anything uncertain rather than guessing. If the folder contains several independent projects, give an overview first and let me choose. This task is for understanding only; do not install or modify anything.", false},
		{"health", "Help me understand the computer I am currently connected to with a brief system health check.", false},
		{"chinese", "请检查当前电脑的系统健康状况。", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent, err := testAgent(liveTitleOnlyGateway{Gateway: gateway, t: t}, WithAgentThreadTitleMode(ThreadTitleModeProvider))
			if err != nil {
				t.Fatal(err)
			}
			host, err := Open(t.Context(), Options{Storage: storage.Memory()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
			if err != nil {
				t.Fatal(err)
			}
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: test.input}, RequestKey: "send"}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(90 * time.Second)
			for time.Now().Before(deadline) {
				summaries, err := service.List(t.Context(), ThreadScope{})
				if err != nil {
					t.Fatal(err)
				}
				if len(summaries) == 1 && summaries[0].TitleGeneration >= 2 {
					summary := summaries[0]
					if summary.TitleStatus == ThreadTitleStatusFailed {
						t.Fatal("live title generation failed")
					}
					if summary.TitleStatus == ThreadTitleStatusReady {
						t.Logf("model=%s title=%q", model, summary.Title)
						hasHan := strings.ContainsFunc(summary.Title, func(r rune) bool { return unicode.Is(unicode.Han, r) })
						if hasHan != test.han {
							t.Fatalf("title language differs from request: %q", summary.Title)
						}
						return
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("live title did not settle")
		})
	}
}

type liveTitleOnlyGateway struct {
	provider.Gateway
	t *testing.T
}

func (g liveTitleOnlyGateway) Stream(ctx context.Context, request provider.Request) (<-chan provider.Event, error) {
	if request.LogicalRequestID == "thread_title" {
		g.t.Logf("title request: max_output_tokens=%d reasoning=%+v system=%q transcript=%q", request.MaxOutputTokens, request.Reasoning, request.Messages[0].Text, request.Messages[1].Text)
		return g.Gateway.Stream(ctx, request)
	}
	events := make(chan provider.Event, 2)
	events <- provider.Event{Type: provider.EventDelta, Text: "Title qualification only."}
	events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	close(events)
	return events, nil
}
