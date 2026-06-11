package agentharness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

const (
	defaultThreadTitleMaxRunes        = 16
	defaultThreadTitleMaxOutputTokens = 64
	threadTitlePromptMaxMessages      = 8
	threadTitlePromptMaxContentRunes  = 600
)

const ThreadTitleLogicalRequestID = "thread_title"

type TitleGenerator interface {
	GenerateTitle(context.Context, TitleRequest) (TitleResult, error)
}

type TitleRequest struct {
	ThreadID string
	TurnID   string
	Messages []session.Message
}

type TitleResult struct {
	Title  string
	Source sessiontree.ThreadTitleSource
}

type ProviderTitleGenerator struct {
	Provider        provider.Provider
	ProviderName    string
	Model           string
	MaxRunes        int
	MaxOutputTokens int64
}

func (g ProviderTitleGenerator) GenerateTitle(ctx context.Context, req TitleRequest) (TitleResult, error) {
	if g.Provider == nil {
		return TitleResult{}, errors.New("thread title generator requires provider")
	}
	prompt, err := threadTitlePromptMessages(req.Messages)
	if err != nil {
		return TitleResult{}, err
	}
	stream, err := g.Provider.Stream(ctx, provider.Request{
		RunID:            threadTitleRunID(req.TurnID),
		ThreadID:         strings.TrimSpace(req.ThreadID),
		TurnID:           strings.TrimSpace(req.TurnID),
		PromptScopeID:    strings.TrimSpace(req.ThreadID),
		TraceID:          threadTitleRunID(req.TurnID),
		LogicalRequestID: ThreadTitleLogicalRequestID,
		Provider:         strings.TrimSpace(g.ProviderName),
		Model:            strings.TrimSpace(g.Model),
		Messages:         prompt,
		MaxOutputTokens:  g.maxOutputTokens(),
		DisableReasoning: true,
	})
	if err != nil {
		return TitleResult{}, err
	}
	var text strings.Builder
	for ev := range stream {
		switch ev.Type {
		case provider.Delta:
			text.WriteString(ev.Text)
		case provider.Reasoning, provider.UsageEvent:
			continue
		case provider.Done:
			return g.titleResult(text.String())
		case provider.Truncated:
			return TitleResult{}, errors.New("provider truncated thread title")
		case provider.Empty:
			return TitleResult{}, errors.New("provider returned empty thread title")
		case provider.Error:
			if ev.Err != nil {
				return TitleResult{}, ev.Err
			}
			return TitleResult{}, errors.New("provider failed to generate thread title")
		case provider.ToolCalls, provider.HostedToolCall, provider.HostedToolResult:
			return TitleResult{}, fmt.Errorf("provider returned unsupported %q event while generating thread title", ev.Type)
		default:
			return TitleResult{}, fmt.Errorf("provider returned unknown %q event while generating thread title", ev.Type)
		}
	}
	return g.titleResult(text.String())
}

func (g ProviderTitleGenerator) titleResult(raw string) (TitleResult, error) {
	title := normalizeThreadTitle(raw, g.maxRunes())
	if title == "" {
		return TitleResult{}, errors.New("provider returned empty thread title")
	}
	return TitleResult{Title: title, Source: sessiontree.ThreadTitleSourceProvider}, nil
}

func (g ProviderTitleGenerator) maxRunes() int {
	if g.MaxRunes > 0 {
		return g.MaxRunes
	}
	return defaultThreadTitleMaxRunes
}

func (g ProviderTitleGenerator) maxOutputTokens() int64 {
	if g.MaxOutputTokens > 0 {
		return g.MaxOutputTokens
	}
	return defaultThreadTitleMaxOutputTokens
}

func threadTitleRunID(turnID string) string {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return "thread-title"
	}
	return turnID + ":thread-title"
}

func threadTitlePromptMessages(messages []session.Message) ([]session.Message, error) {
	lines := make([]string, 0, threadTitlePromptMaxMessages)
	for _, msg := range messages {
		if len(lines) >= threadTitlePromptMaxMessages {
			break
		}
		if msg.Kind != "" || msg.ToolCallID != "" || msg.ToolName != "" {
			continue
		}
		content := cleanTitleContent(msg.Content)
		if content == "" {
			continue
		}
		content = truncateTitlePromptContent(content, threadTitlePromptMaxContentRunes)
		switch msg.Role {
		case session.User:
			lines = append(lines, "User: "+content)
		case session.Assistant:
			lines = append(lines, "Assistant: "+content)
		}
	}
	if len(lines) == 0 {
		return nil, errors.New("thread title requires user or assistant content")
	}
	system := strings.Join([]string{
		"You generate concise thread titles for an interactive AI agent.",
		"Return only one plain-text title.",
		"Do not reason; return only the title.",
		"The title must summarize the user's primary intent, not quote the raw transcript.",
		"Keep the title specific, single-line, and no more than 16 Unicode characters.",
		"Use the same language as the user's request when the request is not in English.",
		"Do not mention chat, thread, assistant, or tools unless central to the request.",
		"Do not include secrets, credentials, private values, markdown, or extra commentary.",
	}, "\n")
	user := "Transcript:\n" + strings.Join(lines, "\n")
	return []session.Message{
		{Role: session.System, Content: system},
		{Role: session.User, Content: user},
	}, nil
}

func cleanTitleContent(text string) string {
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "` \t\r\n")
	text = strings.ReplaceAll(text, "\u0000", " ")
	return collapseWhitespace(text)
}

func truncateTitlePromptContent(text string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	runes := []rune(text)
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return strings.TrimSpace(string(runes[:maxRunes-3])) + "..."
}

func normalizeThreadTitle(text string, maxRunes int) string {
	text = collapseWhitespace(strings.TrimSpace(text))
	text = trimTitleEdges(text)
	if text == "" {
		return ""
	}
	if maxRunes <= 0 {
		maxRunes = defaultThreadTitleMaxRunes
	}
	if utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	runes := []rune(text)
	cut := maxRunes
	for cut > maxRunes/2 && cut < len(runes) && !unicode.IsSpace(runes[cut]) {
		cut--
	}
	if cut <= 0 {
		cut = maxRunes
	}
	return trimTitleEdges(string(runes[:cut]))
}

func collapseWhitespace(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func trimTitleEdges(text string) string {
	return strings.TrimFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
	})
}
