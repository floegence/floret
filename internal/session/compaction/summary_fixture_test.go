package compaction

import (
	"context"
	"github.com/floegence/floret/v7/internal/session"
	"strings"
)

type extractiveSummaryFixture struct {
	PromptOptions PromptOptions
}

func (g extractiveSummaryFixture) GenerateSummary(_ context.Context, prep Preparation) (string, error) {
	options := NormalizePromptOptions(g.PromptOptions)
	var out strings.Builder
	out.WriteString("# " + options.SummaryTitle + "\n")
	out.WriteString("schema: " + SummarySchemaVersion + "\n\n")
	out.WriteString("## Goals\n")
	writeRoleSamples(&out, prep.CompactedHead, session.User, "- ")
	if prep.Request.PreviousSummary != "" {
		out.WriteString("\n## Previous Summary\n")
		out.WriteString(strings.TrimSpace(prep.Request.PreviousSummary))
		out.WriteString("\n")
	}
	out.WriteString("\n## Completed Work And Decisions\n")
	writeRoleSamples(&out, prep.CompactedHead, session.Assistant, "- ")
	out.WriteString("\n## Tool Results, Commands, And Errors\n")
	writeToolSamples(&out, prep.CompactedHead, "- ")
	out.WriteString("\n## Open Items\n")
	out.WriteString("- Continue from the retained tail without re-reading compacted transcript unless needed.\n")
	out.WriteString("- Preserve user constraints, file paths, command outcomes, errors, and unresolved intent from this summary.\n")
	return out.String(), nil
}

func writeRoleSamples(out *strings.Builder, messages []session.Message, role session.Role, prefix string) {
	wrote := 0
	for _, msg := range messages {
		if msg.Role != role || strings.TrimSpace(msg.Content) == "" {
			continue
		}
		out.WriteString(prefix)
		out.WriteString(trimForSummary(msg.Content, 360))
		out.WriteString("\n")
		wrote++
		if wrote >= 8 {
			break
		}
	}
	if wrote == 0 {
		out.WriteString(prefix)
		out.WriteString("No explicit items captured in this section.\n")
	}
}

func writeToolSamples(out *strings.Builder, messages []session.Message, prefix string) {
	wrote := 0
	for _, msg := range messages {
		if msg.Role != session.Tool {
			continue
		}
		out.WriteString(prefix)
		out.WriteString(msg.ToolName)
		if msg.ToolCallID != "" {
			out.WriteString(" ")
			out.WriteString(msg.ToolCallID)
		}
		out.WriteString(": ")
		out.WriteString(trimForSummary(msg.Content, 360))
		out.WriteString("\n")
		wrote++
		if wrote >= 8 {
			break
		}
	}
	if wrote == 0 {
		out.WriteString(prefix)
		out.WriteString("No tool results were compacted.\n")
	}
}

func trimForSummary(value string, max int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
