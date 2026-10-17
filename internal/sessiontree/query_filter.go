package sessiontree

import (
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/threadquery"
	"strings"
	"time"
	"unicode/utf8"
)

func validateQuery(object threadquery.Object, filter threadquery.Filter, ordering *threadquery.Sort, limit *int) error {
	keys := map[threadquery.Object][]string{threadquery.Threads: {"created", "updated", "title", "thread_id"}, threadquery.Messages: {"time", "order"}, threadquery.ToolCalls: {"time", "completed", "tool_name", "elapsed"}}
	allowed, ok := keys[object]
	if !ok {
		return fmt.Errorf("%w: unsupported object", threadquery.ErrInvalidQuery)
	}
	if ordering.Key == "" {
		ordering.Key = allowed[0]
	}
	if !containsString(allowed, ordering.Key) {
		return fmt.Errorf("%w: unsupported sort key", threadquery.ErrInvalidQuery)
	}
	if *limit == 0 {
		*limit = 50
	}
	if *limit < 1 || *limit > 200 {
		return fmt.Errorf("%w: limit must be 1..200", threadquery.ErrInvalidQuery)
	}
	for _, match := range []threadquery.TextMatch{filter.Text, filter.Title, filter.Arguments, filter.Result} {
		if !utf8.ValidString(match.Contains) || !utf8.ValidString(match.Phrase) || len(match.Contains)+len(match.Phrase) > 16384 || len(match.Keywords) > 64 {
			return threadquery.ErrInvalidQuery
		}
		for _, term := range match.Keywords {
			if term == "" || !utf8.ValidString(term) || len(term) > 16384 {
				return threadquery.ErrInvalidQuery
			}
		}
	}
	for _, span := range []threadquery.TimeRange{filter.Time, filter.Created, filter.Updated, filter.Completed} {
		if !span.From.IsZero() && !span.To.IsZero() && !span.From.Before(span.To) {
			return fmt.Errorf("%w: invalid half-open time range", threadquery.ErrInvalidQuery)
		}
	}
	if object != threadquery.Threads && (len(filter.ParentThreadIDs) > 0 || len(filter.Activities) > 0 || len(filter.Outcomes) > 0 || !emptyText(filter.Title) || !emptyTime(filter.Created) || !emptyTime(filter.Updated)) {
		return fmt.Errorf("%w: thread-only filter", threadquery.ErrInvalidQuery)
	}
	if object != threadquery.Messages && len(filter.Roles) > 0 {
		return fmt.Errorf("%w: message-only role filter", threadquery.ErrInvalidQuery)
	}
	if object != threadquery.ToolCalls && (len(filter.ToolNames) > 0 || len(filter.ToolKinds) > 0 || !emptyText(filter.Arguments) || !emptyText(filter.Result) || !emptyTime(filter.Completed)) {
		return fmt.Errorf("%w: tool-only filter", threadquery.ErrInvalidQuery)
	}
	if object == threadquery.Threads && (len(filter.TurnIDs) > 0 || len(filter.RunIDs) > 0) {
		return threadquery.ErrInvalidQuery
	}
	for _, id := range filter.ThreadIDs {
		if _, err := identity.ParseThreadID(id.String()); err != nil {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, id := range filter.ParentThreadIDs {
		if _, err := identity.ParseThreadID(id.String()); err != nil {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, id := range filter.TurnIDs {
		if _, err := identity.ParseTurnID(id.String()); err != nil {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, id := range filter.RunIDs {
		if _, err := identity.ParseRunID(id.String()); err != nil {
			return threadquery.ErrInvalidQuery
		}
	}
	if object == threadquery.Messages && len(filter.Statuses) > 0 {
		return threadquery.ErrInvalidQuery
	}
	for _, activity := range filter.Activities {
		if activity != "idle" && activity != "active" {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, outcome := range filter.Outcomes {
		if !containsString([]string{"completed", "failed", "interrupted", "cancelled"}, outcome) {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, values := range [][]string{filter.ToolNames, filter.ToolKinds, filter.Statuses} {
		for _, value := range values {
			if value == "" || value != strings.TrimSpace(value) || !utf8.ValidString(value) {
				return threadquery.ErrInvalidQuery
			}
		}
	}
	for _, role := range filter.Roles {
		if role != "user" && role != "assistant" {
			return threadquery.ErrInvalidQuery
		}
	}
	for _, kind := range filter.ToolKinds {
		if kind != "local" && kind != "hosted" {
			return threadquery.ErrInvalidQuery
		}
	}
	return nil
}
func emptyText(match threadquery.TextMatch) bool {
	return match.Contains == "" && match.Phrase == "" && len(match.Keywords) == 0
}
func emptyTime(span threadquery.TimeRange) bool { return span.From.IsZero() && span.To.IsZero() }
func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}
func queryTextMatches(text string, match threadquery.TextMatch) bool {
	normalize := func(value string) string {
		if match.CaseSensitive {
			return value
		}
		return strings.ToLower(value)
	}
	text = normalize(text)
	if match.Contains != "" && !strings.Contains(text, normalize(match.Contains)) {
		return false
	}
	if match.Phrase != "" && !strings.Contains(text, normalize(match.Phrase)) {
		return false
	}
	any := false
	for _, term := range match.Keywords {
		hit := strings.Contains(text, normalize(term))
		if !hit && !match.AnyKeyword {
			return false
		}
		any = any || hit
	}
	return !match.AnyKeyword || len(match.Keywords) == 0 || any
}
func queryRecordLess(a, b *queryRecord, ordering threadquery.Sort) bool {
	compare := 0
	compareTime := func(x, y time.Time) int {
		if x.Before(y) {
			return -1
		}
		if x.After(y) {
			return 1
		}
		return 0
	}
	switch ordering.Key {
	case "created":
		compare = compareTime(a.row.CreatedAt, b.row.CreatedAt)
	case "updated":
		compare = compareTime(a.row.UpdatedAt, b.row.UpdatedAt)
	case "time":
		compare = compareTime(a.row.Time, b.row.Time)
	case "completed":
		compare = compareTime(a.row.CompletedAt, b.row.CompletedAt)
	case "title":
		compare = strings.Compare(a.row.Title, b.row.Title)
	case "thread_id":
		compare = strings.Compare(a.row.ThreadID.String(), b.row.ThreadID.String())
	case "tool_name":
		compare = strings.Compare(a.row.ToolName, b.row.ToolName)
	case "order":
		if a.row.Order < b.row.Order {
			compare = -1
		} else if a.row.Order > b.row.Order {
			compare = 1
		}
	case "elapsed":
		if a.row.Elapsed < b.row.Elapsed {
			compare = -1
		} else if a.row.Elapsed > b.row.Elapsed {
			compare = 1
		}
	}
	if compare == 0 {
		compare = strings.Compare(a.key, b.key)
	}
	if ordering.Descending {
		return compare > 0
	}
	return compare < 0
}
