// Package activitymerge owns field replacement decisions for typed tool facts.
// It operates on presence and bounded discriminators, never activity text.
package activitymerge

type Fact struct {
	NonZero bool
	Value   string
}
type Selection struct {
	Replace         bool
	Fields          map[string]bool
	ResultsProvided bool
}

// Select is shared by complete typed merging and source-only field merging.
// Its keys are the existing ActivityPayload JSON fields, not product UI fields.
func Select(renderer string, right map[string]Fact) Selection {
	out := Selection{Fields: make(map[string]bool, len(right))}
	switch renderer {
	case "structured", "terminal", "file", "patch", "web_fetch", "web_search", "question", "subagent_operation":
	default:
		out.Replace = true
		return out
	}
	for key, fact := range right {
		if fact.NonZero {
			out.Fields[key] = true
		}
	}
	switch renderer {
	case "structured":
		if right["rows_provided"].NonZero {
			out.Fields["rows"] = true
		}
	case "terminal":
		if right["operation"].Value == "read" || right["first_seq"].NonZero || right["last_seq"].NonZero || right["latest_seq"].NonZero {
			for _, key := range []string{"first_seq", "last_seq", "latest_seq", "has_more"} {
				out.Fields[key] = true
			}
		} else {
			delete(out.Fields, "has_more")
		}
	case "file":
		for _, key := range []string{"additions", "deletions", "unified_diff", "diff_unavailable_reason"} {
			out.Fields[key] = right["change_type"].NonZero
		}
	case "web_search":
		if right["results_provided"].NonZero || right["results"].NonZero {
			out.Fields["results"] = true
			out.ResultsProvided = true
		}
	}
	return out
}
