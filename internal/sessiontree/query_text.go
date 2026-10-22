package sessiontree

import (
	"encoding/json"
	"strings"
)

// visitText supplies the exact public search field as borrowed source segments.
// The separator rule is shared with explicit full-field materialization.
func (record *queryRecord) visitText(field string, visit func(string)) {
	bytes := 0
	for _, part := range record.parts {
		if field != "text" && field != part.field {
			continue
		}
		if bytes > 0 && part.field != "body" {
			visit("\n")
			bytes++
		}
		visit(part.text)
		bytes += len(part.text)
	}
	if len(record.activities) > 0 && (field == "text" || field == "presentation") {
		raw, _ := json.Marshal(record.activities)
		if bytes > 0 {
			visit("\n")
		}
		visit(string(raw))
	}
}

func (record *queryRecord) text(field string) string {
	var result strings.Builder
	record.visitText(field, func(text string) { result.WriteString(text) })
	return result.String()
}
