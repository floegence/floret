package sessiontree

import (
	"fmt"
	"testing"
)

func TestValidateAgentTodoItemsOwnsCanonicalInvariants(t *testing.T) {
	items := make([]AgentTodoItem, MaxAgentTodoItems+1)
	for index := range items {
		items[index] = AgentTodoItem{ID: fmt.Sprintf("todo-%d", index), Content: "work", Status: AgentTodoPending}
	}
	if err := ValidateAgentTodoItems(items); err == nil {
		t.Fatalf("accepted %d todo items", len(items))
	}
	if err := ValidateAgentTodoItems([]AgentTodoItem{
		{ID: "first", Content: "first", Status: AgentTodoInProgress},
		{ID: "second", Content: "second", Status: AgentTodoInProgress},
	}); err == nil {
		t.Fatal("accepted two in-progress items")
	}
}
