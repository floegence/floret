package runtime_test

import (
	"fmt"
	"testing"

	"github.com/floegence/floret/v3/runtime"
)

func TestValidateAgentTodosOwnsCanonicalTodoInvariants(t *testing.T) {
	items := make([]runtime.AgentTodo, runtime.MaxAgentTodos+1)
	for index := range items {
		items[index] = runtime.AgentTodo{ID: fmt.Sprintf("todo-%d", index), Content: "work", Status: runtime.AgentTodoPending}
	}
	if err := runtime.ValidateAgentTodos(items); err == nil {
		t.Fatalf("accepted %d todos", len(items))
	}
	if err := runtime.ValidateAgentTodos([]runtime.AgentTodo{
		{ID: "first", Content: "first", Status: runtime.AgentTodoInProgress},
		{ID: "second", Content: "second", Status: runtime.AgentTodoInProgress},
	}); err == nil {
		t.Fatal("accepted more than one in-progress todo")
	}
	if err := runtime.ValidateAgentTodos([]runtime.AgentTodo{
		{ID: "first", Content: "first", Status: runtime.AgentTodoInProgress},
		{ID: "second", Content: "second", Status: runtime.AgentTodoPending},
	}); err != nil {
		t.Fatal(err)
	}
}
