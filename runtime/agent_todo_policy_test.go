package runtime

import (
	"testing"

	"github.com/floegence/floret/v3/internal/sessiontree"
)

func TestPublicAndStorageAgentTodoLimitsStayEqual(t *testing.T) {
	if MaxAgentTodos != sessiontree.MaxAgentTodoItems {
		t.Fatalf("runtime MaxAgentTodos = %d, storage limit = %d", MaxAgentTodos, sessiontree.MaxAgentTodoItems)
	}
}
