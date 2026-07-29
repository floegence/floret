package runtime_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/floegence/floret/v2/runtime"
)

func TestRecoveryHandlesBindAllAuthorityIdentityAtIssuance(t *testing.T) {
	assertNoField := func(name string, value any, fields ...string) {
		t.Helper()
		typ := reflect.TypeOf(value)
		for _, field := range fields {
			if _, ok := typ.FieldByName(field); ok {
				t.Fatalf("%s repeats bound identity field %s", name, field)
			}
		}
	}

	assertNoField("PendingToolRecoveryRequest", runtime.PendingToolRecoveryRequest{},
		"ThreadID", "ParentThreadID", "Target", "TurnID", "RunID", "ToolCallID")
	var host *runtime.Host
	var pendingTarget runtime.PendingToolRecoveryTarget
	var interruptedTarget runtime.InterruptedTurnRecoveryTarget
	var sink runtime.EventSink

	pending, _ := host.PendingToolRecovery(context.Background(), pendingTarget, sink)
	_, _ = pending.Settle(context.Background(), runtime.PendingToolRecoveryRequest{})
	interrupted, _ := host.InterruptedTurnRecovery(context.Background(), interruptedTarget, sink)
	_, _ = interrupted.Recover(context.Background())
}

func TestThreadInventoryIsIssuedOnlyByHost(t *testing.T) {
	var host *runtime.Host
	inventory, _ := host.ThreadInventory(context.Background())
	_, _ = inventory.List(context.Background(), runtime.ListRootThreadsRequest{})
}
