package florettest_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

func TestIDSourceOwnsDeterministicIdentityInjection(t *testing.T) {
	ctx := context.Background()
	ids := florettest.NewIDSource(florettest.IDSourceOptions{
		ThreadIDs: []identity.ThreadID{"thread-test"},
	})
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory(), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-test"})
	if err != nil {
		t.Fatal(err)
	}
	if created.ThreadID != "thread-test" {
		t.Fatalf("thread id = %q", created.ThreadID)
	}
}
