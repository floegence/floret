package runtime

import (
	"github.com/floegence/floret/v7/identity"
	"reflect"
	"testing"
)

func TestThreadSummaryReaderDoesNotCreateActor(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	_ = host

	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "summary"})
	if err != nil {
		t.Fatal(err)
	}
	before := len(service.(*threadRuntimeService).runtimes)
	summary, err := service.(ThreadSummaryReader).Summary(t.Context(), identity.ThreadID(created.ThreadID))
	if err != nil {
		t.Fatal(err)
	}
	if summary.ID != created.ThreadID || len(service.(*threadRuntimeService).runtimes) != before {
		t.Fatal("summary changed actor inventory")
	}
	listed, err := service.List(t.Context(), ThreadScope{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0], summary) {
		t.Fatalf("summary differs from list: %+v %+v", summary, listed)
	}
}
