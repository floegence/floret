package provider

import (
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/internal/session"
)

func TestMessagesWithEphemeralUserRejectsInvalidIdentityAndIndex(t *testing.T) {
	messages := []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "user"}}
	tests := map[string]*EphemeralUserMessage{
		"durable identity": {Message: session.Message{Role: session.User, Content: "private", EntryID: "forbidden"}, HistoryInsertAt: 1},
		"invalid index":    {Message: session.Message{Role: session.User, Content: "private"}, HistoryInsertAt: 2},
		"wrong role":       {Message: session.Message{Role: session.Assistant, Content: "private"}, HistoryInsertAt: 1},
	}
	for name, ephemeral := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := MessagesWithEphemeralUser(messages, ephemeral); err == nil {
				t.Fatal("invalid ephemeral message succeeded")
			}
		})
	}
}

func TestGenericRequestEstimateIncludesEphemeralUser(t *testing.T) {
	req := Request{
		Messages: []session.Message{
			{Role: session.System, Content: "system"},
			{Role: session.User, Content: "durable"},
		},
		EphemeralUser: &EphemeralUserMessage{
			Message:         session.Message{Role: session.User, Content: strings.Repeat("private context ", 512)},
			HistoryInsertAt: 1,
		},
	}
	merged, err := MessagesWithEphemeralUser(req.Messages, req.EphemeralUser)
	if err != nil {
		t.Fatal(err)
	}
	expectedRequest := req
	expectedRequest.Messages = merged
	expectedRequest.EphemeralUser = nil
	expected, err := GenericRequestEstimate(expectedRequest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := GenericRequestEstimate(req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("estimate with ephemeral=%#v, want merged payload estimate %#v", got, expected)
	}
}
