package agentharness

import (
	"time"

	"github.com/floegence/floret/v3/identity"
)

// unifiedThread is the internal Phase 1 domain root. It is deliberately
// separate from the published runtime projection until the v4 boundary.
type unifiedThread struct {
	ID        identity.ThreadID
	ParentID  identity.ThreadID
	Title     string
	CreatedAt time.Time
}

type unifiedTurn struct {
	ID               identity.TurnID
	ThreadID         identity.ThreadID
	RunID            identity.RunID
	LogicalRequestID identity.LogicalRequestID
	Status           string
}

type unifiedPendingInteraction struct {
	ID       string
	ThreadID identity.ThreadID
	TurnID   identity.TurnID
	Kind     string
	Payload  []byte
}

type unifiedAttachmentRef struct {
	ID       string
	Name     string
	MIMEType string
	Size     int64
	Digest   string
}
