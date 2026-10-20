// Package providertransport defines non-semantic provider transport observations.
package providertransport

import (
	"fmt"
	"regexp"
)

type Kind string

const (
	HTTP      Kind = "http"
	HTTPSSE   Kind = "http_sse"
	WebSocket Kind = "websocket"
)

func (k Kind) Valid() bool { return k == HTTP || k == HTTPSSE || k == WebSocket }

type Phase string

const (
	Connecting Phase = "connecting"
	Sending    Phase = "sending"
	Receiving  Phase = "receiving"
	Completed  Phase = "completed"
	Failed     Phase = "failed"
)

func (p Phase) Valid() bool {
	return p == Connecting || p == Sending || p == Receiving || p == Completed || p == Failed
}

// Observation contains protocol facts only. Reason is a bounded machine code,
// never a URL, credential, response body, or unsanitized peer close reason.
type Observation struct {
	Transport     Kind   `json:"transport"`
	Phase         Phase  `json:"phase"`
	NextTransport Kind   `json:"next_transport,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

var machineReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func (o Observation) Validate() error {
	if !o.Transport.Valid() || !o.Phase.Valid() || o.NextTransport != "" && !o.NextTransport.Valid() || o.Reason != "" && !machineReason.MatchString(o.Reason) {
		return fmt.Errorf("invalid provider transport observation")
	}
	return nil
}
func Clone(o *Observation) *Observation {
	if o == nil {
		return nil
	}
	copy := *o
	return &copy
}
