package provider

import "github.com/floegence/floret/v7/internal/providertransport"

// TransportKind identifies the actual provider request protocol.
type TransportKind = providertransport.Kind

const (
	TransportHTTP      = providertransport.HTTP
	TransportHTTPSSE   = providertransport.HTTPSSE
	TransportWebSocket = providertransport.WebSocket
)

// TransportPhase describes protocol progress without semantic model output.
type TransportPhase = providertransport.Phase

const (
	TransportConnecting = providertransport.Connecting
	TransportSending    = providertransport.Sending
	TransportReceiving  = providertransport.Receiving
	TransportCompleted  = providertransport.Completed
	TransportFailed     = providertransport.Failed
)

// TransportObservation is optional non-semantic feedback. It never renews output
// timeouts, admits tools, contributes tokens, or authorizes another attempt.
type TransportObservation = providertransport.Observation
