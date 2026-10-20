package providertransport

import "testing"

func TestObservationRejectsUnstructuredDiagnosticData(t *testing.T) {
	for _, reason := range []string{"", "connection_lost", "connect_timeout"} {
		if err := (Observation{Transport: WebSocket, Phase: Failed, NextTransport: HTTPSSE, Reason: reason}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, reason := range []string{"Bearer secret", "https://example.com", "raw close reason", "UPPER"} {
		if err := (Observation{Transport: WebSocket, Phase: Failed, Reason: reason}).Validate(); err == nil {
			t.Fatalf("accepted %q", reason)
		}
	}
	for _, o := range []Observation{{}, {Transport: "other", Phase: Connecting}, {Transport: HTTP, Phase: "other"}, {Transport: HTTP, Phase: Failed, NextTransport: "other"}} {
		if o.Validate() == nil {
			t.Fatalf("accepted %+v", o)
		}
	}
}
