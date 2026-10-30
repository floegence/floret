package adapters

import (
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/internal/providerretry"
	"net/http"
	"time"
)

func protocolFailure(name string, status int, headers http.Header, body []byte, received time.Time) error {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	code := envelope.Error.Code
	if code == "" {
		code = envelope.Error.Type
	}
	return providerretry.WithAdvice(&protocolHTTPError{name, status, code, envelope.Error.Message}, providerretry.HTTPAdvice(status, code, headers, received))
}

type protocolHTTPError struct {
	name          string
	status        int
	code, message string
}

func (e *protocolHTTPError) Error() string {
	return fmt.Sprintf("%s provider status %d", e.name, e.status)
}
func (e *protocolHTTPError) ProviderHTTPDetails() (string, int, string, string) {
	return e.name, e.status, e.code, e.message
}
