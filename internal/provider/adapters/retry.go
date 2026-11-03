package adapters

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/providerretry"
	"net/http"
	"time"
)

func protocolFailure(name string, status int, headers http.Header, body []byte, received time.Time, readErrors ...error) error {
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
	readErr := errors.Join(readErrors...)
	failure := &protocolHTTPError{name: name, status: status, code: code, message: envelope.Error.Message, body: bytes.Clone(body), requestID: headers.Get("x-request-id"), complete: readErr == nil}
	return providerretry.WithAdvice(errors.Join(failure, readErr), providerretry.HTTPAdvice(status, code, headers, received))
}

type protocolHTTPError struct {
	name          string
	status        int
	code, message string
	body          []byte
	requestID     string
	complete      bool
}

func (e *protocolHTTPError) Error() string {
	return fmt.Sprintf("%s provider status %d", e.name, e.status)
}
func (e *protocolHTTPError) ProviderHTTPDetails() (string, int, string, string) {
	return e.name, e.status, e.code, e.message
}

// Original response bytes remain separate from safe summary text.
func (e *protocolHTTPError) ProviderFailureResponse() ([]byte, string, bool) {
	return bytes.Clone(e.body), e.requestID, e.complete
}
