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
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	code := envelope.Error.Code
	if code == "" {
		code = envelope.Error.Type
	}
	return providerretry.WithAdvice(fmt.Errorf("%s provider status %d", name, status), providerretry.HTTPAdvice(status, code, headers, received))
}
