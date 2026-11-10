package tools

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/identity"
	"strings"
	"unicode"
	"unicode/utf8"
)

// OutputReference identifies one canonical output. It is not a capability:
// hosts must authorize ThreadID afresh before every read or search.
type OutputReference struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Kind     string            `json:"kind"`
	ID       string            `json:"id"`
}

func (r OutputReference) Validate() error {
	if r.Kind != "artifact" && r.Kind != "host_work" {
		return errors.New("invalid output reference kind")
	}
	if _, err := identity.ParseThreadID(r.ThreadID.String()); err != nil {
		return err
	}
	for _, s := range []string{r.ThreadID.String(), r.ID} {
		if s == "" || len(s) > 240 || !utf8.ValidString(s) || strings.IndexFunc(s, unicode.IsControl) >= 0 {
			return errors.New("invalid output reference identity")
		}
	}
	return nil
}

// String returns a versioned locator, or an empty string for invalid identities.
func (r OutputReference) String() string {
	if r.Validate() != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	return "output:v1:" + base64.RawURLEncoding.EncodeToString(raw)
}
func ParseOutputReference(ref string) (OutputReference, error) {
	var r OutputReference
	if len(ref) > 2048 || !strings.HasPrefix(ref, "output:v1:") {
		return r, errors.New("invalid output reference")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(ref, "output:v1:"))
	if err != nil {
		return r, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, err
	}
	if err := r.Validate(); err != nil {
		return r, err
	}
	if r.String() != ref {
		return r, errors.New("noncanonical output reference")
	}
	return r, nil
}
