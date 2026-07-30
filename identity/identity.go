// Package identity defines Floret's durable execution and artifact identities.
package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

const maxLength = 128

var (
	// ErrInvalid reports an identity outside Floret's canonical text format.
	ErrInvalid = errors.New("invalid Floret identity")
	pattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type ThreadID string
type TurnID string
type RunID string
type PromptScopeID string
type TraceID string
type LogicalRequestID string
type ArtifactID string

func validate(kind, raw string) error {
	if len(raw) == 0 || len(raw) > maxLength || !pattern.MatchString(raw) {
		return fmt.Errorf("%w: %s must match %s", ErrInvalid, kind, pattern.String())
	}
	return nil
}

func parse[T ~string](kind, raw string) (T, error) {
	if err := validate(kind, raw); err != nil {
		return "", err
	}
	return T(raw), nil
}

func marshalText[T ~string](kind string, value T) ([]byte, error) {
	if err := validate(kind, string(value)); err != nil {
		return nil, err
	}
	return []byte(value), nil
}

func unmarshalText[T ~string](kind string, target *T, text []byte) error {
	if target == nil {
		return fmt.Errorf("%w: nil %s target", ErrInvalid, kind)
	}
	parsed, err := parse[T](kind, string(text))
	if err != nil {
		return err
	}
	*target = parsed
	return nil
}

func marshalJSON[T ~string](kind string, value T) ([]byte, error) {
	if err := validate(kind, string(value)); err != nil {
		return nil, err
	}
	return json.Marshal(string(value))
}

func unmarshalJSON[T ~string](kind string, target *T, data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: decode %s: %v", ErrInvalid, kind, err)
	}
	return unmarshalText(kind, target, []byte(raw))
}

func ParseThreadID(raw string) (ThreadID, error)     { return parse[ThreadID]("thread ID", raw) }
func (id ThreadID) String() string                   { return string(id) }
func (id ThreadID) MarshalText() ([]byte, error)     { return marshalText("thread ID", id) }
func (id *ThreadID) UnmarshalText(text []byte) error { return unmarshalText("thread ID", id, text) }
func (id ThreadID) MarshalJSON() ([]byte, error)     { return marshalJSON("thread ID", id) }
func (id *ThreadID) UnmarshalJSON(data []byte) error { return unmarshalJSON("thread ID", id, data) }

func ParseTurnID(raw string) (TurnID, error)       { return parse[TurnID]("turn ID", raw) }
func (id TurnID) String() string                   { return string(id) }
func (id TurnID) MarshalText() ([]byte, error)     { return marshalText("turn ID", id) }
func (id *TurnID) UnmarshalText(text []byte) error { return unmarshalText("turn ID", id, text) }
func (id TurnID) MarshalJSON() ([]byte, error)     { return marshalJSON("turn ID", id) }
func (id *TurnID) UnmarshalJSON(data []byte) error { return unmarshalJSON("turn ID", id, data) }

func ParseRunID(raw string) (RunID, error)        { return parse[RunID]("run ID", raw) }
func (id RunID) String() string                   { return string(id) }
func (id RunID) MarshalText() ([]byte, error)     { return marshalText("run ID", id) }
func (id *RunID) UnmarshalText(text []byte) error { return unmarshalText("run ID", id, text) }
func (id RunID) MarshalJSON() ([]byte, error)     { return marshalJSON("run ID", id) }
func (id *RunID) UnmarshalJSON(data []byte) error { return unmarshalJSON("run ID", id, data) }

func ParsePromptScopeID(raw string) (PromptScopeID, error) {
	return parse[PromptScopeID]("prompt scope ID", raw)
}
func (id PromptScopeID) String() string               { return string(id) }
func (id PromptScopeID) MarshalText() ([]byte, error) { return marshalText("prompt scope ID", id) }
func (id *PromptScopeID) UnmarshalText(text []byte) error {
	return unmarshalText("prompt scope ID", id, text)
}
func (id PromptScopeID) MarshalJSON() ([]byte, error) { return marshalJSON("prompt scope ID", id) }
func (id *PromptScopeID) UnmarshalJSON(data []byte) error {
	return unmarshalJSON("prompt scope ID", id, data)
}

func ParseTraceID(raw string) (TraceID, error)      { return parse[TraceID]("trace ID", raw) }
func (id TraceID) String() string                   { return string(id) }
func (id TraceID) MarshalText() ([]byte, error)     { return marshalText("trace ID", id) }
func (id *TraceID) UnmarshalText(text []byte) error { return unmarshalText("trace ID", id, text) }
func (id TraceID) MarshalJSON() ([]byte, error)     { return marshalJSON("trace ID", id) }
func (id *TraceID) UnmarshalJSON(data []byte) error { return unmarshalJSON("trace ID", id, data) }

func ParseLogicalRequestID(raw string) (LogicalRequestID, error) {
	return parse[LogicalRequestID]("logical request ID", raw)
}
func (id LogicalRequestID) String() string { return string(id) }
func (id LogicalRequestID) MarshalText() ([]byte, error) {
	return marshalText("logical request ID", id)
}
func (id *LogicalRequestID) UnmarshalText(text []byte) error {
	return unmarshalText("logical request ID", id, text)
}
func (id LogicalRequestID) MarshalJSON() ([]byte, error) {
	return marshalJSON("logical request ID", id)
}
func (id *LogicalRequestID) UnmarshalJSON(data []byte) error {
	return unmarshalJSON("logical request ID", id, data)
}

func ParseArtifactID(raw string) (ArtifactID, error)   { return parse[ArtifactID]("artifact ID", raw) }
func (id ArtifactID) String() string                   { return string(id) }
func (id ArtifactID) MarshalText() ([]byte, error)     { return marshalText("artifact ID", id) }
func (id *ArtifactID) UnmarshalText(text []byte) error { return unmarshalText("artifact ID", id, text) }
func (id ArtifactID) MarshalJSON() ([]byte, error)     { return marshalJSON("artifact ID", id) }
func (id *ArtifactID) UnmarshalJSON(data []byte) error { return unmarshalJSON("artifact ID", id, data) }
