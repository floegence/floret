package tools

type Invocation[T any] struct {
	CallID    string
	Name      string
	RawArgs   string
	Args      T
	RunID     string
	SessionID string
	Step      int
	CWD       string
}

type Result struct {
	CallID     string
	Name       string
	Title      string
	Text       string
	Structured map[string]any
	Metadata   map[string]any
	Artifacts  []ArtifactRef
	IsError    bool
}

type ArtifactRef struct {
	Kind string
	Path string
	MIME string
}

func ErrorResult(callID, name, text string) Result {
	return Result{CallID: callID, Name: name, Text: text, IsError: true}
}

func (r Result) withCall(callID, name string) Result {
	if r.CallID == "" {
		r.CallID = callID
	}
	if r.Name == "" {
		r.Name = name
	}
	return r
}
