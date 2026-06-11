package tools

import "github.com/floegence/floret/session/artifact"

type Invocation[T any] struct {
	CallID      string
	Name        string
	RawArgs     string
	Args        T
	RunID       string
	SessionID   string
	Step        int
	CWD         string
	Labels      map[string]string
	HostContext map[string]string
}

type Result struct {
	CallID       string
	Name         string
	Title        string
	Text         string
	Structured   map[string]any
	Metadata     map[string]any
	Artifacts    []artifact.Ref
	OutputPolicy *OutputPolicy
	IsError      bool
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
