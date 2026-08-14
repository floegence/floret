// Package activityview contains internal operations over the sealed public
// activity variants. It keeps durable and runtime projections on one shape.
package activityview

import (
	"strings"

	"github.com/floegence/floret/v4/tools"
)

// WithTerminalStatus returns a detached presentation with a terminal status.
// Error details are represented by the renderer's single typed payload shape.
func WithTerminalStatus(in *tools.ActivityPresentation, status, reason string) *tools.ActivityPresentation {
	out := tools.CloneActivityPresentation(in)
	if out == nil {
		out = &tools.ActivityPresentation{}
	}
	status = strings.TrimSpace(status)
	reason = strings.TrimSpace(reason)
	activityError := func(existing *tools.ActivityError) *tools.ActivityError {
		if existing != nil && strings.TrimSpace(existing.Message) != "" {
			copy := *existing
			return &copy
		}
		if reason == "" {
			return nil
		}
		return &tools.ActivityError{Message: reason}
	}
	switch payload := out.Payload.(type) {
	case tools.StructuredActivityPayload:
		payload.Status = status
		payload.Error = activityError(payload.Error)
		out.Renderer = tools.ActivityRendererStructured
		out.Payload = payload
	case tools.TerminalActivityPayload:
		payload.Status = status
		payload.Error = activityError(payload.Error)
		out.Renderer = tools.ActivityRendererTerminal
		out.Payload = payload
	case tools.FileActivityPayload:
		payload.Status = status
		payload.Error = activityError(payload.Error)
		out.Renderer = tools.ActivityRendererFile
		out.Payload = payload
	case tools.PatchActivityPayload:
		payload.Status = status
		payload.Error = activityError(payload.Error)
		out.Renderer = tools.ActivityRendererPatch
		out.Payload = payload
	case tools.WebSearchActivityPayload:
		payload.Status = status
		payload.Error = activityError(payload.Error)
		out.Renderer = tools.ActivityRendererWebSearch
		out.Payload = payload
	case tools.CompletionActivityPayload:
		payload.Status = status
		if payload.Summary == "" {
			payload.Summary = reason
		}
		out.Renderer = tools.ActivityRendererCompletion
		out.Payload = payload
	default:
		out.Renderer = tools.ActivityRendererStructured
		out.Payload = tools.StructuredActivityPayload{
			Status: status,
			Error:  activityError(nil),
		}
	}
	return out
}

// PendingHandle returns the exact handle chip authored by PendingToolActivity.
func PendingHandle(in *tools.ActivityPresentation) string {
	if in == nil {
		return ""
	}
	for _, chip := range in.Chips {
		if chip.Kind == "handle" {
			return strings.TrimSpace(chip.Value)
		}
	}
	return ""
}
