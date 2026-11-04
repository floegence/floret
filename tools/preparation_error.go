package tools

import "errors"

// PreparationError carries host-sanitized facts from resource extraction before
// authorization or handler execution. It never authorizes retry or dispatch.
// Code and Message must be suitable for model output and Activity presentation.
type PreparationError struct {
	Code    string
	Message string
	Cause   error
}

func (e *PreparationError) Error() string { return e.Message }
func (e *PreparationError) Unwrap() error { return e.Cause }

func resourcePreparationFailure(call ToolCall, def Definition, inv erasedInvocation, err error) Result {
	var failure *PreparationError
	if !errors.As(err, &failure) || failure == nil {
		return ErrorResult(call.ID, call.Name, "tool \""+call.Name+"\" resource extraction failed: "+err.Error())
	}
	detail := &ActivityError{Code: failure.Code, Message: failure.Message}
	if detail.Code == "" || validatePayloadTextAndError(nil, detail) != nil {
		return ErrorResult(call.ID, call.Name, "tool preparation returned invalid error facts")
	}
	result := ErrorResult(call.ID, call.Name, failure.Message)
	result.Structured = map[string]any{
		"status": "error", "error": map[string]any{"code": failure.Code, "message": failure.Message},
		"execution": map[string]any{"phase": "resource_extraction", "started": false},
	}
	result.Activity = CloneActivityPresentation(preparedDispatchActivity(def, inv))
	if result.Activity == nil {
		result.Activity = &ActivityPresentation{Renderer: ActivityRendererStructured}
	}
	payload, ok := result.Activity.Payload.(StructuredActivityPayload)
	if !ok {
		payload = StructuredActivityPayload{Operation: "prepare"}
	}
	payload.Status, payload.Error = "error", detail
	result.Activity.Renderer, result.Activity.Payload = ActivityRendererStructured, payload
	return result
}
