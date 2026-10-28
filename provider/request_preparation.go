package provider

// RequestPreparationError reports rejection of temporary transport references
// before model execution. A new logical preparation may resolve original input
// again; transport retries must continue replaying their frozen payload.
type RequestPreparationError struct{ Cause error }

func (e *RequestPreparationError) Error() string {
	return "temporary image reference expired or unavailable; prepare the request again: " + e.Cause.Error()
}
func (e *RequestPreparationError) Unwrap() error                  { return e.Cause }
func (e *RequestPreparationError) RequestReprepareRequired() bool { return true }
