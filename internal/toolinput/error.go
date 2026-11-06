// Package toolinput identifies input-decoder failures across the tool and engine boundary.
package toolinput

// DecodeError preserves the decoder's cause without classifying projector,
// resource, permission, storage, or handler failures as correctable input.
type DecodeError struct {
	Cause error
}

func (err *DecodeError) Error() string { return err.Cause.Error() }
func (err *DecodeError) Unwrap() error { return err.Cause }
