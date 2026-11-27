package provider

import "context"

// PreparedInputCounter optionally counts the complete frozen input at its
// selected provider without generating output or consuming the handle. Return
// false on incomplete coverage, unavailability or timeout. Implementations must
// honor cancellation. A count is not native usage or a calibration anchor.
type PreparedInputCounter interface {
	CountPreparedInput(context.Context) (tokens int64, available bool)
}
