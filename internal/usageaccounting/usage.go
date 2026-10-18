// Package usageaccounting owns provider usage arithmetic, without protocol inference.
package usageaccounting

// CombinedInputScope never upgrades mixed or missing protocol evidence.
func CombinedInputScope(a, b string) string {
	if a == b {
		return a
	}
	return "unknown"
}

func WindowInput(reported, input, read, write int64) int64 {
	if reported > 0 {
		return reported
	}
	return input + read + write
}

func Total(reported int64, present bool, input, output, reasoning, read, write int64, inclusive bool) int64 {
	if present || reported != 0 {
		return reported
	}
	if inclusive {
		reasoning = 0
	}
	return input + read + write + output + reasoning
}

// HasInput preserves historical native measurements while explicit protocol
// declarations distinguish output-only usage from a measured input zero.
func HasInput(reported bool, scope string, available bool, input, read, write, window, output, reasoning int64) bool {
	if reported {
		return true
	}
	if scope != "" {
		return false
	}
	return available && (input > 0 || read > 0 || write > 0 || window > 0 || (output == 0 && reasoning == 0))
}
