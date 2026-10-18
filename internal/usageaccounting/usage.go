// Package usageaccounting owns provider usage arithmetic, without protocol inference.
package usageaccounting

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
