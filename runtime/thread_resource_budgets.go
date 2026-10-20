package runtime

// Native observation budgets bound transient transport memory, never canonical
// content or admitted operations. Overflow requires an authoritative baseline.
const (
	threadUpdateQueueCount = 64
	threadUpdateQueueBytes = 1 << 20
)
