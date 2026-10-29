package provider

import "errors"

// CapacityLimit reads optional aggregate evidence without coupling the Engine
// to an adapter or relabeling independent media capacity as token overflow.
func CapacityLimit(err error) (kind string, actual, limit int64) {
	var capacity interface{ RequestCapacityLimit() (string, int64, int64) }
	if errors.As(err, &capacity) {
		kind, actual, limit = capacity.RequestCapacityLimit()
		if (kind == "image_count" || kind == "image_bytes") && limit > 0 && actual > limit {
			return
		}
	}
	return "", 0, 0
}
func IsCapacityOverflow(err error) bool {
	kind, _, _ := CapacityLimit(err)
	return kind != "" || errors.Is(err, ErrContextOverflow)
}
