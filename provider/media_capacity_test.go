package provider_test

import (
	"errors"
	"github.com/floegence/floret/v7/provider"
	"testing"
)

func TestMediaCapacityClassificationPreservesErrorIdentity(t *testing.T) {
	for _, kind := range []string{"count", "total_bytes", "bytes", "dimension", "animated_dimension", "unknown"} {
		cause := &provider.MediaLimitError{Kind: kind, Actual: 3, Limit: 2}
		got, actual, limit := cause.RequestCapacityLimit()
		aggregate := kind == "count" || kind == "total_bytes"
		if (got != "") != aggregate || aggregate && (actual != 3 || limit != 2) {
			t.Fatalf("%s: %s %d/%d", kind, got, actual, limit)
		}
		var typed *provider.MediaLimitError
		if errors.Is(cause, provider.ErrContextOverflow) || !errors.As(errors.Join(errors.New("wrapped"), cause), &typed) {
			t.Fatal("changed published error identity")
		}
	}
	for _, cause := range []*provider.MediaLimitError{nil, {Kind: "count", Actual: 1, Limit: 2}, {Kind: "count", Actual: 3, Limit: 0}} {
		if kind, _, _ := cause.RequestCapacityLimit(); kind != "" {
			t.Fatal("invalid capacity evidence accepted")
		}
	}
}
