package sessiontree

import (
	"errors"
	"time"
)

// LeasePolicy is retained only to validate and migrate the released physical
// store schema. ThreadRuntime does not use leases for active lifecycle state.
type LeasePolicy struct {
	TTL                time.Duration
	RenewInterval      time.Duration
	ClockSkewAllowance time.Duration
}

func (policy LeasePolicy) Validate() error {
	if policy.TTL <= 0 {
		return errors.New("lease TTL must be positive")
	}
	if policy.RenewInterval <= 0 {
		return errors.New("lease renew interval must be positive")
	}
	if policy.ClockSkewAllowance < 0 {
		return errors.New("lease clock skew allowance must be non-negative")
	}
	if policy.RenewInterval > policy.TTL/3 {
		return errors.New("lease renew interval must not exceed one third of TTL")
	}
	return nil
}

var DefaultLeasePolicy = LeasePolicy{
	TTL:                30 * time.Second,
	RenewInterval:      10 * time.Second,
	ClockSkewAllowance: 2 * time.Second,
}
