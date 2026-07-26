// Package runtime exposes Floret's durable, host-facing Agent runtime.
//
// Floret owns canonical threads, turns, runs, provider-visible history,
// approvals, Agent todos, tool outcomes, projections, and recovery state. A
// host retains product policy, authorization, resources, model transport, UI,
// and commands that have not yet been admitted to a Floret thread.
//
// Hosts create one Store, configure its capability binders exactly once with
// ConfigureHostCapabilities, bind the narrow capability needed for an exact
// ThreadID or parent ThreadID, and close the Store only after active work has
// stopped. Public snapshot and result Validate methods are intended for host
// integration boundaries; invalid values must not be repaired from host
// metadata, observation events, or Floret implementation records.
package runtime
