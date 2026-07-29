// Package runtime exposes Floret's durable, host-facing Agent runtime.
//
// Floret owns canonical threads, turns, runs, provider-visible history,
// approvals, Agent todos, tool outcomes, projections, and recovery state. A
// host retains product policy, authorization, resources, model transport, UI,
// and commands that have not yet been admitted to a Floret thread.
//
// Hosts create one store, configure its capability binders exactly once with
// configureHostCapabilities, bind the narrow capability needed for an exact
// ThreadID or parent ThreadID, and close the store only after active work has
// stopped. Public snapshot and result Validate methods are intended for host
// integration boundaries; invalid values must not be repaired from host
// metadata, observation events, or Floret implementation records.
//
// Bind methods bind an identity or intent without first claiming that the
// referenced store state exists. Provider-free capabilities that require
// existing authority use NewHost(ctx, ...) so construction can validate the
// store. Recovery keeps explicit BindThread and BindSubAgent entry points
// because root and parent-child recovery authority are different contracts.
// Provider-backed factories accept only opaque option values returned by
// newTurnExecutionOptions, newThreadCompactionOptions, or
// newSubAgentOptions; factories revalidate both those values and current
// store authority before issuing a Host.
package runtime
