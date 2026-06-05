// Package session defines the provider-visible transcript shape and low-level
// ephemeral transcript store.
//
// Message is the normalized transcript item passed to providers after host and
// control projections have been applied. Store is a run-scoped transcript cache
// for low-level engine execution; it is not the recommended durable persistence
// API for host applications. Durable conversations should use
// sessiontree.Repo through agentharness.Thread.
package session
