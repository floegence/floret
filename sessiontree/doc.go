// Package sessiontree stores durable conversation journals.
//
// Repo implementations own thread metadata, append-only entries, forks, leaf
// movement, and provider-visible context reconstruction. FileRepo and MemoryRepo
// are safe for concurrent use. Repos used by agentharness should implement
// TurnLeaseRepo so active-turn serialization is durable per ThreadID and shared
// across harness instances that use the same backend. Different ThreadIDs,
// including forked threads, may run concurrently.
package sessiontree
