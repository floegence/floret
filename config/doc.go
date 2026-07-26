// Package config defines product-neutral Agent, provider, reasoning, and
// context-policy configuration for Floret.
//
// Configuration describes how a host asks Floret to run an Agent. It does not
// own execution identity, durable conversation state, credential persistence,
// or host authorization. A resolved Config may carry provider credentials for
// one composition; the host remains responsible for secret selection, storage,
// and lifecycle. Runtime identities and capabilities are defined by package
// runtime.
package config
