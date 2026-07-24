// Package florettest provides deterministic, consumer-facing test helpers for
// Floret integrations. It includes scripted model behavior, public tool and
// approval/effect contracts, terminal outcome contracts, and public-input-only
// Store fixture population. It is intended for test code, examples, and
// external conformance suites; production Floret packages never depend on it.
//
// A contract that requires process death or storage fault injection reports a
// typed ContractPrerequisite and skips only that subtest when the consumer does
// not supply the corresponding fixture. The package never exposes SQLite
// tables, storage internals, or Floret internal packages.
package florettest
