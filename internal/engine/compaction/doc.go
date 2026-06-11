// Package compaction adapts provider-backed summary generation for engine
// context compaction.
//
// The shared compaction data structures and deterministic cut-point algorithm
// live in session/compaction. This package only connects that shared algorithm
// to provider streaming and records provider-summary retry details. It requires
// a provider; deterministic extractive summaries stay in session/compaction.
package compaction
