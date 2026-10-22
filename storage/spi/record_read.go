package spi

// BoundedRecordReader optionally reads record metadata and value slices without
// materializing the complete value. The built-in backends implement it so
// startup probes and historical snapshot import do not load large bodies.
// Methods share the transaction's snapshot, cancellation, and lifetime.
// Capability errors must be returned, never retried through Get or Scan.
type BoundedRecordReader interface {
	// NamespaceHasRecords reports whether any key exists without loading values.
	// An empty namespace returns ErrInvalidArgument.
	NamespaceHasRecords(namespace string) (bool, error)
	// RecordSize returns the byte length, including zero for an empty value.
	// A missing record returns ErrNotFound.
	RecordSize(namespace string, key []byte) (int64, error)
	// ReadRecordRange returns a caller-owned slice of at most limit bytes,
	// starting at the zero-based byte offset. At or beyond the end it returns
	// an empty slice. Missing records return ErrNotFound. Negative offsets,
	// nonpositive limits, empty namespaces, and empty keys are invalid.
	ReadRecordRange(namespace string, key []byte, offset int64, limit int) ([]byte, error)
}
