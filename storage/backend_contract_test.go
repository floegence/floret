package storage

import (
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/florettest"
)

func TestOfficialBackendsSatisfySPIContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		florettest.RunBackendContract(t, memorySource{})
	})
	t.Run("sqlite", func(t *testing.T) {
		florettest.RunBackendContract(t, sqliteSource{path: filepath.Join(t.TempDir(), "floret.db")})
	})
}
