package florettest_test

import (
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v2/florettest"
	"github.com/floegence/floret/v2/storage"
)

func TestOfficialBackendsSatisfyPublicContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		florettest.RunBackendContract(t, storage.Memory())
	})
	t.Run("SQLite", func(t *testing.T) {
		florettest.RunBackendContract(t, storage.SQLite(filepath.Join(t.TempDir(), "floret.db")))
	})
}
