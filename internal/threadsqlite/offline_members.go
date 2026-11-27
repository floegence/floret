//go:build floret_layout_cutover

package threadsqlite

import (
	"context"
	"database/sql"
	"path/filepath"

	"github.com/floegence/floret/v7/identity"
)

func (backend *Backend) OfflineMembers(ctx context.Context) (map[identity.ThreadID]string, string, int, error) {
	members := make(map[identity.ThreadID]string)
	deleted := 0
	err := backend.catalogTransaction(ctx, true, func(tx *sql.Tx) error {
		var pending int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM floret_thread_staging").Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return ErrFileIdentity
		}
		rows, err := tx.QueryContext(ctx, "SELECT thread,file FROM floret_thread_routes ORDER BY thread")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var thread string
			var raw []byte
			if err := rows.Scan(&thread, &raw); err != nil {
				return err
			}
			id, err := identity.ParseThreadID(thread)
			if err != nil {
				return err
			}
			if raw == nil {
				deleted++
				continue
			}
			var file FileID
			if len(raw) != len(file) {
				return ErrFileIdentity
			}
			copy(file[:], raw)
			if file == (FileID{}) {
				return ErrFileIdentity
			}
			members[id] = backend.Files.path(file)
		}
		return rows.Err()
	})
	return members, filepath.Clean(backend.Catalog.path), deleted, err
}
