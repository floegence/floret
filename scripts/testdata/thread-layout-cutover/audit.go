//go:build floret_layout_cutover

// Package cutover provides offline checks for the single-layout transition.
package cutover

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"net/url"
	"os"
	"path/filepath"

	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

type Source struct{ transaction *sql.Tx }

type Inventory struct {
	Rows       uint64
	ValueBytes uint64
	Digest     [32]byte
}

func WithSource(ctx context.Context, path string, inspect func(*Source) error) (err error) {
	return withExclusiveSource(ctx, path, false, inspect)
}

func withExclusiveSource(ctx context.Context, path string, writable bool, inspect func(*Source) error) (err error) {
	if ctx == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || inspect == nil {
		return spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if resolved != path {
		return spi.ErrInvalidArgument
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return spi.ErrInvalidArgument
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return spi.ErrInvalidArgument
		}
	}
	uri := url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: url.Values{"mode": {"rw"}, "_txlock": {"exclusive"}, "_pragma": {"busy_timeout(0)", "trusted_schema(OFF)", "synchronous(FULL)"}}.Encode()}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	var mode string
	if err := conn.QueryRowContext(ctx, `PRAGMA locking_mode=EXCLUSIVE`).Scan(&mode); err != nil {
		return err
	}
	if mode != "exclusive" {
		return spi.ErrConflict
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return sqliterecords.ClassifyError(ctx, err)
	}
	defer func() {
		if rollback := tx.Rollback(); !errors.Is(rollback, sql.ErrTxDone) {
			err = errors.Join(err, rollback)
		}
	}()
	if !writable {
		if _, err := tx.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
			return err
		}
	}
	if err := storage.CheckOfflineLayoutPhysicalSchema(ctx, tx); err != nil {
		return err
	}
	if err := inspect(&Source{tx}); err != nil || !writable {
		return err
	}
	return tx.Commit()
}

func writeLength(digest hash.Hash, size uint64) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], size)
	digest.Write(length[:])
}

func (source *Source) Records(ctx context.Context) (Inventory, error) {
	var result Inventory
	digest := sha256.New()
	digest.Write([]byte("floret.offline.record-inventory.v1\x00"))
	const chunkBytes = 128 << 10
	rows, err := source.transaction.QueryContext(ctx, `SELECT namespace,key,length(value),typeof(namespace),typeof(key),typeof(value),CASE WHEN length(value)<=? THEN value END FROM floret_backend_records ORDER BY namespace COLLATE BINARY,key`, chunkBytes)
	if err != nil {
		return Inventory{}, err
	}
	defer rows.Close()
	read, err := source.transaction.PrepareContext(ctx, `SELECT substr(value,?,?) FROM floret_backend_records WHERE namespace=? AND key=?`)
	if err != nil {
		return Inventory{}, err
	}
	defer read.Close()
	for rows.Next() {
		var namespace, nsType, keyType, valueType string
		var key, inline []byte
		var size uint64
		if err := rows.Scan(&namespace, &key, &size, &nsType, &keyType, &valueType, &inline); err != nil {
			return Inventory{}, err
		}
		if nsType != "text" || keyType != "blob" || valueType != "blob" || namespace == "" || len(key) == 0 {
			return Inventory{}, fmt.Errorf("%w: invalid physical record", spi.ErrInvalidArgument)
		}
		writeLength(digest, uint64(len(namespace)))
		digest.Write([]byte(namespace))
		writeLength(digest, uint64(len(key)))
		digest.Write(key)
		writeLength(digest, size)
		if size <= chunkBytes && uint64(len(inline)) != size {
			return Inventory{}, errors.New("incomplete inline record")
		}
		digest.Write(inline)
		for pos := uint64(len(inline)); pos < size; {
			length := min(size-pos, chunkBytes)
			var chunk []byte
			if err := read.QueryRowContext(ctx, pos+1, length, namespace, key).Scan(&chunk); err != nil {
				return Inventory{}, err
			}
			if uint64(len(chunk)) != length {
				return Inventory{}, errors.New("incomplete record range")
			}
			digest.Write(chunk)
			pos += length
		}
		result.Rows++
		result.ValueBytes += size
	}
	if err := rows.Err(); err != nil {
		return Inventory{}, err
	}
	if err := ctx.Err(); err != nil {
		return Inventory{}, err
	}
	copy(result.Digest[:], digest.Sum(nil))
	return result, nil
}
