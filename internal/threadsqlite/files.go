package threadsqlite

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

type FileID [16]byte

type Reference struct {
	File   FileID
	Thread identity.ThreadID
}

type Files struct {
	directory string
	store     FileID
	pool      *filePool
	format    fileFormat
}

type fileFormat struct {
	version    int
	schema     []string
	initialize func(context.Context, *sql.Tx, FileID, Reference) error
}

const headerSchema = `CREATE TABLE floret_thread_header (singleton INTEGER PRIMARY KEY CHECK(singleton=1), store BLOB NOT NULL, file BLOB NOT NULL, thread TEXT NOT NULL, format INTEGER NOT NULL)`
const sealSchema = `CREATE TABLE floret_thread_preparation (singleton INTEGER PRIMARY KEY CHECK(singleton=1), fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32))`
const recordsSchema = `CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY(namespace,key))`

var ErrFileIdentity = errors.New("unexpected Floret thread file identity")
var ErrFileFormat = errors.New("unsupported Floret thread file format")

func NewFiles(directory string, store FileID, limit, maxIdle int, ttl time.Duration) (*Files, error) {
	return newFiles(directory, store, limit, maxIdle, ttl, fileFormat{1, []string{headerSchema, recordsSchema, sealSchema}, initializeRecordFile})
}

func NewMemberFiles(directory string, store FileID, limit, maxIdle int, ttl time.Duration) (*Files, error) {
	return newFiles(directory, store, limit, maxIdle, ttl, fileFormat{member.Format, member.Definitions(), func(ctx context.Context, tx *sql.Tx, store FileID, ref Reference) error {
		return member.Initialize(ctx, tx, store, ref.File, ref.Thread)
	}})
}

func newFiles(directory string, store FileID, limit, maxIdle int, ttl time.Duration, format fileFormat) (*Files, error) {
	if !filepath.IsAbs(directory) || store == (FileID{}) {
		return nil, spi.ErrInvalidArgument
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, err
	}
	if resolved != filepath.Clean(directory) {
		return nil, ErrFileIdentity
	}
	if err := checkFile(directory, true); err != nil {
		return nil, err
	}
	files := &Files{directory: directory, store: store, format: format}
	files.pool, err = newFilePool(limit, maxIdle, ttl, files.open)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func checkFile(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return ErrFileIdentity
	}
	return nil
}

func (files *Files) path(file FileID) string {
	key := hex.EncodeToString(file[:])
	return filepath.Join(files.directory, key[:2], key+".sqlite")
}

func checkSidecars(path string, allowExisting bool) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		err := checkFile(path+suffix, false)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !allowExisting {
			return os.ErrExist
		}
	}
	return nil
}

func existingDB(path string, pragmas ...string) (*sql.DB, error) {
	options := url.Values{"mode": {"rw"}, "_pragma": {"synchronous(FULL)", "trusted_schema(OFF)", "foreign_keys(ON)"}}
	for _, pragma := range pragmas {
		options.Add("_pragma", pragma)
	}
	db, err := sql.Open("sqlite", databaseURI(path, options))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func databaseURI(path string, options url.Values) string {
	name := filepath.ToSlash(path)
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	dsn := url.URL{Scheme: "file", Path: name, RawQuery: options.Encode()}
	return dsn.String()
}

func (files *Files) open(ctx context.Context, file FileID) (*sql.DB, error) {
	path := files.path(file)
	if err := checkFile(filepath.Dir(path), true); err != nil {
		return nil, err
	}
	if err := checkFile(path, false); err != nil {
		return nil, err
	}
	if err := checkSidecars(path, true); err != nil {
		return nil, err
	}
	db, err := existingDB(path)
	if err != nil {
		return nil, err
	}
	if err := checkSchema(ctx, db, files.format.schema); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func checkSchema(ctx context.Context, query interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, schemas []string) error {
	return checkDatabaseSchema(ctx, query, "main", schemas)
}

func checkDatabaseSchema(ctx context.Context, query interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, database string, schemas []string) error {
	if database != "main" && database != "floret_related" {
		return ErrFileFormat
	}
	rows, err := query.QueryContext(ctx, `SELECT sql FROM `+database+`.sqlite_schema WHERE name NOT GLOB 'sqlite_*'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	missing := make(map[string]struct{}, len(schemas))
	for _, definition := range schemas {
		missing[definition] = struct{}{}
	}
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			return err
		}
		if _, ok := missing[definition]; !ok {
			return ErrFileFormat
		}
		delete(missing, definition)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(missing) != 0 {
		return ErrFileFormat
	}
	return nil
}

func Allocate(thread identity.ThreadID) (Reference, error) {
	if thread == "" {
		return Reference{}, spi.ErrInvalidArgument
	}
	var key FileID
	if _, err := rand.Read(key[:]); err != nil {
		return Reference{}, err
	}
	return Reference{File: key, Thread: thread}, nil
}

func (files *Files) Create(ctx context.Context, ref Reference) error {
	if ctx == nil || ref.File == (FileID{}) || ref.Thread == "" {
		return spi.ErrInvalidArgument
	}
	return files.pool.access(ctx, ref.File, true, func(ctx context.Context, _ FileID) (*sql.DB, error) { return files.create(ctx, ref) }, func(*sql.DB) error { return nil })
}

func (files *Files) create(ctx context.Context, ref Reference) (*sql.DB, error) {
	path := files.path(ref.File)
	bucket := filepath.Dir(path)
	if err := os.Mkdir(bucket, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if err := checkFile(bucket, true); err != nil {
		return nil, err
	}
	if err := checkSidecars(path, false); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	db, err := existingDB(path)
	if err != nil {
		return nil, err
	}
	err = files.initialize(ctx, db, ref)
	if err == nil {
		err = syncDirectory(bucket)
	}
	if err == nil {
		err = syncDirectory(files.directory)
	}
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func (files *Files) initialize(ctx context.Context, db *sql.DB, ref Reference) error {
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil || mode != "delete" {
		return errors.Join(fmt.Errorf("thread journal mode: %q", mode), err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := files.format.initialize(ctx, tx, files.store, ref); err != nil {
		return err
	}
	return tx.Commit()
}

func initializeRecordFile(ctx context.Context, tx *sql.Tx, store FileID, ref Reference) error {
	for _, statement := range []string{headerSchema, recordsSchema, sealSchema} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_header VALUES(1,?,?,?,1)`, store[:], ref.File[:], string(ref.Thread))
	return err
}

func (files *Files) transaction(ctx context.Context, ref Reference, readOnly bool, action func(*sql.Tx) error) error {
	if ref.Thread == "" || action == nil {
		return spi.ErrInvalidArgument
	}
	return files.pool.withFile(ctx, ref.File, func(db *sql.DB) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		return files.onConnection(ctx, conn, ref, readOnly, action)
	})
}

func (files *Files) onConnection(ctx context.Context, conn *sql.Conn, ref Reference, readOnly bool, action func(*sql.Tx) error) error {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var store, key []byte
	var thread string
	var format int
	if err := tx.QueryRowContext(ctx, `SELECT store,file,thread,format FROM floret_thread_header WHERE singleton=1`).Scan(&store, &key, &thread, &format); err != nil {
		return err
	}
	if format != files.format.version {
		return ErrFileFormat
	}
	if !bytes.Equal(store, files.store[:]) || !bytes.Equal(key, ref.File[:]) || thread != string(ref.Thread) {
		return ErrFileIdentity
	}
	if err := action(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if readOnly {
		return nil
	}
	return tx.Commit()
}

func (files *Files) Project(ctx context.Context, source, target Reference, project func(read, write *sql.Tx) error) error {
	if project == nil || source.Thread == "" || target.Thread == "" || source.Thread == target.Thread || source.File == target.File || source.File == (FileID{}) || target.File == (FileID{}) {
		return spi.ErrInvalidArgument
	}
	first, second := source, target
	if bytes.Compare(first.File[:], second.File[:]) > 0 {
		first, second = second, first
	}
	withConn := func(ref Reference, use func(*sql.Conn) error) error {
		return files.pool.accessGranted(ctx, ref.File, false, files.pool.open, func(db *sql.DB) error {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			return use(conn)
		})
	}
	return files.pool.admit(ctx, 2, func() error {
		return withConn(first, func(left *sql.Conn) error {
			return withConn(second, func(right *sql.Conn) error {
				reader, writer := left, right
				if first != source {
					reader, writer = right, left
				}
				return files.onConnection(ctx, reader, source, true, func(snapshot *sql.Tx) error {
					return files.onConnection(ctx, writer, target, false, func(destination *sql.Tx) error { return project(snapshot, destination) })
				})
			})
		})
	})
}

func (files *Files) Close(ctx context.Context) error { return files.pool.shutdown(ctx) }

func (files *Files) View(ctx context.Context, ref Reference, action func(*sql.Tx) error) error {
	return files.transaction(ctx, ref, true, action)
}

func (files *Files) Update(ctx context.Context, ref Reference, action func(*sql.Tx) error) error {
	return files.transaction(ctx, ref, false, action)
}
