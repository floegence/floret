package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

var errMemberRecord = errors.New("thread members accept domain facts, not key/value records")

type memberTx struct{ journal *member.Journal }

func (tx memberTx) MemberJournal() *member.Journal { return tx.journal }
func (memberTx) Get(namespace string, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf("%w: get %s", errMemberRecord, namespace)
}
func (memberTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	return spi.ScanPage{}, fmt.Errorf("%w: scan %s", errMemberRecord, request.Namespace)
}
func (memberTx) Put(namespace string, _, _ []byte) error {
	return fmt.Errorf("%w: put %s", errMemberRecord, namespace)
}
func (memberTx) Delete(namespace string, _ []byte) error {
	return fmt.Errorf("%w: delete %s", errMemberRecord, namespace)
}

type memberWrite struct {
	memberTx
	threadCatalog
}

type memberRead struct {
	memberTx
	catalog spi.ReadTx
	memberPeers
}

func (tx memberRead) CatalogReader() spi.ReadTx { return tx.catalog }

type threadCatalog struct {
	memberPeers
	catalog *sqliterecords.Tx
	owner   identity.ThreadID
	other   identity.ThreadID
	related spi.WriteTx
}

type memberPeers struct {
	files *Files
	tx    *sql.Tx
	ctx   context.Context
}

func (peers memberPeers) ViewPeer(thread identity.ThreadID, visit func(spi.ReadTx) error) (result error) {
	if peers.files == nil || peers.tx == nil || thread == "" || visit == nil {
		return spi.ErrInvalidArgument
	}
	ctx := peers.ctx
	var raw []byte
	err := peers.tx.QueryRowContext(ctx, `SELECT file FROM floret_catalog.floret_thread_routes WHERE thread=?`, thread.String()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storagebridge.ErrRouteMissing
	}
	if err != nil {
		return err
	}
	if raw == nil {
		return storagebridge.ErrRouteDeleted
	}
	if len(raw) != len(FileID{}) {
		return ErrFileIdentity
	}
	ref := Reference{File: FileID(raw), Thread: thread}
	if ref.File == (FileID{}) {
		return ErrFileIdentity
	}
	select {
	case peers.files.peerReads <- struct{}{}:
		defer func() { <-peers.files.peerReads }()
	case <-ctx.Done():
		return ctx.Err()
	}
	path := peers.files.path(ref.File)
	if err := checkFile(filepath.Dir(path), true); err != nil {
		return err
	}
	if err := checkFile(path, false); err != nil {
		return err
	}
	if err := checkSidecars(path, true); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", databaseURI(path, url.Values{"mode": {"ro"}, "_pragma": {"trusted_schema(OFF)", "foreign_keys(ON)"}}))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return peers.files.onConnection(ctx, conn, ref, true, func(tx *sql.Tx) error {
		if err := checkSchema(ctx, tx, peers.files.format.schema); err != nil {
			return err
		}
		journal, err := member.OpenJournal(ctx, tx, thread, false)
		if err != nil {
			return err
		}
		journal.RestrictToReads()
		catalog := sqliterecords.AttachedCatalog(ctx, peers.tx, true)
		defer sqliterecords.End(catalog)
		return visit(memberRead{memberTx: memberTx{journal}, catalog: catalog})
	})
}

func (tx threadCatalog) ThreadOwner() identity.ThreadID { return tx.owner }
func (tx threadCatalog) CatalogReader() spi.ReadTx      { return tx.catalog }
func (tx threadCatalog) CatalogWriter() spi.WriteTx     { return tx.catalog }
func (tx threadCatalog) PutCatalogRecord(record storagebridge.CatalogRecord, raw []byte) error {
	return tx.catalog.Put(record.Namespace, record.Key, raw)
}
func (tx threadCatalog) RelatedWriter(thread identity.ThreadID) (spi.WriteTx, error) {
	if thread == tx.owner || thread == "" || thread != tx.other || tx.related == nil {
		return nil, ErrFileIdentity
	}
	return tx.related, nil
}

func (files *Files) withLocal(ctx context.Context, tx *sql.Tx, thread identity.ThreadID, attached, readOnly bool, owner *threadCatalog, use func(spi.WriteTx) error) error {
	if files.format.version == member.Format {
		journal, err := member.OpenJournal(ctx, tx, thread, attached)
		if err != nil {
			return err
		}
		if readOnly {
			journal.RestrictToReads()
		}
		local := memberTx{journal}
		if owner == nil {
			return use(local)
		}
		return use(memberWrite{local, *owner})
	}
	var records *sqliterecords.Tx
	if attached {
		records = sqliterecords.AttachedThread(ctx, tx)
	} else {
		records = sqliterecords.New(ctx, tx, readOnly)
	}
	defer sqliterecords.End(records)
	if owner == nil {
		return use(records)
	}
	return use(threadWrite{records, *owner})
}
