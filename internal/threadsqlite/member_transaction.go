package threadsqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

var errMemberRecord = errors.New("thread members accept domain facts, not key/value records")

type memberTx struct{ journal *member.Journal }

func (tx memberTx) MemberJournal() *member.Journal          { return tx.journal }
func (memberTx) Get(string, []byte) ([]byte, error)         { return nil, errMemberRecord }
func (memberTx) Scan(spi.ScanRequest) (spi.ScanPage, error) { return spi.ScanPage{}, errMemberRecord }
func (memberTx) Put(string, []byte, []byte) error           { return errMemberRecord }
func (memberTx) Delete(string, []byte) error                { return errMemberRecord }

type memberWrite struct {
	memberTx
	threadCatalog
}

type threadCatalog struct {
	catalog *sqliterecords.Tx
	owner   identity.ThreadID
	other   identity.ThreadID
	related spi.WriteTx
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
