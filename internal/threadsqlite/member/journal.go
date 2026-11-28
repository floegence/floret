package member

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sync"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type Journal struct {
	ctx      context.Context
	tx       *sql.Tx
	database string
	thread   identity.ThreadID
	revision int64
	readOnly bool
	entries  struct {
		sync.Mutex
		byID     map[string]EntryEvent
		byNumber map[int64]string
		bytes    int
		closed   bool
	}
}

func Enabled(backend spi.Backend) bool {
	typed, ok := backend.(interface{ UsesMemberJournals() bool })
	return ok && typed.UsesMemberJournals()
}

func From(tx spi.ReadTx) *Journal {
	if source, ok := tx.(interface{ MemberJournal() *Journal }); ok {
		return source.MemberJournal()
	}
	return nil
}

func (journal *Journal) RestrictToReads() func() {
	journal.readOnly = true
	return func() {
		journal.entries.Lock()
		defer journal.entries.Unlock()
		journal.entries.closed = true
		journal.entries.byID, journal.entries.byNumber = nil, nil
	}
}

func (journal *Journal) Context() context.Context { return journal.ctx }

func OpenJournal(ctx context.Context, tx *sql.Tx, thread identity.ThreadID, related bool) (*Journal, error) {
	if ctx == nil || tx == nil || thread == "" {
		return nil, spi.ErrInvalidArgument
	}
	database := "main."
	if related {
		database = "floret_related."
	}
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+database+`floret_thread_header WHERE singleton=1 AND format=2 AND thread=?)`, thread.String()).Scan(&valid); err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New("thread journal identity mismatch")
	}
	return &Journal{ctx: ctx, tx: tx, database: database, thread: thread}, nil
}

func (journal *Journal) writeRevision() (int64, error) {
	if journal.readOnly {
		return 0, spi.ErrInvalidArgument
	}
	if journal.revision != 0 {
		return journal.revision, nil
	}
	var revision int64
	err := journal.tx.QueryRowContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET commit_rev=commit_rev+1 WHERE singleton=1 AND commit_rev<? RETURNING commit_rev`, int64(math.MaxInt64)).Scan(&revision)
	if err == nil {
		journal.revision = revision
	}
	return revision, err
}
