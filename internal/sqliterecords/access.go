package sqliterecords

import (
	"context"
	"database/sql"

	"github.com/floegence/floret/v7/storage/spi"
)

func New(ctx context.Context, transaction *sql.Tx, readOnly bool) *Tx {
	return &Tx{ctx: ctx, tx: transaction, active: true, readOnly: readOnly, table: "main.floret_backend_records"}
}

func AttachedCatalog(ctx context.Context, transaction *sql.Tx, readOnly bool) *Tx {
	return &Tx{ctx: ctx, tx: transaction, active: true, readOnly: readOnly, table: "floret_catalog.floret_backend_records"}
}

func AttachedThread(ctx context.Context, transaction *sql.Tx) *Tx {
	return &Tx{ctx: ctx, tx: transaction, active: true, table: "floret_related.floret_backend_records"}
}

func Access(records *Tx, write bool, action func(context.Context, *sql.Tx) (bool, error)) (bool, error) {
	records.mu.Lock()
	defer records.mu.Unlock()
	if err := records.usable(); err != nil {
		return false, err
	}
	if write && records.readOnly {
		return false, spi.ErrInvalidArgument
	}
	return action(records.ctx, records.tx)
}
