package threadsqlite

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

const promptOwnerNamespace = "floret.layout.prompt-owner.v1"

func BindPromptScope(tx spi.WriteTx, scope string, thread identity.ThreadID) error {
	catalog := storagebridge.CatalogWriter(tx)
	owner, err := promptOwner(catalog, scope)
	if err != nil || thread == "" || owner != "" && owner != thread {
		return errors.Join(spi.ErrConflict, err)
	}
	return catalog.Put(promptOwnerNamespace, []byte(scope), []byte(thread))
}

func promptOwner(tx spi.ReadTx, scope string) (identity.ThreadID, error) {
	if scope == "" {
		return "", spi.ErrInvalidArgument
	}
	value, err := tx.Get(promptOwnerNamespace, []byte(scope))
	if errors.Is(err, spi.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	owner, err := identity.ParseThreadID(string(value))
	if err != nil || owner.String() != string(value) {
		return "", ErrFileIdentity
	}
	return owner, nil
}

type emptyPromptScope struct{}

func (emptyPromptScope) Get(string, []byte) ([]byte, error) { return nil, spi.ErrNotFound }
func (emptyPromptScope) Scan(spi.ScanRequest) (spi.ScanPage, error) {
	return spi.ScanPage{}, nil
}

func (backend *Backend) ViewPromptRecords(ctx context.Context, scope string, read func(spi.ReadTx) error) error {
	if read == nil {
		return spi.ErrInvalidArgument
	}
	var owner identity.ThreadID
	err := backend.View(ctx, func(tx spi.ReadTx) (err error) {
		owner, err = promptOwner(tx, scope)
		if err == nil && owner == "" {
			return read(emptyPromptScope{})
		}
		return err
	})
	if err != nil || owner == "" {
		return err
	}
	return backend.ViewThreadRecords(ctx, storagebridge.ThreadRead{Thread: owner}, read)
}

func (backend *Backend) UpdatePromptRecords(ctx context.Context, destinations []storagebridge.PromptDestination, write func(spi.WriteTx) error) error {
	if len(destinations) == 0 || write == nil {
		return spi.ErrInvalidArgument
	}
	var thread identity.ThreadID
	binding := func(tx spi.ReadTx) error {
		for _, destination := range destinations {
			owner, err := promptOwner(tx, destination.Scope)
			if err != nil {
				return err
			}
			if owner == "" {
				owner = destination.Author
			}
			if owner == "" || destination.Author != "" && owner != destination.Author || thread != "" && thread != owner {
				return ErrUnscopedWrite
			}
			thread = owner
		}
		return nil
	}
	if err := backend.View(ctx, binding); err != nil {
		return err
	}
	return backend.UpdateThreadRecords(ctx, thread, func(tx spi.WriteTx) error {
		catalog := storagebridge.CatalogWriter(tx)
		if err := binding(catalog); err != nil {
			return err
		}
		for _, destination := range destinations {
			if err := BindPromptScope(tx, destination.Scope, thread); err != nil {
				return err
			}
		}
		return write(tx)
	})
}
