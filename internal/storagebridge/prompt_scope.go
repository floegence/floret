package storagebridge

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type PromptDestination struct {
	Scope  string
	Author identity.ThreadID
}

func ViewPrompt(ctx context.Context, backend spi.Backend, scope string, read func(spi.ReadTx) error) error {
	if routed, ok := backend.(interface {
		ViewPromptRecords(context.Context, string, func(spi.ReadTx) error) error
	}); ok {
		return routed.ViewPromptRecords(ctx, scope, read)
	}
	return backend.View(ctx, read)
}

func UpdatePrompt(ctx context.Context, backend spi.Backend, destinations []PromptDestination, write func(spi.WriteTx) error) error {
	if routed, ok := backend.(interface {
		UpdatePromptRecords(context.Context, []PromptDestination, func(spi.WriteTx) error) error
	}); ok && len(destinations) != 0 {
		return routed.UpdatePromptRecords(ctx, destinations, write)
	}
	return backend.Update(ctx, write)
}
