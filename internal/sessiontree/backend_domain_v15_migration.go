package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
)

// The reference addition changes no existing record, fingerprint or identity.
func migrateBackendDomainV14ToV15(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV14Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV15Memory(memory)
}

func validatePreThreadReferenceMemory(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		for _, entry := range entries {
			references := entry.Message.References
			if entry.Type == EntryQueueAdded {
				var queued struct {
					Input struct {
						References []session.MessageReference `json:"references"`
					} `json:"input"`
				}
				if err := json.Unmarshal(entry.Payload, &queued); err != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				references = append(append([]session.MessageReference(nil), references...), queued.Input.References...)
			}
			for _, reference := range references {
				if reference.Kind == session.MessageReferenceThread || reference.ReferencedThreadID != "" {
					return errors.Join(ErrAuthorityCorrupt, errors.New("thread references require domain schema 15"))
				}
			}

		}
	}
	return nil
}
