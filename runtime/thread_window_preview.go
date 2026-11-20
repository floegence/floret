package runtime

import (
	"context"
	"strings"
	"sync"
)

type windowTextKey struct {
	thread, leaf, item string
	bytes, limit       int
	kind               ThreadItemKind
}

const windowPreviewSlots = 1024

type windowTextPreviews struct {
	mu     sync.Mutex
	values map[windowTextKey]string
	order  []windowTextKey
	next   int
}

func (actor *threadRuntimeState) readWindowPreview(ctx context.Context, item ThreadItem, limit int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if limit <= 0 || limit > 1024 || item.Live || item.liveBody != nil || item.textLeafID == "" || item.CanonicalOrdinal == 0 || item.controlTextBytes != 0 || item.Interaction != nil || item.Kind != ThreadItemUser && item.Kind != ThreadItemAssistant || len(actor.threadID)+len(item.ID)+len(item.textLeafID) > 512 {
		return actor.service.readItemText(ctx, actor.threadID, item, limit)
	}
	key := windowTextKey{actor.threadID, item.textLeafID, item.ID, item.textBytes, limit, item.Kind}
	pool := &actor.service.windowPreviews
	pool.mu.Lock()
	value, found := pool.values[key]
	pool.mu.Unlock()
	if found {
		return value, nil
	}
	value, err := actor.service.readItemText(ctx, actor.threadID, item, limit)
	if err != nil {
		return "", err
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if _, found := pool.values[key]; !found {
		if pool.values == nil {
			pool.values = make(map[windowTextKey]string)
		}
		if len(pool.order) == windowPreviewSlots {
			delete(pool.values, pool.order[pool.next])
			pool.order[pool.next] = key
			pool.next = (pool.next + 1) % windowPreviewSlots
		} else {
			pool.order = append(pool.order, key)
		}
		pool.values[key] = strings.Clone(value)
	}
	return value, nil
}
