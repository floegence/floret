package sessiontree

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/floegence/floret/v7/storage/spi"
)

const canonicalBodyCacheBytes = 128 << 20

// Only immutable, hash-verified source bytes are cached. Journal identity and
// lifetime remain owned by the canonical store; a cache hit grants no access.
var canonicalBodyCache = struct {
	sync.Mutex
	bytes   int
	entries map[string]*list.Element
	order   list.List
}{entries: map[string]*list.Element{}}

type cachedCanonicalBody struct {
	ref  canonicalBodyRef
	text string
}

func cachedBody(ref canonicalBodyRef) (string, bool) {
	canonicalBodyCache.Lock()
	defer canonicalBodyCache.Unlock()
	element := canonicalBodyCache.entries[ref.SHA256]
	if element == nil {
		return "", false
	}
	body := element.Value.(cachedCanonicalBody)
	if body.ref != ref {
		return "", false
	}
	canonicalBodyCache.order.MoveToFront(element)
	return body.text, true
}

func cacheBody(ref canonicalBodyRef, text string) {
	if len(text) != ref.Bytes || len(text) > canonicalBodyCacheBytes {
		return
	}
	canonicalBodyCache.Lock()
	defer canonicalBodyCache.Unlock()
	if existing := canonicalBodyCache.entries[ref.SHA256]; existing != nil {
		canonicalBodyCache.order.MoveToFront(existing)
		return
	}
	for canonicalBodyCache.bytes+len(text) > canonicalBodyCacheBytes {
		element := canonicalBodyCache.order.Back()
		previous := element.Value.(cachedCanonicalBody)
		delete(canonicalBodyCache.entries, previous.ref.SHA256)
		canonicalBodyCache.bytes -= len(previous.text)
		canonicalBodyCache.order.Remove(element)
	}
	canonicalBodyCache.entries[ref.SHA256] = canonicalBodyCache.order.PushFront(cachedCanonicalBody{ref, text})
	canonicalBodyCache.bytes += len(text)
}

func releaseCachedBody(ref canonicalBodyRef) {
	canonicalBodyCache.Lock()
	defer canonicalBodyCache.Unlock()
	if element := canonicalBodyCache.entries[ref.SHA256]; element != nil {
		canonicalBodyCache.bytes -= len(element.Value.(cachedCanonicalBody).text)
		canonicalBodyCache.order.Remove(element)
		delete(canonicalBodyCache.entries, ref.SHA256)
	}
}

// compactCanonicalEntry runs only after validation and durable installation.
// Metadata and source locators remain resident; complete reads hydrate into the
// caller's result, never back into the journal's retained entry slice.
func compactCanonicalEntry(entry Entry) Entry {
	if entry.bodyFields != nil {
		return entry
	}
	refs := map[string]canonicalBodyRef{}
	values, err := canonicalStoredFields(entry, true)
	if err != nil {
		panic(err)
	} // Validated entries have JSON-encodable typed activity.
	for field, value := range values {
		refs[field] = canonicalBodyRef{SHA256: stableHash(value), Bytes: len(value)}
		_ = clearCanonicalField(&entry, field)
	}
	if len(refs) == 0 {
		return entry
	}
	entry.Raw = ""
	entry.bodyFields = refs
	return entry
}

func (r *MemoryRepo) completeEntryLocked(entry Entry) (Entry, error) {
	if entry.bodyFields == nil {
		return entry, nil
	}
	ctx := r.bodyContext
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	complete := entry
	for field, ref := range entry.bodyFields {
		text, found := cachedBody(ref)
		if !found {
			read := func(tx spi.ReadTx) error {
				var err error
				text, err = materializeCanonicalBody(ctx, tx, ref)
				return err
			}
			var err error
			if r.bodyTx != nil {
				err = read(r.bodyTx)
			} else if r.bodyBackend != nil {
				err = r.bodyBackend.View(ctx, read)
			} else {
				err = errors.New("canonical body storage is unavailable")
			}
			if err != nil {
				return Entry{}, err
			}
			cacheBody(ref, text)
		}
		if err := restoreCanonicalField(&complete, field, text); err != nil {
			return Entry{}, err
		}
	}
	complete.bodyFields = nil
	complete.Raw = rawForEntry(complete)
	if err := ValidateEntryIntegrity(complete); err != nil {
		return Entry{}, err
	}
	return complete, nil
}

func (r *MemoryRepo) completeEntriesLocked(entries []Entry) ([]Entry, error) {
	result := make([]Entry, len(entries))
	for i, entry := range entries {
		complete, err := r.completeEntryLocked(entry)
		if err != nil {
			return nil, err
		}
		result[i] = cloneEntry(complete)
	}
	return result, nil
}

func (r *MemoryRepo) compactBodiesLocked(backend spi.Backend) {
	r.bodyBackend = backend
	for threadID, entries := range r.entries {
		for ordinal := range entries {
			entries[ordinal] = compactCanonicalEntry(entries[ordinal])
		}
		r.entries[threadID] = entries
	}
}
