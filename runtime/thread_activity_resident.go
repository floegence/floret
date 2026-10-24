package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/threadquery"
)

const residentActivityFieldCount = 256
const residentActivityFieldBytes = 8 << 20
const residentActivityFieldSpillBytes = 256 << 20

type residentActivityStorage struct {
	owner            *outputbody.Owner
	body             *outputbody.Body
	size, references int
}

type residentActivityField struct {
	id, item, path, text string
	run                  identity.RunID
	used                 uint64
	size                 int
	hash                 [32]byte
	owner                *outputbody.Owner
	body                 *outputbody.Body
	shared               *residentActivityStorage
}

// The existing actor owns this disposable reading cache. Its separate mutex
// keeps large field snapshots and spill I/O outside the execution boundary.
// No source survives restart or conveys authority. Canonical history remains
// the only durable owner. Memory, spill bytes and entry count have fixed limits.
type residentActivityFields struct {
	mu                sync.Mutex
	entries           []residentActivityField
	bytes, spillBytes int
	clock             uint64
	closed            bool
	closeErr          error
}

func (fields *residentActivityFields) retain(item ThreadItem, path, source string) (string, error) {
	return fields.retainContext(context.Background(), item, path, source)
}
func (fields *residentActivityFields) retainContext(ctx context.Context, item ThreadItem, path, source string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(source) > residentActivityFieldSpillBytes {
		return "", threadquery.ErrInvalidQuery
	}
	// Freeze a candidate descriptor, not its bytes. A concurrent eviction or
	// append is checked again before sharing its verified immutable prefix.
	fields.mu.Lock()
	var candidate residentActivityField
	for _, field := range fields.entries {
		if field.item == item.ID && field.run == item.RunID && field.path == path && field.shared != nil && field.size == field.shared.size && field.size <= len(source) && field.size > candidate.size {
			candidate = field
		}
	}
	fields.mu.Unlock()
	hasher := sha256.New()
	var prefixHash [32]byte
	for offset := 0; offset < len(source); {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		end := min(len(source), offset+16384)
		if offset < candidate.size {
			end = min(end, candidate.size)
		}
		hasher.Write([]byte(source[offset:end]))
		offset = end
		if offset == candidate.size {
			copy(prefixHash[:], hasher.Sum(nil))
		}
	}
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	fields.mu.Lock()
	defer fields.mu.Unlock()
	if fields.closed {
		return "", threadquery.ErrRecordUnavailable
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fields.clock++
	var shared *residentActivityStorage
	for i := range fields.entries {
		field := &fields.entries[i]
		if field.item == item.ID && field.run == item.RunID && field.path == path && field.size == len(source) && field.hash == hash {
			field.used = fields.clock
			return field.id, nil
		}
		if field.id == candidate.id && field.shared != nil && field.shared.size == candidate.size && prefixHash == candidate.hash {
			shared = field.shared
		}
	}
	spilled := len(source) > residentActivityFieldBytes
	additional := len(source)
	if shared != nil {
		additional -= shared.size
	}
	for len(fields.entries) >= residentActivityFieldCount || !spilled && fields.bytes+additional > residentActivityFieldBytes || spilled && fields.spillBytes+additional > residentActivityFieldSpillBytes {
		oldest := -1
		for i := range fields.entries {
			field := fields.entries[i]
			// Keep one source reference until its new frozen extent is installed.
			if shared != nil && field.shared == shared && shared.references == 1 {
				continue
			}
			if oldest < 0 || field.used < fields.entries[oldest].used {
				oldest = i
			}
		}
		if oldest < 0 {
			return "", threadquery.ErrInvalidQuery
		}
		if err := fields.remove(oldest); err != nil {
			return "", err
		}
	}
	field := residentActivityField{id: rand.Text(), item: item.ID, run: item.RunID, path: path, size: len(source), hash: hash, used: fields.clock}
	if shared != nil {
		// Body.Append is atomic with respect to its published byte extent. Old
		// readers keep their original size even while the shared body grows.
		if err := shared.body.Append(source[shared.size:]); err != nil {
			return "", err
		}
		fields.spillBytes += len(source) - shared.size
		shared.size = len(source)
		shared.references++
		field.shared, field.owner, field.body = shared, shared.owner, shared.body
	} else if spilled {
		field.owner = outputbody.NewOwner()
		field.body = field.owner.NewBody()
		for offset := 0; offset < len(source); {
			if err := ctx.Err(); err != nil {
				return "", errors.Join(err, field.owner.Close())
			}
			end := min(len(source), offset+16384)
			if err := field.body.Append(source[offset:end]); err != nil {
				return "", errors.Join(err, field.owner.Close())
			}
			offset = end
		}
		field.shared = &residentActivityStorage{owner: field.owner, body: field.body, size: field.size, references: 1}
		fields.spillBytes += field.size
	} else {
		field.text = strings.Clone(source)
		fields.bytes += field.size
	}
	fields.entries = append(fields.entries, field)
	return field.id, nil
}
func (fields *residentActivityFields) read(ctx context.Context, item ThreadItem, input *ThreadActivityFieldInput, limit int) (string, int, bool, error) {
	fields.mu.Lock()
	defer fields.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}
	if fields.closed {
		return "", 0, false, nil
	}
	for i := range fields.entries {
		field := &fields.entries[i]
		if field.id != input.SourceID || field.item != item.ID || field.run != item.RunID || field.path != input.Path {
			continue
		}
		fields.clock++
		field.used = fields.clock
		if input.OffsetBytes > field.size {
			return "", 0, true, threadquery.ErrInvalidQuery
		}
		if field.body != nil {
			if input.AlignStart {
				for shift := 0; shift < utf8.UTFMax; shift++ {
					_, err := field.body.Read(ctx, input.OffsetBytes, utf8.UTFMax, field.size)
					if err == nil {
						break
					}
					if !errors.Is(err, os.ErrInvalid) || input.OffsetBytes == 0 {
						return "", 0, true, err
					}
					input.OffsetBytes--
				}
			}
			text, err := field.body.Read(ctx, input.OffsetBytes, limit, field.size)
			if errors.Is(err, os.ErrInvalid) {
				err = threadquery.ErrInvalidQuery
			}
			return text, field.size, true, err
		}
		if input.AlignStart {
			for input.OffsetBytes > 0 && input.OffsetBytes < field.size && !utf8.RuneStart(field.text[input.OffsetBytes]) {
				input.OffsetBytes--
			}
		}
		if input.OffsetBytes < field.size && !utf8.RuneStart(field.text[input.OffsetBytes]) {
			return "", 0, true, threadquery.ErrInvalidQuery
		}
		end := min(field.size, input.OffsetBytes+limit)
		for end < field.size && end > input.OffsetBytes && !utf8.RuneStart(field.text[end]) {
			end--
		}
		return strings.Clone(field.text[input.OffsetBytes:end]), field.size, true, nil
	}
	return "", 0, false, nil
}

// remove releases physical storage only after its final frozen extent leaves.
// The cache mutex serializes readers, growth and eviction; execution never owns it.
func (fields *residentActivityFields) remove(index int) error {
	field := fields.entries[index]
	if field.shared != nil {
		if field.shared.references == 1 {
			if err := field.shared.owner.Close(); err != nil {
				fields.closeErr = errors.Join(fields.closeErr, err)
				return err
			}
			fields.spillBytes -= field.shared.size
		}
		field.shared.references--
	} else {
		fields.bytes -= field.size
	}
	fields.entries[index] = fields.entries[len(fields.entries)-1]
	fields.entries = fields.entries[:len(fields.entries)-1]
	return nil
}
func (fields *residentActivityFields) close() error {
	fields.mu.Lock()
	defer fields.mu.Unlock()
	fields.closed = true
	for index := len(fields.entries) - 1; index >= 0; index-- {
		_ = fields.remove(index)
	}
	return fields.closeErr
}
func (service *threadRuntimeService) readResidentActivityField(ctx context.Context, item ThreadItem, input ThreadActivityFieldInput) (ThreadActivityFieldChunk, error) {
	limit := input.LimitBytes
	if limit == 0 {
		limit = 16 << 10
	}
	if input.EntryID != "" || input.RunID == "" || input.OffsetBytes < 0 || limit < 4 || limit > 16<<10 || input.SHA256 != "" || input.SourceSHA256 != "" {
		return ThreadActivityFieldChunk{}, threadquery.ErrInvalidQuery
	}
	if item.RunID != input.RunID {
		return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
	}
	actor := service.runtime(input.ThreadID)
	if err := ctx.Err(); err != nil {
		return ThreadActivityFieldChunk{}, err
	}
	text, size, found, err := actor.activityFields.read(ctx, item, &input, limit)
	if err != nil {
		return ThreadActivityFieldChunk{}, err
	}
	if !found {
		return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ThreadActivityFieldChunk{}, err
	}
	return boundActivityFieldChunk(ThreadActivityFieldChunk{ThreadID: input.ThreadID, RunID: item.RunID, SourceID: input.SourceID, Path: input.Path, Bytes: size, OffsetBytes: input.OffsetBytes, Text: text})
}
