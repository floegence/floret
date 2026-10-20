// Package outputbody owns disposable, append-only provider output. It has no
// journal, replay or recovery role; canonical installation remains transactional.
package outputbody

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"unicode/utf8"
)

const MemoryBytes = 4 << 20
const ChunkBytes = 16 << 10

// Reader freezes an immutable byte extent while its source continues growing.
// It never conveys authorization.
type Reader interface {
	Read(context.Context, int, int, int) (string, error)
}

// Owner is private to one provider attempt. Its aggregate resident source bytes
// stay within the run budget; completed output is materialized once for commit.
type Owner struct {
	mu        sync.Mutex
	resident  int
	limit     int
	directory string
	bodies    []*Body
	closed    bool
	create    func(string) (spillFile, error)
}

type spillFile interface {
	io.ReaderAt
	io.WriterAt
	Truncate(int64) error
	Close() error
	Name() string
}

type Body struct {
	mu           sync.Mutex
	owner        *Owner
	text         strings.Builder
	file         spillFile
	path         string
	bytes        int
	materialized bool
	closed       bool
	appendErr    error
	presentation *presentation
}

// presentation shares the unchanged raw prefix. Only bytes following the first
// redaction require a separate source, charged to the same attempt budget.
type presentation struct {
	source *Body
	prefix int
	tail   *Body
}

func (body *Body) Present(original, text string) (Reader, error) {
	owner := body.owner
	owner.mu.Lock()
	body.mu.Lock()
	if body.closed || owner.closed {
		body.mu.Unlock()
		owner.mu.Unlock()
		return nil, os.ErrClosed
	}
	projected := body.presentation
	if projected == nil && original == text {
		body.mu.Unlock()
		owner.mu.Unlock()
		return body, nil
	}
	if projected == nil {
		if len(original) > body.bytes {
			body.mu.Unlock()
			owner.mu.Unlock()
			return nil, os.ErrInvalid
		}
		tail := &Body{owner: owner}
		owner.bodies = append(owner.bodies, tail)
		projected = &presentation{source: body, prefix: body.bytes - len(original), tail: tail}
		body.presentation = projected
	}
	body.mu.Unlock()
	owner.mu.Unlock()
	if err := projected.tail.Append(text); err != nil {
		return nil, err
	}
	return projected, nil
}

func (source *presentation) Read(ctx context.Context, offset, limit, through int) (string, error) {
	if ctx == nil || offset < 0 || through < offset || limit < 4 || limit > ChunkBytes {
		return "", os.ErrInvalid
	}
	if offset < source.prefix {
		return source.source.Read(ctx, offset, limit, min(through, source.prefix))
	}
	return source.tail.Read(ctx, offset-source.prefix, limit, through-source.prefix)
}

func NewOwner() *Owner { return &Owner{limit: MemoryBytes} }

func (owner *Owner) NewBody() *Body {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	body := &Body{owner: owner, closed: owner.closed}
	owner.bodies = append(owner.bodies, body)
	return body
}

func (body *Body) Append(text string) error {
	owner := body.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed || owner.closed {
		return os.ErrClosed
	}
	if body.appendErr != nil {
		return body.appendErr
	}
	if text == "" {
		return nil
	}
	if body.file == nil && owner.resident+len(text) <= owner.limit {
		body.text.WriteString(text)
		body.bytes += len(text)
		owner.resident += len(text)
		body.materialized = false
		return nil
	}
	if body.file == nil {
		create := owner.create
		if create == nil {
			create = func(directory string) (spillFile, error) { return os.CreateTemp(directory, "floret-output-*") }
		}
		file, err := create(owner.directory)
		if err != nil {
			body.appendErr = err
			return err
		}
		if err := writeText(file, body.text.String(), 0); err != nil {
			body.file, body.path = file, file.Name()
			body.appendErr = errors.Join(err, body.closeFile())
			return body.appendErr
		}
		body.file, body.path = file, file.Name()
		owner.resident -= body.text.Len()
		body.text.Reset()
	}
	if err := writeText(body.file, text, body.bytes); err != nil {
		body.appendErr = errors.Join(err, body.file.Truncate(int64(body.bytes)))
		return body.appendErr
	}
	body.bytes += len(text)
	body.materialized = false
	return nil
}

func writeText(writer io.WriterAt, text string, offset int) error {
	for len(text) > 0 {
		part := text[:min(len(text), ChunkBytes)]
		n, err := writer.WriteAt([]byte(part), int64(offset))
		if err != nil {
			return err
		}
		if n != len(part) {
			return io.ErrShortWrite
		}
		text = text[n:]
		offset += n
	}
	return nil
}

// Read returns complete UTF-8 boundaries and never retains a large source
// through a small result. CRLF and source byte offsets remain unchanged.
func (body *Body) Read(ctx context.Context, offset, limit, through int) (string, error) {
	if ctx == nil || offset < 0 || limit < utf8.UTFMax || limit > ChunkBytes || through < offset {
		return "", os.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return "", os.ErrClosed
	}
	if through > body.bytes {
		return "", os.ErrInvalid
	}
	end := offset + min(through-offset, limit)
	if offset == end {
		return "", nil
	}
	var text string
	if body.file == nil {
		text = strings.Clone(body.text.String()[offset:end])
	} else {
		buffer := make([]byte, end-offset)
		if _, err := body.file.ReadAt(buffer, int64(offset)); err != nil {
			return "", err
		}
		text = string(buffer)
	}
	if !utf8.RuneStart(text[0]) {
		return "", os.ErrInvalid
	}
	if end < through {
		// Only the final rune may straddle the requested boundary. Never
		// conceal malformed source bytes by trimming an arbitrary suffix.
		start := len(text) - 1
		for start > 0 && !utf8.RuneStart(text[start]) {
			start--
		}
		if !utf8.FullRuneInString(text[start:]) {
			text = text[:start]
		}
	}
	if !utf8.ValidString(text) {
		return "", os.ErrInvalid
	}
	return text, nil
}

// Materialize snapshots complete output for its existing canonical commit.
// Presentation may still read this immutable source until its actor replaces it.
func (body *Body) Materialize() (text string, err error) {
	// A sanitized tail must survive the same brief canonical-install boundary
	// as raw output. Do this outside the source locks to retain one lock order.
	defer func() {
		body.mu.Lock()
		projected := body.presentation
		body.mu.Unlock()
		if projected != nil {
			_, tailErr := projected.tail.Materialize()
			err = errors.Join(err, tailErr)
		}
	}()
	owner := body.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return "", os.ErrClosed
	}
	if body.file != nil {
		// Commit still requires a complete immutable string. Fill its builder
		// directly instead of allocating another body-sized byte slice first.
		body.text.Grow(body.bytes)
		reader := io.NewSectionReader(body.file, 0, int64(body.bytes))
		n, err := io.CopyBuffer(&body.text, reader, make([]byte, ChunkBytes))
		if err == nil && n != int64(body.bytes) {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			body.text.Reset()
			return "", err
		}
		owner.resident += body.bytes
		body.materialized = true
		if err := body.closeFile(); err != nil {
			return body.text.String(), err
		}
	}
	body.materialized = true
	return body.text.String(), nil
}

func (body *Body) closeFile() error {
	var err error
	if body.file != nil {
		err = body.file.Close()
		body.file = nil
	}
	if body.path == "" {
		return err
	}
	removeErr := os.Remove(body.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if removeErr == nil {
		body.path = ""
	}
	return errors.Join(err, removeErr)
}

// Close releases unfinished spill data on every attempt exit. A materialized
// source remains readable for the short canonical-installation boundary.
func (owner *Owner) Close() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.closed = true
	var err error
	var unreleased []*Body
	for _, body := range owner.bodies {
		body.mu.Lock()
		err = errors.Join(err, body.closeFile())
		if !body.materialized {
			body.text.Reset()
			body.closed = true
		}
		if body.path != "" {
			unreleased = append(unreleased, body)
		}
		body.mu.Unlock()
	}
	owner.resident, owner.bodies = 0, unreleased
	return err
}
