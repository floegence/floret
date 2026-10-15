package engine

import (
	"unicode/utf8"

	"github.com/floegence/floret/v7/tools"
)

// Output estimation is independent of billable Usage and scans only new bytes.
// Separate UTF-8 tails prevent interleaved calls from joining partial runes.
type outputEstimate struct {
	quarters int64
	tails    map[string]string
}

func (e *outputEstimate) append(channel, delta string) {
	if e.tails == nil {
		e.tails = make(map[string]string)
	}
	text := e.tails[channel] + delta
	delete(e.tails, channel)
	for len(text) > 0 {
		if !utf8.FullRuneInString(text) {
			e.tails[channel] = text
			break
		}
		r, n := utf8.DecodeRuneInString(text)
		if r <= 127 {
			e.quarters++
		} else {
			e.quarters += 6
		}
		text = text[n:]
	}
}

func (e *outputEstimate) tokens() int64 { return (e.quarters + 3) / 4 }

type argumentPreviewCall struct {
	parser   tools.ArgumentPreviewer
	bytes    int
	complete bool
	disabled bool
}

func newArgumentPreviewCall(registry *tools.Registry, name string) (call *argumentPreviewCall) {
	call = &argumentPreviewCall{}
	defer func() {
		if recover() != nil {
			call.disabled = true
		}
	}()
	if registry != nil {
		if definition, ok := registry.Definition(name); ok && definition.NewArgumentPreview != nil {
			call.parser = definition.NewArgumentPreview()
		}
	}
	return call
}

func (call *argumentPreviewCall) append(delta string, complete bool) (preview *tools.ArgumentPreview, updated bool, failed bool) {
	call.bytes += len(delta)
	if call.disabled {
		return nil, true, true
	}
	if call.parser == nil || call.complete {
		return nil, false, false
	}
	defer func() {
		if recover() != nil {
			call.disabled = true
			call.parser = nil
			preview, updated, failed = nil, true, true
		}
	}()
	var err error
	preview, err = call.parser(delta, complete)
	if err != nil {
		call.disabled = true
		call.parser = nil
		return nil, true, true
	}
	call.complete = complete
	return preview.Clone(), true, false
}
