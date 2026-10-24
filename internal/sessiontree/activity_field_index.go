package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/activityfields"
)

// This is source-location metadata, not a copy of the activity or its text.
// A string block starts and ends at a JSON escape/UTF-8 boundary, so one decoded
// range never requires parsing an earlier prefix or the other activity fields.
type activityTextPart struct {
	Offset, EncodedBytes, Bytes int
	DecodedOffset               int
	SHA256                      string
}
type activityFieldNode struct {
	TextExtentVersion           int
	ShapeOnly                   bool
	Scalar                      json.RawMessage
	Kind                        string
	Offset, EncodedBytes, Bytes int
	SHA256                      string
	TrimOffset, TrimBytes       int
	Count, PartCount            int
	Children                    []string
	Parts                       []activityTextPart
}
type activityJSONIndexer struct {
	ctx      context.Context
	source   string
	position int
	nodes    map[string]activityFieldNode
	summary  bool
}

func activityPointer(parent, key string) string {
	return parent + "/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
}
func indexActivityJSON(ctx context.Context, source string) (map[string]activityFieldNode, error) {
	return indexActivityJSONMode(ctx, source, false)
}

// Historical summary preparation validates the complete source but retains no
// hidden detail nodes or text-part hashes. New canonical writes still create
// the complete locator index in their append transaction.
func indexActivitySummaryJSON(ctx context.Context, source string) (map[string]activityFieldNode, error) {
	return indexActivityJSONMode(ctx, source, true)
}
func indexActivityJSONMode(ctx context.Context, source string, summary bool) (map[string]activityFieldNode, error) {
	if ctx == nil {
		return nil, ErrAuthorityCorrupt
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !json.Valid([]byte(source)) {
		return nil, ErrAuthorityCorrupt
	}
	parser := activityJSONIndexer{ctx: ctx, source: source, nodes: map[string]activityFieldNode{}, summary: summary}
	if err := parser.value("", 0); err != nil {
		return nil, err
	}
	parser.space()
	if parser.position != len(source) {
		return nil, ErrAuthorityCorrupt
	}
	return parser.nodes, nil
}
func (p *activityJSONIndexer) space() {
	for p.position < len(p.source) && strings.ContainsRune(" \r\n\t", rune(p.source[p.position])) {
		p.position++
	}
}
func (p *activityJSONIndexer) value(path string, depth int) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if depth > 64 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("activity nesting exceeds typed contract"))
	}
	p.space()
	start := p.position
	if start >= len(p.source) {
		return ErrAuthorityCorrupt
	}
	node := activityFieldNode{Offset: start}
	keep := !p.summary || path == "" || path == "/payload" || strings.Count(path, "/") == 1 || strings.HasPrefix(path, "/payload/") && strings.Count(path, "/") == 2 || strings.HasPrefix(path, "/chips/")
	keepChildren := !p.summary || path == "" || path == "/payload" || path == "/chips" || strings.HasPrefix(path, "/chips/")
	switch p.source[p.position] {
	case '{', '[':
		open := p.source[p.position]
		close := byte('}')
		node.Kind = "object"
		if open == '[' {
			close = ']'
			node.Kind = "array"
		}
		p.position++
		p.space()
		seen := map[string]bool{}
		for p.position < len(p.source) && p.source[p.position] != close {
			key := strconv.Itoa(node.Count)
			if open == '{' {
				keyStart := p.position
				if _, err := p.stringNodeMode(false); err != nil {
					return err
				}
				if err := json.Unmarshal([]byte(p.source[keyStart:p.position]), &key); err != nil {
					return ErrAuthorityCorrupt
				}
				p.space()
				if p.position >= len(p.source) || p.source[p.position] != ':' {
					return ErrAuthorityCorrupt
				}
				p.position++
			}
			child := activityPointer(path, key)
			if open == '{' {
				if seen[key] {
					return errors.Join(ErrAuthorityCorrupt, errors.New("duplicate activity field"))
				}
				seen[key] = true
			}
			if err := p.value(child, depth+1); err != nil {
				return err
			}
			node.Count++
			if keepChildren {
				node.Children = append(node.Children, key)
			}
			p.space()
			if p.position < len(p.source) && p.source[p.position] == ',' {
				p.position++
				p.space()
				continue
			}
			break
		}
		if p.position >= len(p.source) || p.source[p.position] != close {
			return ErrAuthorityCorrupt
		}
		p.position++
	case '"':
		var err error
		node, err = p.stringNodeMode(!p.summary || activityfields.SummaryText(path))
		if err != nil {
			return err
		}
	default:
		node.Kind = "scalar"
		for p.position < len(p.source) && !strings.ContainsRune(",}] \r\n\t", rune(p.source[p.position])) {
			p.position++
		}
		if p.position == start {
			return ErrAuthorityCorrupt
		}
	}
	node.EncodedBytes = p.position - start
	node.PartCount = len(node.Parts)
	if p.summary && activitySummaryPath(path) && !activityfields.SummaryText(path) {
		node.ShapeOnly = true
	}
	if node.Kind == "scalar" {
		node.Scalar = json.RawMessage(p.source[start:p.position])
	}
	if keep {
		p.nodes[path] = node
	}
	return nil
}
func (p *activityJSONIndexer) stringNodeMode(details bool) (activityFieldNode, error) {
	start := p.position
	node := activityFieldNode{Kind: "string", Offset: start}
	if start >= len(p.source) || p.source[start] != '"' {
		return node, ErrAuthorityCorrupt
	}
	p.position++
	partStart := p.position
	decoded := make([]byte, 0, canonicalBodyBlockBytes)
	whole := sha256.New()
	total := 0
	firstNonSpace, lastNonSpace := -1, 0
	flush := func() {
		if len(decoded) == 0 {
			return
		}
		if details {
			node.Parts = append(node.Parts, activityTextPart{DecodedOffset: total, Offset: partStart, EncodedBytes: p.position - partStart, Bytes: len(decoded), SHA256: stableHash(string(decoded))})
			whole.Write(decoded)
		}
		total += len(decoded)
		decoded = decoded[:0]
		partStart = p.position
	}
	for p.position < len(p.source) && p.source[p.position] != '"' {
		if err := p.ctx.Err(); err != nil {
			return node, err
		}
		tokenStart := p.position
		var token string
		if p.source[p.position] != '\\' {
			r, size := utf8.DecodeRuneInString(p.source[p.position:])
			if r == utf8.RuneError && size == 1 {
				return node, ErrAuthorityCorrupt
			}
			token = p.source[p.position : p.position+size]
			p.position += size
		} else {
			p.position += 2
			if p.position > len(p.source) {
				return node, ErrAuthorityCorrupt
			}
			switch p.source[p.position-1] {
			case 'u':
				if p.position+4 > len(p.source) {
					return node, ErrAuthorityCorrupt
				}
				n, err := strconv.ParseUint(p.source[p.position:p.position+4], 16, 16)
				if err != nil {
					return node, ErrAuthorityCorrupt
				}
				p.position += 4
				r := rune(n)
				if utf16.IsSurrogate(r) {
					if p.position+6 > len(p.source) || p.source[p.position:p.position+2] != "\\u" {
						return node, ErrAuthorityCorrupt
					}
					next, err := strconv.ParseUint(p.source[p.position+2:p.position+6], 16, 16)
					if err != nil {
						return node, ErrAuthorityCorrupt
					}
					r = utf16.DecodeRune(r, rune(next))
					if r == utf8.RuneError {
						return node, ErrAuthorityCorrupt
					}
					p.position += 6
				}
				token = string(r)
			default:
				var text string
				if err := json.Unmarshal([]byte("\""+p.source[tokenStart:p.position]+"\""), &text); err != nil {
					return node, ErrAuthorityCorrupt
				}
				token = text
			}
		}
		if len(decoded)+len(token) > canonicalBodyBlockBytes {
			end := p.position
			p.position = tokenStart
			flush()
			p.position = end
		}
		position := total + len(decoded)
		r, _ := utf8.DecodeRuneInString(token)
		if !unicode.IsSpace(r) {
			if firstNonSpace < 0 {
				firstNonSpace = position
			}
			lastNonSpace = position + len(token)
		}
		decoded = append(decoded, token...)
	}
	if p.position >= len(p.source) {
		return node, fmt.Errorf("%w: unterminated activity string", ErrAuthorityCorrupt)
	}
	flush()
	p.position++
	node.Bytes = total
	node.TextExtentVersion = 1
	node.TrimOffset = total
	if firstNonSpace >= 0 {
		node.TrimOffset, node.TrimBytes = firstNonSpace, lastNonSpace-firstNonSpace
	}
	if details {
		node.SHA256 = hex.EncodeToString(whole.Sum(nil))
	}
	node.EncodedBytes = p.position - start
	return node, nil
}
