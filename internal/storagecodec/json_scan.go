package storagecodec

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// JSONSpan identifies bytes in an immutable record. It contains no body data.
type JSONSpan struct {
	Offset int64
	Length int64
}

// JSONScanFrame is the grammar state of one open container. Keys are kept only
// at the indexed depth; arbitrarily large string values are never buffered.
type JSONScanFrame struct {
	Kind  byte
	State uint8
	Start int64
	Key   string
	Index int
	Path  []string
}

// JSONScanState can be checkpointed together with discovered source locators.
// Offset always points at the next unread byte, including inside large values.
type JSONScanState struct {
	Offset     int64
	Frames     []JSONScanFrame
	Token      byte
	TokenStart int64
	TokenPath  []string
	TokenKey   bool
	Escape     bool
	Unicode    int
	Number     uint8
	Literal    string
	LiteralAt  int
	Started    bool
	Done       bool
}

// ScanJSON advances at most budget source bytes. source starts at state.Offset;
// readSpan supplies bounded source ranges for small indexed object keys. Values
// deeper than depth are syntax-checked without generating index records.
func ScanJSON(source io.Reader, state *JSONScanState, budget int64, depth int, readSpan func(JSONSpan) ([]byte, error), visit func([]string, JSONSpan, byte) error) error {
	if source == nil || state == nil || budget <= 0 || depth < 0 || readSpan == nil || visit == nil {
		return errors.New("JSON scan requires source, checkpoint, budget and visitors")
	}
	if err := state.Validate(depth); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(io.LimitReader(source, budget), 32<<10)
	consumed := int64(0)
	invalid := func() error { return fmt.Errorf("invalid JSON at source offset %d", state.Offset) }
	finish := func(kind byte, start int64, path []string) error {
		if path != nil && len(path) <= depth {
			if err := visit(path, JSONSpan{Offset: start, Length: state.Offset - start}, kind); err != nil {
				return err
			}
		}
		if len(state.Frames) == 0 {
			state.Done = true
			return nil
		}
		frame := &state.Frames[len(state.Frames)-1]
		if frame.Kind == '{' {
			frame.State = 3
		} else {
			frame.State = 1
			frame.Index++
		}
		return nil
	}
	for consumed < budget {
		value, err := reader.ReadByte()
		if errors.Is(err, io.EOF) {
			// Reaching the real source end is distinct from consuming this batch.
			if state.Token == 'n' && jsonNumberComplete(state.Number) {
				if err := finish('n', state.TokenStart, state.TokenPath); err != nil {
					return err
				}
				state.Token = 0
			}
			if state.Done && state.Token == 0 && len(state.Frames) == 0 {
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		reprocess := true
		for reprocess {
			reprocess = false
			if state.Token != 0 {
				switch state.Token {
				case 's':
					if state.Unicode > 0 {
						if !jsonHex(value) {
							return invalid()
						}
						state.Unicode--
					} else if state.Escape {
						state.Escape = false
						switch value {
						case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
						case 'u':
							state.Unicode = 4
						default:
							return invalid()
						}
					} else {
						switch {
						case value == '\\':
							state.Escape = true
						case value == '"':
							state.Offset++
							consumed++
							if state.TokenKey {
								frame := &state.Frames[len(state.Frames)-1]
								if frame.Path != nil && len(frame.Path) < depth {
									raw, err := readSpan(JSONSpan{Offset: state.TokenStart, Length: state.Offset - state.TokenStart})
									if err != nil {
										return err
									}
									if err := json.Unmarshal(raw, &frame.Key); err != nil {
										return err
									}
								}
								frame.State = 1
							} else if err := finish('s', state.TokenStart, state.TokenPath); err != nil {
								return err
							}
							state.Token = 0
							continue
						case value < 0x20:
							return invalid()
						}
					}
				case 'l':
					if state.LiteralAt >= len(state.Literal) || value != state.Literal[state.LiteralAt] {
						return invalid()
					}
					state.LiteralAt++
					if state.LiteralAt == len(state.Literal) {
						state.Offset++
						consumed++
						if err := finish('l', state.TokenStart, state.TokenPath); err != nil {
							return err
						}
						state.Token = 0
						continue
					}
				case 'n':
					next, valid := jsonNumberNext(state.Number, value)
					if !valid {
						if !jsonNumberComplete(state.Number) {
							return invalid()
						}
						if err := finish('n', state.TokenStart, state.TokenPath); err != nil {
							return err
						}
						state.Token = 0
						reprocess = true
						continue
					}
					state.Number = next
				default:
					return invalid()
				}
				state.Offset++
				consumed++
				continue
			}
			if jsonSpace(value) {
				state.Offset++
				consumed++
				continue
			}
			if state.Done {
				return invalid()
			}
			path := []string{}
			if len(state.Frames) > 0 {
				frame := &state.Frames[len(state.Frames)-1]
				if frame.Kind == '{' {
					switch frame.State {
					case 0, 4:
						if value == '}' && frame.State == 0 {
							state.Offset++
							consumed++
							done := *frame
							state.Frames = state.Frames[:len(state.Frames)-1]
							if err := finish('{', done.Start, done.Path); err != nil {
								return err
							}
							continue
						}
						if value != '"' {
							return invalid()
						}
						state.Token, state.TokenStart, state.TokenKey = 's', state.Offset, true
						state.Offset++
						consumed++
						continue
					case 1:
						if value != ':' {
							return invalid()
						}
						frame.State = 2
						state.Offset++
						consumed++
						continue
					case 3:
						if value == ',' {
							frame.State = 4
							state.Offset++
							consumed++
							continue
						}
						if value != '}' {
							return invalid()
						}
						state.Offset++
						consumed++
						done := *frame
						state.Frames = state.Frames[:len(state.Frames)-1]
						if err := finish('{', done.Start, done.Path); err != nil {
							return err
						}
						continue
					case 2:
					default:
						return invalid()
					}
					if frame.Path == nil || len(frame.Path) >= depth {
						path = nil
					} else {
						path = append(append([]string{}, frame.Path...), frame.Key)
					}
				} else if frame.Kind == '[' {
					switch frame.State {
					case 0, 2:
						if value == ']' && frame.State == 0 {
							state.Offset++
							consumed++
							done := *frame
							state.Frames = state.Frames[:len(state.Frames)-1]
							if err := finish('[', done.Start, done.Path); err != nil {
								return err
							}
							continue
						}
					case 1:
						if value == ',' {
							frame.State = 2
							state.Offset++
							consumed++
							continue
						}
						if value != ']' {
							return invalid()
						}
						state.Offset++
						consumed++
						done := *frame
						state.Frames = state.Frames[:len(state.Frames)-1]
						if err := finish('[', done.Start, done.Path); err != nil {
							return err
						}
						continue
					default:
						return invalid()
					}
					if frame.Path == nil || len(frame.Path) >= depth {
						path = nil
					} else {
						path = append(append([]string{}, frame.Path...), fmt.Sprint(frame.Index))
					}
				} else {
					return invalid()
				}
			} else if state.Started {
				return invalid()
			}
			state.Started = true
			state.TokenStart, state.TokenPath, state.TokenKey = state.Offset, path, false
			switch value {
			case '{', '[':
				if len(state.Frames) >= 10000 {
					return invalid()
				}
				state.Frames = append(state.Frames, JSONScanFrame{Kind: value, Start: state.Offset, Path: path})
			case '"':
				state.Token = 's'
			case 't':
				state.Token, state.Literal, state.LiteralAt = 'l', "true", 1
			case 'f':
				state.Token, state.Literal, state.LiteralAt = 'l', "false", 1
			case 'n':
				state.Token, state.Literal, state.LiteralAt = 'l', "null", 1
			default:
				next, valid := jsonNumberNext(0, value)
				if !valid {
					return invalid()
				}
				state.Token, state.Number = 'n', next
			}
			state.Offset++
			consumed++
		}
	}
	return nil
}

// Validate rejects impossible persisted parser states before any source bytes
// or locators are consumed. It does not claim to authenticate source content.
func (state JSONScanState) Validate(depth int) error {
	invalid := errors.New("invalid JSON scan checkpoint")
	if depth < 0 || state.Offset < 0 || len(state.Frames) > 10000 || state.Done && (!state.Started || state.Token != 0 || len(state.Frames) != 0) {
		return invalid
	}
	if !state.Started && (state.Done || state.Token != 0 || len(state.Frames) != 0) {
		return invalid
	}
	var previous int64 = -1
	for index, frame := range state.Frames {
		if frame.Start <= previous || frame.Start >= state.Offset || frame.Index < 0 {
			return invalid
		}
		previous = frame.Start
		if frame.Kind != '{' && frame.Kind != '[' || frame.Kind == '{' && (frame.State > 4 || frame.Index != 0) || frame.Kind == '[' && frame.State > 2 {
			return invalid
		}
		if index <= depth {
			if frame.Path == nil || len(frame.Path) != index {
				return invalid
			}
		} else if frame.Path != nil {
			return invalid
		}
		if index+1 < len(state.Frames) && (frame.Kind == '{' && frame.State != 2 || frame.Kind == '[' && frame.State != 0 && frame.State != 2) {
			return invalid
		}
	}
	if state.Token == 0 {
		if state.Started && !state.Done && len(state.Frames) == 0 {
			return invalid
		}
		return nil
	}
	if !state.Started || state.TokenStart < 0 || state.TokenStart >= state.Offset {
		return invalid
	}
	if state.TokenKey {
		if state.Token != 's' || len(state.Frames) == 0 {
			return invalid
		}
		frame := state.Frames[len(state.Frames)-1]
		if frame.Kind != '{' || frame.State != 0 && frame.State != 4 || state.TokenStart <= frame.Start {
			return invalid
		}
	}
	switch state.Token {
	case 's':
		if state.Unicode < 0 || state.Unicode > 4 || state.Escape && state.Unicode != 0 {
			return invalid
		}
	case 'n':
		if state.Number < 1 || state.Number > 8 {
			return invalid
		}
	case 'l':
		if state.Literal != "null" && state.Literal != "true" && state.Literal != "false" || state.LiteralAt < 1 || state.LiteralAt >= len(state.Literal) {
			return invalid
		}
	default:
		return invalid
	}
	return nil
}

func jsonSpace(value byte) bool {
	return value == ' ' || value == '\n' || value == '\r' || value == '\t'
}
func jsonHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}
func jsonNumberComplete(state uint8) bool {
	return state == 2 || state == 3 || state == 5 || state == 8
}
func jsonNumberNext(state uint8, value byte) (uint8, bool) {
	switch state {
	case 0:
		if value == '-' {
			return 1, true
		}
		fallthrough
	case 1:
		if value == '0' {
			return 2, true
		}
		if value >= '1' && value <= '9' {
			return 3, true
		}
	case 2, 3:
		if state == 3 && value >= '0' && value <= '9' {
			return 3, true
		}
		if value == '.' {
			return 4, true
		}
		if value == 'e' || value == 'E' {
			return 6, true
		}
	case 4:
		if value >= '0' && value <= '9' {
			return 5, true
		}
	case 5:
		if value >= '0' && value <= '9' {
			return 5, true
		}
		if value == 'e' || value == 'E' {
			return 6, true
		}
	case 6:
		if value == '+' || value == '-' {
			return 7, true
		}
		fallthrough
	case 7:
		if value >= '0' && value <= '9' {
			return 8, true
		}
	case 8:
		if value >= '0' && value <= '9' {
			return 8, true
		}
	}
	return 0, false
}
