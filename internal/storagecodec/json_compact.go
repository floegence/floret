package storagecodec

import (
	"bufio"
	"io"
)

// WriteCompactJSON writes the encoding/json representation of a RawMessage
// without buffering the value. The caller must already have validated JSON
// syntax. Like json.Marshal, it removes external whitespace and escapes HTML
// characters and literal U+2028/U+2029 inside strings. Existing escapes remain
// byte-exact, as required by the released recovery-journal checksum.
func WriteCompactJSON(destination io.Writer, source io.Reader) error {
	input := bufio.NewReaderSize(source, 32<<10)
	output := bufio.NewWriterSize(destination, 32<<10)
	quoted, escaped := false, false
	for {
		b, err := input.ReadByte()
		if err == io.EOF {
			return output.Flush()
		}
		if err != nil {
			return err
		}
		if !quoted {
			switch b {
			case ' ', '\n', '\r', '\t':
				continue
			}
			if b == '"' {
				quoted = true
			}
		} else if escaped {
			escaped = false
		} else {
			switch b {
			case '\\':
				escaped = true
			case '"':
				quoted = false
			case '<', '>', '&':
				escape := []byte{'\\', 'u', '0', '0', "0123456789abcdef"[b>>4], "0123456789abcdef"[b&15]}
				if _, err := output.Write(escape); err != nil {
					return err
				}
				continue
			case 0xe2:
				if next, _ := input.Peek(2); len(next) == 2 && next[0] == 0x80 && (next[1] == 0xa8 || next[1] == 0xa9) {
					escape := "\\u2028"
					if next[1] == 0xa9 {
						escape = "\\u2029"
					}
					if _, err := input.Discard(2); err != nil {
						return err
					}
					if _, err := output.WriteString(escape); err != nil {
						return err
					}
					continue
				}
			}
		}
		if err := output.WriteByte(b); err != nil {
			return err
		}
	}
}
