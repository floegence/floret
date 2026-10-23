package storagecodec

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
)

// DecodeJSON decodes an already resident value without copying it into a stream
// buffer. V1 options preserve the released storage semantics; unknown fields and
// trailing values remain errors.
func DecodeJSON(raw []byte, target any) error {
	return jsonv2.Unmarshal(raw, target, json.DefaultOptionsV1(), jsonv2.RejectUnknownMembers(true))
}
