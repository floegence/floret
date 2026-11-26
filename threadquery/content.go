package threadquery

// ContentRange addresses UTF-8 bytes in [StartByte, EndByte). The source is
// deterministic and grants no authority. No page handle is allocated.
type ContentRange struct {
	Source    string `json:"source"`
	StartByte int64  `json:"start_byte"`
	EndByte   int64  `json:"end_byte"`
}

type ContentField struct {
	Field string       `json:"field"`
	Read  ContentRange `json:"read"`
}

type ContentFields []ContentField

type ContentPage struct {
	Text     string        `json:"text"`
	NextRead *ContentRange `json:"next_read"`
}
