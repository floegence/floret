package storage

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

var promptKinds = [promptCategoryCount]string{"provider_segment", "provider_tools", "provider_request", "provider_response"}
var promptParts = [promptCategoryCount][]string{{"raw", "message"}, {"tools", "hosted_tools"}, {"turn_surface", "segment_ids"}, {}}

func visitPromptCategory(tx spi.ReadTx, scope string, category int, first uint64, visit func(promptRecord, int) error) error {
	if journal := member.From(tx); journal != nil {
		count, err := journal.PromptThrough(scope, promptKinds[category])
		if err != nil {
			return err
		}
		for ordinal := first; ordinal <= count; ordinal++ {
			record, err := readMemberPrompt(journal, scope, category, ordinal)
			if err != nil {
				return err
			}
			if err := visit(record, len(record.Value)); err != nil {
				return err
			}
		}
		return nil
	}
	prefix := promptRecordPrefix(scope, category)
	start := prefix
	if first > 1 {
		start = promptRecordKey(promptRecord{ScopeID: scope, Category: category, Ordinal: first})
	}
	return scanPromptRecords(tx, start, prefixEnd(prefix), func(row spi.Record) error {
		var record promptRecord
		if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil {
			return err
		}
		if record.Ordinal != first || record.ScopeID != scope || record.Category != category || !bytes.Equal(promptRecordKey(record), row.Key) {
			return sessiontree.ErrAuthorityCorrupt
		}
		first++
		return visit(record, len(row.Value))
	})
}

type promptPayload struct {
	Inline json.RawMessage           `json:"facts,omitempty"`
	Facts  *member.BodyRef           `json:"facts_ref,omitempty"`
	Parts  map[string]member.BodyRef `json:"parts,omitempty"`
	Lookup *promptSelector           `json:"lookup,omitempty"`
}

type promptSelector struct {
	Provider     string `json:"provider,omitempty"`
	Model        string `json:"model,omitempty"`
	Request      string `json:"request,omitempty"`
	Attempt      string `json:"attempt,omitempty"`
	ContextEntry string `json:"context_entry,omitempty"`
	Logical      string `json:"logical,omitempty"`
	Epoch        int    `json:"epoch,omitempty"`
	SurfaceTurn  string `json:"surface_turn,omitempty"`
	Pressure     bool   `json:"pressure,omitempty"`
}

func selectPrompt(record promptRecord) (*promptSelector, error) {
	switch record.Category {
	case promptToolset:
		var tools cache.ToolsetSnapshot
		if err := decodePromptJSON(record.Value, &tools); err != nil {
			return nil, err
		}
		return &promptSelector{Provider: tools.Provider, Model: tools.Model}, nil
	case promptRequest:
		var request cache.ProviderRequestRecord
		if err := decodePromptJSON(record.Value, &request); err != nil {
			return nil, err
		}
		return &promptSelector{Provider: request.Provider, Model: request.Model, Request: request.ID, Attempt: request.AttemptID, ContextEntry: request.ModelContextEntryID, Logical: request.LogicalRequestID, Epoch: request.AttemptEpoch, SurfaceTurn: promptSurfaceTurn(request)}, nil
	case promptResponse:
		var response cache.ProviderResponseRecord
		if err := decodePromptJSON(record.Value, &response); err != nil {
			return nil, err
		}
		if anchor := response.PressureAnchor; matchingPressure(anchor, record.ScopeID, "", "") {
			return &promptSelector{Provider: anchor.Provider, Model: anchor.Model, Pressure: true}, nil
		}
	}
	return nil, nil
}

func promptFact(record promptRecord, owner string) (member.PromptFact, error) {
	thread, err := promptRecordAuthor(record)
	if err != nil || thread != owner || record.Ordinal == 0 || record.Ordinal > math.MaxInt64 {
		return member.PromptFact{}, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	var header struct {
		Turn       string    `json:"turn_id"`
		Run        string    `json:"run_id"`
		AuthorTurn string    `json:"created_by_turn_id"`
		AuthorRun  string    `json:"created_by_run_id"`
		At         time.Time `json:"created_at"`
	}
	if err := json.Unmarshal(record.Value, &header); err != nil {
		return member.PromptFact{}, err
	}
	if record.Category == promptSegment || record.Category == promptToolset {
		header.Turn, header.Run = header.AuthorTurn, header.AuthorRun
	}
	var at sql.NullInt64
	if !header.At.IsZero() {
		at = sql.NullInt64{Valid: true, Int64: header.At.UnixNano()}
		if !time.Unix(0, at.Int64).Equal(header.At) {
			return member.PromptFact{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	return member.PromptFact{Scope: record.ScopeID, Kind: promptKinds[record.Category], Number: int64(record.Ordinal), Turn: header.Turn, Run: header.Run, At: at}, nil
}

func appendMemberPrompt(journal *member.Journal, record promptRecord) error {
	fact, err := promptFact(record, journal.Thread())
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(record.Value, &fields); err != nil {
		return err
	}
	payload := promptPayload{}
	payload.Lookup, err = selectPrompt(record)
	if err != nil {
		return err
	}
	storedAt := time.Now().UnixNano()
	for _, name := range promptParts[record.Category] {
		value := fields[name]
		if len(value) < 1024 {
			continue
		}
		if name == "raw" {
			var raw string
			if err := json.Unmarshal(value, &raw); err != nil {
				return err
			}
			value = []byte(raw)
		}
		ref, err := journal.PutBody(fact.Kind+":"+name, storedAt, value)
		if err != nil {
			return err
		}
		if payload.Parts == nil {
			payload.Parts = make(map[string]member.BodyRef)
		}
		payload.Parts[name] = ref
		delete(fields, name)
	}
	remaining, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if len(remaining) > 8192 {
		ref, err := journal.PutBody(fact.Kind+":facts", storedAt, remaining)
		if err != nil {
			return err
		}
		payload.Facts = &ref
	} else {
		payload.Inline = remaining
	}
	fact.Payload, err = json.Marshal(payload)
	if err != nil {
		return err
	}
	return journal.AppendPrompt(fact)
}

func readMemberPrompt(journal *member.Journal, scope string, category int, ordinal uint64) (promptRecord, error) {
	record := promptRecord{ScopeID: scope, Category: category, Ordinal: ordinal}
	if category < 0 || category >= promptCategoryCount || ordinal == 0 || ordinal > math.MaxInt64 {
		return record, spi.ErrInvalidArgument
	}
	fact, err := journal.Prompt(scope, promptKinds[category], int64(ordinal))
	if err != nil {
		return record, err
	}
	var payload promptPayload
	if err := decodePromptJSON(fact.Payload, &payload); err != nil || (payload.Facts == nil) != (len(payload.Inline) != 0) {
		return record, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	read := func(name string, ref member.BodyRef) ([]byte, error) {
		var buf bytes.Buffer
		err := journal.CopyBody(fact.Kind+":"+name, ref, &buf)
		return buf.Bytes(), err
	}
	if payload.Facts != nil {
		payload.Inline, err = read("facts", *payload.Facts)
		if err != nil {
			return record, err
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload.Inline, &fields); err != nil || fields == nil {
		return record, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	for _, name := range promptParts[category] {
		ref, found := payload.Parts[name]
		if !found {
			continue
		}
		if _, exists := fields[name]; exists {
			return record, sessiontree.ErrAuthorityCorrupt
		}
		value, err := read(name, ref)
		if err != nil {
			return record, err
		}
		if name == "raw" {
			value, err = json.Marshal(string(value))
			if err != nil {
				return record, err
			}
		}
		fields[name] = value
		delete(payload.Parts, name)
	}
	if len(payload.Parts) != 0 {
		return record, sessiontree.ErrAuthorityCorrupt
	}
	record.Value, err = json.Marshal(fields)
	if err != nil {
		return record, err
	}
	header, err := promptFact(record, journal.Thread())
	if err != nil || header.Turn != fact.Turn || header.Run != fact.Run || header.At != fact.At {
		return record, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	want, err := selectPrompt(record)
	if err != nil || !reflect.DeepEqual(want, payload.Lookup) {
		return record, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return record, nil
}

func deleteMemberPromptScope(journal *member.Journal, scope string) error {
	type ownedBody struct {
		class string
		ref   member.BodyRef
	}
	orphans := map[int64]ownedBody{}
	found := false
	check := func(others bool) error {
		return journal.VisitPromptPayloads(scope, others, func(kind string, raw []byte) error {
			var payload promptPayload
			if err := decodePromptJSON(raw, &payload); err != nil {
				return err
			}
			category := slices.Index(promptKinds[:], kind)
			if category < 0 {
				return sessiontree.ErrAuthorityCorrupt
			}
			if !others {
				found = true
			}
			accept := func(part string, ref member.BodyRef) error {
				body := ownedBody{kind + ":" + part, ref}
				if existing, ok := orphans[ref.Object]; ok && existing != body {
					return sessiontree.ErrAuthorityCorrupt
				}
				if others {
					delete(orphans, ref.Object)
				} else {
					orphans[ref.Object] = body
				}
				return nil
			}
			for name, ref := range payload.Parts {
				if !slices.Contains(promptParts[category], name) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err := accept(name, ref); err != nil {
					return err
				}
			}
			if payload.Facts != nil {
				return accept("facts", *payload.Facts)
			}
			return nil
		})
	}
	if err := check(false); err != nil || !found {
		return err
	}
	if len(orphans) != 0 {
		if err := check(true); err != nil {
			return err
		}
	}
	if err := journal.RemovePromptScope(scope); err != nil {
		return err
	}
	for _, body := range orphans {
		if err := journal.RemoveBody(body.class, body.ref); err != nil {
			return err
		}
	}
	return nil
}
