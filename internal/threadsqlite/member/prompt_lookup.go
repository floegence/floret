package member

import "database/sql"

func (journal *Journal) PromptThrough(scope, kind string) (uint64, error) {
	return journal.aggregatePrompt("max(prompt_no)", "prompt_scope=? AND kind=? AND prompt_no IS NOT NULL", scope, kind)
}

func (journal *Journal) aggregatePrompt(aggregate, condition string, args ...any) (uint64, error) {
	var number sql.NullInt64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT `+aggregate+` FROM `+journal.database+`history_events WHERE `+condition, args...).Scan(&number)
	if err != nil || number.Int64 < 0 {
		if err != nil {
			return 0, err
		}
		return 0, ErrContentIntegrity
	}
	return uint64(number.Int64), nil
}

const promptProvider = `coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.provider'),'')`
const promptModel = `coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.model'),'')`
const promptRequestFilter = `kind='provider_request' AND prompt_scope=?`

func (journal *Journal) TurnPrompts(scope, turn string) (surface, model, conflict uint64, err error) {
	surface, err = journal.aggregatePrompt("min(prompt_no)", promptRequestFilter+` AND coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.surface_turn'),'')=?`, scope, turn)
	if err != nil {
		return
	}
	byTurn := promptRequestFilter + ` AND turn_id IS nullif(?,'')`
	model, err = journal.aggregatePrompt("min(prompt_no)", byTurn, scope, turn)
	if err != nil || model == 0 {
		return
	}
	var provider, modelName string
	err = journal.tx.QueryRowContext(journal.ctx, `SELECT `+promptProvider+`,`+promptModel+` FROM `+journal.database+`history_events WHERE `+promptRequestFilter+` AND prompt_no=?`, scope, model).Scan(&provider, &modelName)
	if err != nil {
		return
	}
	conflict, err = journal.aggregatePrompt("min(prompt_no)", byTurn+` AND (`+promptProvider+`<>? OR `+promptModel+`<>?)`, scope, turn, provider, modelName)
	return
}

func (journal *Journal) RequestEpoch(scope, turn, run, logical string) (uint64, error) {
	return journal.aggregatePrompt(`max(max(0,coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.epoch'),0)))`, promptRequestFilter+` AND turn_id IS nullif(?,'') AND run_id IS nullif(?,'') AND coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.logical'),'')=?`, scope, turn, run, logical)
}

func (journal *Journal) ToolsetOrdinal(scope, provider, model string) (uint64, error) {
	return journal.aggregatePrompt("max(prompt_no)", `kind='provider_tools' AND prompt_scope=? AND `+promptProvider+`=? AND `+promptModel+`=?`, scope, provider, model)
}

func (journal *Journal) PressureOrdinal(scope, provider, model string) (uint64, error) {
	return journal.aggregatePrompt("max(prompt_no)", `kind='provider_response' AND prompt_scope=? AND json_extract(CAST(payload AS TEXT),'$.lookup.pressure')=1 AND (?='' OR `+promptProvider+`=?) AND (?='' OR `+promptModel+`=?)`, scope, provider, provider, model, model)
}

func (journal *Journal) RequestOrdinal(scope, request, attempt string, measured bool) (uint64, error) {
	return journal.aggregatePrompt("max(prompt_no)", promptRequestFilter+` AND coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.request'),'')=? AND (NOT ? OR coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.attempt'),'')=?)`, scope, request, measured, attempt)
}

func (journal *Journal) ContextOrdinal(scope, thread, entry string) (uint64, error) {
	if thread != journal.Thread() {
		return 0, nil
	}
	return journal.aggregatePrompt("max(prompt_no)", promptRequestFilter+` AND coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.context_entry'),'')=?`, scope, entry)
}
