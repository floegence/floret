package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
)

const legacySubAgentTerminalAdmissionSourceV323 = "v3.2.3_subagent_input"

var errHistoricalSubAgentAdmission = errors.New("historical SubAgent input has no live turn authority")

type legacySubAgentTerminalAdmissionProof struct {
	Source     string
	SourceHash string
}

func SubAgentInputAdmissionFingerprint(parentThreadID, childThreadID, turnID, runID, ownerID, inputID string) string {
	return StableHash(strings.Join([]string{
		"subagent-input-admission", strings.TrimSpace(parentThreadID), strings.TrimSpace(childThreadID),
		strings.TrimSpace(turnID), strings.TrimSpace(runID), strings.TrimSpace(ownerID), strings.TrimSpace(inputID),
	}, "\x00"))
}

func migrateMemoryStateV2ToV3(state *memoryState) error {
	if state == nil || state.Version != 2 {
		return errors.New("session-tree v2 migration requires exact version 2 state")
	}
	if err := validateRequiredMemoryStateMaps(*state); err != nil {
		return err
	}
	for childThreadID, inputs := range state.SubAgentInputs {
		for _, input := range inputs {
			if input.State != SubAgentInputAdmitted {
				continue
			}
			key := turnAdmissionKey(childThreadID, input.AdmittedTurnID)
			if _, exists := state.TurnAdmissions[key]; exists {
				continue
			}
			admission, err := reconstructV2SubAgentAdmission(*state, childThreadID, input)
			if errors.Is(err, errHistoricalSubAgentAdmission) {
				continue
			}
			if err != nil {
				return err
			}
			state.TurnAdmissions[key] = admission
		}
	}
	state.Version = 3
	return nil
}

func reconstructV2SubAgentAdmission(state memoryState, childThreadID string, input SubAgentInputRecord) (turnAdmissionLedger, error) {
	started, user, err := subAgentAdmissionJournal(state.Entries, childThreadID, input)
	if err != nil {
		return turnAdmissionLedger{}, err
	}
	meta, exists := state.Threads[childThreadID]
	if !exists || meta.ParentThreadID != input.ParentThreadID {
		return turnAdmissionLedger{}, errors.New("SubAgent input does not match parent-child authority")
	}
	key := turnAdmissionKey(childThreadID, input.AdmittedTurnID)
	active, activeExists := state.Leases[childThreadID]
	activeOK := activeExists && active.Purpose == TurnLeasePurposeTurn && active.TurnID == input.AdmittedTurnID
	finish, finished := state.TurnFinishes[key]
	if !activeOK && !finished {
		return turnAdmissionLedger{}, errHistoricalSubAgentAdmission
	}
	if activeOK == finished {
		return turnAdmissionLedger{}, errors.New("SubAgent admission must have exactly one active or terminal authority")
	}
	admission := turnAdmissionLedger{
		ThreadID: childThreadID, TurnID: input.AdmittedTurnID, RunID: input.AdmittedRunID,
		TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: started.ParentID,
	}
	if activeOK {
		if active.Validate() != nil || active.Purpose != TurnLeasePurposeTurn || active.ThreadID != childThreadID ||
			active.TurnID != input.AdmittedTurnID || state.LeaseGeneration[childThreadID] != active.Generation {
			return turnAdmissionLedger{}, errors.New("SubAgent input does not match its active lease")
		}
		admission.Lease = active
		admission.RequestFingerprint = SubAgentInputAdmissionFingerprint(
			input.ParentThreadID, childThreadID, input.AdmittedTurnID, input.AdmittedRunID, active.OwnerID, input.SubAgentInputID,
		)
		return admission, nil
	}
	if err := validateLegacySubAgentTerminalSource(state, childThreadID, input, finish); err != nil {
		return turnAdmissionLedger{}, err
	}
	sourceHash, err := legacySubAgentTerminalSourceHash(state, childThreadID, input, started, user, finish)
	if err != nil {
		return turnAdmissionLedger{}, err
	}
	admission.RequestFingerprint = "legacy-subagent-terminal:" + sourceHash
	admission.LegacyTerminalProof = &legacySubAgentTerminalAdmissionProof{
		Source: legacySubAgentTerminalAdmissionSourceV323, SourceHash: sourceHash,
	}
	return admission, nil
}

func validateSubAgentAdmissionState(repo *MemoryRepo) error {
	seen := make(map[string]struct{})
	for childThreadID, inputs := range repo.subAgentInputs {
		for _, input := range inputs {
			if input.State != SubAgentInputAdmitted {
				continue
			}
			key := turnAdmissionKey(childThreadID, input.AdmittedTurnID)
			if _, duplicate := seen[key]; duplicate {
				return errors.New("SubAgent turn has multiple admitted inputs")
			}
			seen[key] = struct{}{}
			active, activeExists := repo.leases[childThreadID]
			activeOK := activeExists && active.Purpose == TurnLeasePurposeTurn && active.TurnID == input.AdmittedTurnID
			_, finished := repo.turnFinishes[key]
			admission, exists := repo.turnAdmissions[key]
			if !activeOK && !finished {
				if exists && admission.LegacyTerminalProof != nil {
					return errors.New("historical SubAgent admission retains a terminal migration proof")
				}
				continue
			}
			if !exists {
				return errors.New("live SubAgent input has no turn admission")
			}
			if err := validateSubAgentAdmission(repo, childThreadID, input, admission); err != nil {
				return err
			}
		}
	}
	for key, admission := range repo.turnAdmissions {
		if admission.LegacyTerminalProof != nil {
			if _, exists := seen[key]; !exists {
				return errors.New("legacy SubAgent terminal proof has no admitted input")
			}
		}
	}
	for threadID, lease := range repo.leases {
		meta, exists := repo.threads[threadID]
		if !exists || strings.TrimSpace(meta.ParentThreadID) == "" || lease.Purpose != TurnLeasePurposeTurn {
			continue
		}
		matches := 0
		for _, input := range repo.subAgentInputs[threadID] {
			if input.State == SubAgentInputAdmitted && input.AdmittedTurnID == lease.TurnID {
				matches++
			}
		}
		if matches != 1 {
			return errors.New("active SubAgent lease has no unique admitted input")
		}
	}
	return nil
}

func validateSubAgentAdmission(repo *MemoryRepo, childThreadID string, input SubAgentInputRecord, admission turnAdmissionLedger) error {
	started, user, err := subAgentAdmissionJournal(repo.entries, childThreadID, input)
	if err != nil {
		return err
	}
	meta, exists := repo.threads[childThreadID]
	if !exists || meta.ParentThreadID != input.ParentThreadID || admission.ThreadID != childThreadID ||
		admission.TurnID != input.AdmittedTurnID || admission.RunID != input.AdmittedRunID ||
		admission.TurnStartedID != started.ID || admission.UserMessageID != user.ID || admission.BaseLeafID != started.ParentID {
		return errors.New("SubAgent turn admission does not match its input journal")
	}
	key := turnAdmissionKey(childThreadID, input.AdmittedTurnID)
	active, activeExists := repo.leases[childThreadID]
	activeOK := activeExists && active.Purpose == TurnLeasePurposeTurn && active.TurnID == input.AdmittedTurnID
	finish, finished := repo.turnFinishes[key]
	if activeOK == finished {
		return errors.New("SubAgent admission must have exactly one active or terminal authority")
	}
	if activeOK {
		if admission.LegacyTerminalProof != nil || !SameTurnLease(admission.Lease, active) || active.Validate() != nil ||
			active.Purpose != TurnLeasePurposeTurn || active.ThreadID != childThreadID || active.TurnID != input.AdmittedTurnID ||
			repo.leaseGeneration[childThreadID] != active.Generation ||
			admission.RequestFingerprint != SubAgentInputAdmissionFingerprint(
				input.ParentThreadID, childThreadID, input.AdmittedTurnID, input.AdmittedRunID, active.OwnerID, input.SubAgentInputID,
			) {
			return errors.New("active SubAgent admission authority is invalid")
		}
		return nil
	}
	if admission.LegacyTerminalProof == nil {
		if admission.Lease.Validate() != nil || admission.Lease.ThreadID != childThreadID ||
			admission.Lease.TurnID != input.AdmittedTurnID || admission.Lease.Generation > repo.leaseGeneration[childThreadID] ||
			admission.RequestFingerprint != SubAgentInputAdmissionFingerprint(
				input.ParentThreadID, childThreadID, input.AdmittedTurnID, input.AdmittedRunID, admission.Lease.OwnerID, input.SubAgentInputID,
			) {
			return errors.New("terminal SubAgent admission authority is invalid")
		}
		return validateNormalSubAgentFinish(finish, admission.Lease, repo.entries[childThreadID])
	}
	if admission.Lease != (TurnLease{}) || admission.LegacyTerminalProof.Source != legacySubAgentTerminalAdmissionSourceV323 ||
		strings.TrimSpace(admission.LegacyTerminalProof.SourceHash) == "" {
		return errors.New("legacy SubAgent terminal proof is invalid")
	}
	state := repo.memoryStateLocked()
	if err := validateLegacySubAgentTerminalSource(state, childThreadID, input, finish); err != nil {
		return err
	}
	hash, err := legacySubAgentTerminalSourceHash(state, childThreadID, input, started, user, finish)
	if err != nil || hash != admission.LegacyTerminalProof.SourceHash || admission.RequestFingerprint != "legacy-subagent-terminal:"+hash {
		return errors.New("legacy SubAgent terminal proof does not match its source")
	}
	return nil
}

func subAgentAdmissionJournal(entries map[string][]Entry, childThreadID string, input SubAgentInputRecord) (Entry, Entry, error) {
	if input.ChildThreadID != childThreadID || strings.TrimSpace(input.ParentThreadID) == "" ||
		strings.TrimSpace(input.SubAgentInputID) == "" || strings.TrimSpace(input.AdmittedTurnID) == "" ||
		strings.TrimSpace(input.AdmittedRunID) == "" || input.AdmittedAt.IsZero() {
		return Entry{}, Entry{}, errors.New("admitted SubAgent input authority is incomplete")
	}
	origin, err := SubAgentUserMessageOrigin(input.RequestKind)
	if err != nil {
		return Entry{}, Entry{}, err
	}
	var starts, users []Entry
	for _, entry := range entries[childThreadID] {
		if entry.TurnID != input.AdmittedTurnID {
			continue
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.Metadata["run_id"] == input.AdmittedRunID {
			starts = append(starts, entry)
		}
		if entry.Type == EntryUserMessage && entry.Metadata[SubAgentInputIDMetadataKey] == input.SubAgentInputID {
			users = append(users, entry)
		}
	}
	if len(starts) != 1 || len(users) != 1 {
		return Entry{}, Entry{}, errors.New("SubAgent admission journal is not unique")
	}
	started, user := starts[0], users[0]
	if ValidateEntryIntegrity(started) != nil || ValidateEntryIntegrity(user) != nil || user.ParentID != started.ID ||
		user.Metadata[SubAgentUserMessageOriginMetadataKey] != origin || !reflect.DeepEqual(user.Message, input.Message) {
		return Entry{}, Entry{}, errors.New("SubAgent admission journal does not match its input")
	}
	return started, user, nil
}

func validateLegacySubAgentTerminalSource(state memoryState, childThreadID string, input SubAgentInputRecord, finish turnFinishLedger) error {
	if finish.ThreadID != childThreadID || finish.TurnID != input.AdmittedTurnID || finish.RunID != input.AdmittedRunID ||
		finish.Generation <= 0 || finish.Generation > state.LeaseGeneration[childThreadID] ||
		strings.TrimSpace(finish.OutcomeFingerprint) == "" || strings.TrimSpace(finish.TerminalEntryID) == "" {
		return errors.New("SubAgent terminal finish authority is invalid")
	}
	terminal, found := findEntry(state.Entries[childThreadID], finish.TerminalEntryID)
	if !found || ValidateEntryIntegrity(terminal) != nil || terminal.ThreadID != childThreadID ||
		terminal.TurnID != input.AdmittedTurnID || terminal.Type != EntryTurnMarker || !terminalTurnMarker(terminal.TurnStatus) {
		return errors.New("SubAgent terminal journal is invalid")
	}
	if finish.FailureEntryID != "" {
		failure, found := findEntry(state.Entries[childThreadID], finish.FailureEntryID)
		if !found || ValidateEntryIntegrity(failure) != nil || failure.ThreadID != childThreadID ||
			failure.TurnID != input.AdmittedTurnID || failure.Type != EntryRunFailure || terminal.ParentID != failure.ID {
			return errors.New("SubAgent terminal failure journal is invalid")
		}
	}
	for _, attempt := range state.EffectAttempts {
		if attempt.Invocation.ThreadID != childThreadID || attempt.Invocation.TurnID != input.AdmittedTurnID {
			continue
		}
		if attempt.Invocation.RunID != input.AdmittedRunID || attempt.Generation > finish.Generation || !effectAttemptTerminalSafe(attempt.State) {
			return errors.New("SubAgent terminal effect authority is invalid")
		}
	}
	return nil
}

func validateNormalSubAgentFinish(finish turnFinishLedger, lease TurnLease, entries []Entry) error {
	if finish.ThreadID != lease.ThreadID || finish.TurnID != lease.TurnID ||
		(finish.Generation != lease.Generation && finish.Generation != lease.Generation+1) {
		return errors.New("SubAgent terminal finish does not match its admission lease")
	}
	terminal, found := findEntry(entries, finish.TerminalEntryID)
	if !found || ValidateEntryIntegrity(terminal) != nil || terminal.Type != EntryTurnMarker ||
		terminal.TurnID != lease.TurnID || !terminalTurnMarker(terminal.TurnStatus) {
		return errors.New("SubAgent terminal finish journal is invalid")
	}
	return nil
}

func legacySubAgentTerminalSourceHash(
	state memoryState,
	childThreadID string,
	input SubAgentInputRecord,
	started, user Entry,
	finish turnFinishLedger,
) (string, error) {
	var effects []EffectAttempt
	for _, attempt := range state.EffectAttempts {
		if attempt.Invocation.ThreadID == childThreadID && attempt.Invocation.TurnID == input.AdmittedTurnID {
			effects = append(effects, attempt)
		}
	}
	sort.Slice(effects, func(i, j int) bool { return effects[i].EffectAttemptID < effects[j].EffectAttemptID })
	terminal, _ := findEntry(state.Entries[childThreadID], finish.TerminalEntryID)
	failureRawHash := ""
	if finish.FailureEntryID != "" {
		failure, _ := findEntry(state.Entries[childThreadID], finish.FailureEntryID)
		failureRawHash = failure.RawHash
	}
	payload, err := json.Marshal(struct {
		Input           SubAgentInputRecord
		StartedRawHash  string
		UserRawHash     string
		Finish          turnFinishLedger
		TerminalRawHash string
		FailureRawHash  string
		Effects         []EffectAttempt
	}{input, started.RawHash, user.RawHash, finish, terminal.RawHash, failureRawHash, effects})
	if err != nil {
		return "", err
	}
	return StableHash(string(payload)), nil
}
