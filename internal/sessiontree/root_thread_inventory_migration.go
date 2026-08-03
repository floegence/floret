package sessiontree

import "errors"

// Version 4 adds the transactionally derived root-thread inventory record.
// The canonical in-memory fields are unchanged; BackendRepo creates and
// verifies the derived record while committing this version edge.
func migrateMemoryStateV3ToV4(state *memoryState) error {
	if state == nil || state.Version != 3 {
		return errors.New("session-tree v3 migration requires exact version 3 state")
	}
	if err := validateRequiredMemoryStateMaps(*state); err != nil {
		return err
	}
	state.Version = 4
	return nil
}
