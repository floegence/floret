package florettest_test

import (
	"testing"

	"github.com/floegence/floret/florettest"
)

func TestTerminalOutcomeContract(t *testing.T) {
	florettest.RunTerminalOutcomeContract(t, florettest.TerminalOutcomeContractOptions{})
}
