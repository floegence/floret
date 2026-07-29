package florettest_test

import (
	"testing"

	"github.com/floegence/floret/v2/florettest"
)

func TestTerminalOutcomeContract(t *testing.T) {
	florettest.RunTerminalOutcomeContract(t, florettest.TerminalOutcomeContractOptions{})
}
