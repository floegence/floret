package florettest_test

import (
	"testing"

	"github.com/floegence/floret/v7/florettest"
)

func TestPublicToolContract(t *testing.T) {
	florettest.RunToolContract(t, florettest.NewToolContractRegistry)
}
