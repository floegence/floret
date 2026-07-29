package florettest_test

import (
	"testing"

	"github.com/floegence/floret/v2/florettest"
)

func TestPublicToolContract(t *testing.T) {
	florettest.RunToolContract(t, florettest.PublicToolContractFactory)
}
