package florettest_test

import (
	"testing"

	"github.com/floegence/floret/florettest"
)

func TestPublicToolContract(t *testing.T) {
	florettest.RunToolContract(t, florettest.PublicToolContractFactory)
}
