package runtime_test

import flruntime "github.com/floegence/floret/v7/runtime"

// Host was comparable in v7.16.2; private runtime state must preserve that API.
var _ = requireComparableHost[flruntime.Host]

func requireComparableHost[T comparable]() {}
