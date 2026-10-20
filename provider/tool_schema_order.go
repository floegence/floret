package provider

import "github.com/floegence/floret/v7/internal/toolrender"

// OrderToolSchemas renders local tool input schema root properties in
// tools.InputPropertyOrder order at known tool declaration positions in format.
// Hosted tools, nested schemas and other request values retain their content.
// Call after final protocol encoding, before estimating, fingerprinting and
// freezing a request. The operation is pure and idempotent; malformed JSON or
// ordering declarations and unsupported formats return an error. It does not
// constrain the order of arguments returned by the model.
func OrderToolSchemas(format RequestFormat, payload []byte) ([]byte, error) {
	return toolrender.OrderSchemas(string(format), payload)
}
