package cache

import "testing"

func TestDecodeMemoryStateRejectsUnknownTrailingAndUnsupportedVersion(t *testing.T) {
	for name, encoded := range map[string][]byte{
		"unknown":     []byte(`{"version":1,"segments":[],"toolsets":[],"requests":[],"responses":[],"unknown":true}`),
		"trailing":    []byte(`{"version":1,"segments":[],"toolsets":[],"requests":[],"responses":[]} {}`),
		"unsupported": []byte(`{"version":2,"segments":[],"toolsets":[],"requests":[],"responses":[]}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeMemoryState(encoded); err == nil {
				t.Fatalf("%s prompt state passed validation", name)
			}
		})
	}
}
