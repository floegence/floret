package sessiontree

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestHistoricalManifestsRejectMigrationFields(t *testing.T) {
	for version := 7; version <= 31; version++ {
		format, err := migrationSourceFormat(version)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"source_version", "migration_time", "migration_phase", "migration_cursor", "migration_roots"} {
			for _, value := range []string{"null", "0", "1", "\"\""} {
				t.Run(fmt.Sprintf("v%d/%s/%s", version, field, value), func(t *testing.T) {
					record := backendDomainRecord{Value: json.RawMessage(fmt.Sprintf(`{"version":%d,"sequence":0,"%s":%s}`, version, field, value))}
					var manifest backendDomainManifest
					if err := decodeBackendDomainValue(format, record, &manifest); err == nil {
						t.Fatal("historical manifest accepted a later schema field")
					}
				})
			}
		}
		record := backendDomainRecord{Value: json.RawMessage(fmt.Sprintf(`{"version":%d,"sequence":19}`, version))}
		var manifest backendDomainManifest
		if err := decodeBackendDomainValue(format, record, &manifest); err != nil || manifest.Version != version || manifest.Sequence != 19 {
			t.Fatalf("v%d historical manifest: %+v, %v", version, manifest, err)
		}
	}
}
