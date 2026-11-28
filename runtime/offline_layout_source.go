//go:build floret_layout_cutover

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type offlinePromptSource struct{ records spi.ReadTx }

func (source offlinePromptSource) View(ctx context.Context, read func(spi.ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return read(source.records)
}

func (offlinePromptSource) Update(context.Context, func(spi.WriteTx) error) error {
	return errors.ErrUnsupported
}
func (offlinePromptSource) Close() error { return nil }

func VerifyOfflineItemWindows(ctx context.Context, source, target spi.ReadTx, thread string) (int, error) {
	var compare func(sessiontree.WindowProjection, sessiontree.WindowProjection) error
	if member.From(target) != nil {
		encode := func(window sessiontree.WindowProjection, records spi.ReadTx) ([]byte, error) {
			kernel, err := storage.NewDeferredBackendKernel(offlinePromptSource{records}, time.Now, func(ctx context.Context) error { return ctx.Err() })
			if err != nil {
				return nil, err
			}
			service := threadRuntimeService{host: &Host{store: &runtimeStore{prompt: kernel}, enableParentEvents: true}}
			if member.From(records) == nil {
				service.host.store.prompt = storage.OfflineRequestLedger{Store: kernel, Records: records}
			}
			view, info, err := service.projectSourceWindow(ctx, ThreadWindowInput{ThreadID: identity.ThreadID(thread)}, window, window.ParentEvents == nil)
			if err != nil {
				return nil, err
			}
			for index, item := range view.Items {
				view.Items[index] = (&threadRuntimeState{}).freezeWindowItem(item, 2000, true)
			}
			result := struct {
				View    ThreadView
				Summary *sessiontree.WindowSummary
			}{view.ThreadView, info.summary}
			normalizeOfflineInstants(reflect.ValueOf(&result))
			return json.Marshal(result)
		}
		compare = func(before, after sessiontree.WindowProjection) error {
			left, err := encode(before, source)
			if err != nil {
				return fmt.Errorf("render source: %w", err)
			}
			right, err := encode(after, target)
			if err != nil {
				return fmt.Errorf("render destination: %w", err)
			}
			parse := func(raw []byte) (any, error) {
				var value any
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.UseNumber()
				err := decoder.Decode(&value)
				return value, err
			}
			original, err := parse(left)
			if err != nil {
				return err
			}
			candidate, err := parse(right)
			if err != nil {
				return err
			}
			return compareOfflineJSON(original, candidate, "view")
		}
	}
	return sessiontree.VerifyOfflineItemWindows(ctx, source, target, thread, func(metadata sessiontree.PathMetadata, control bool) []string {
		return coldProjectionFields(metadata, false, control)
	}, compare)
}

func normalizeOfflineInstants(value reflect.Value) {
	if value.Type() == reflect.TypeFor[time.Time]() && value.CanSet() {
		value.Set(reflect.ValueOf(value.Interface().(time.Time).UTC()))
		return
	}
	switch value.Kind() {
	case reflect.Pointer:
		if !value.IsNil() {
			normalizeOfflineInstants(value.Elem())
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).IsExported() {
				normalizeOfflineInstants(value.Field(index))
			}
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() != reflect.Uint8 {
			for index := 0; index < value.Len(); index++ {
				normalizeOfflineInstants(value.Index(index))
			}
		}
	}
}

func compareOfflineJSON(left, right any, path string) error {
	if reflect.DeepEqual(left, right) {
		return nil
	}
	switch original := left.(type) {
	case []any:
		if candidate, ok := right.([]any); ok && len(original) == len(candidate) {
			for i := range original {
				if err := compareOfflineJSON(original[i], candidate[i], fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	case map[string]any:
		if candidate, ok := right.(map[string]any); ok {
			for _, key := range slices.Sorted(maps.Keys(original)) {
				v, exists := candidate[key]
				if !exists {
					return fmt.Errorf("missing field %s.%s: %w", path, key, ErrAuthorityCorrupt)
				}
				if err := compareOfflineJSON(original[key], v, path+"."+key); err != nil {
					return err
				}
			}
			for _, key := range slices.Sorted(maps.Keys(candidate)) {
				if _, exists := original[key]; !exists {
					return fmt.Errorf("unexpected field %s.%s: %w", path, key, ErrAuthorityCorrupt)
				}
			}
		}
	}
	return fmt.Errorf("different %s: %w", path, ErrAuthorityCorrupt)
}

func CheckOfflineLayoutLogicalSchema(records spi.ReadTx) error {
	if records == nil {
		return spi.ErrInvalidArgument
	}
	state, err := inspectLogicalSchemaTransaction(records)
	if err != nil {
		return err
	}
	if state != logicalSchemaCurrent {
		return spi.ErrMigrationRequired
	}
	return nil
}

func OfflineSourceDomainVersion(records spi.ReadTx) (int, error) {
	state, err := inspectLogicalSchemaTransaction(records)
	if err != nil {
		return 0, err
	}
	switch state {
	case logicalSchemaV19:
		return 42, nil
	case logicalSchemaCurrent:
		return 43, nil
	default:
		return 0, spi.ErrMigrationRequired
	}
}
