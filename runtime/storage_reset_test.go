package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

const testOwnershipManifestSHA256 = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestResetStorageRemovesThreadsAndInitializesFreshSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.sqlite")
	source := storage.SQLite(path)
	host, err := Open(t.Context(), Options{Storage: source})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "reset-test", Name: "Reset Test"}, SystemPrompt: "Test.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, newBlockingThreadGateway())
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "before-reset"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := StorageResetRequest{
		Storage: source, EnvironmentID: "env_local", OperationID: "reinstall-1",
		OwnershipManifestSHA256: testOwnershipManifestSHA256,
	}
	preflight, err := PreflightStorageReset(t.Context(), request)
	if err != nil || !preflight.Supported {
		t.Fatalf("PreflightStorageReset() = %#v, %v", preflight, err)
	}
	result, err := ResetStorage(t.Context(), request)
	if err != nil || !result.PreviousDataRemoved {
		t.Fatalf("ResetStorage() = %#v, %v", result, err)
	}
	reopened, err := Open(t.Context(), Options{Storage: source})
	if err != nil {
		t.Fatal(err)
	}
	reopenedService, err := reopened.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	threads, err := reopenedService.List(t.Context(), ThreadScope{})
	if err != nil || len(threads) != 0 {
		t.Fatalf("threads after reset = %#v, %v", threads, err)
	}
	if err := reopened.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := ResetStorage(t.Context(), request); err != nil {
		t.Fatalf("repeated ResetStorage() error = %v", err)
	}
}

func TestStorageResetFailsClosedForUnsupportedBackendAndInvalidOwnership(t *testing.T) {
	request := StorageResetRequest{
		Storage: storage.NewSource(unsupportedResetSource{}), EnvironmentID: "env_local", OperationID: "reinstall-2",
		OwnershipManifestSHA256: testOwnershipManifestSHA256,
	}
	if _, err := PreflightStorageReset(t.Context(), request); !errors.Is(err, ErrStorageResetUnsupported) {
		t.Fatalf("PreflightStorageReset() error = %v, want %v", err, ErrStorageResetUnsupported)
	}
	request.OwnershipManifestSHA256 = "sha256:bad"
	if _, err := ResetStorage(t.Context(), request); !errors.Is(err, ErrStorageResetRequest) {
		t.Fatalf("ResetStorage() error = %v, want %v", err, ErrStorageResetRequest)
	}
}

type unsupportedResetSource struct{}

func (unsupportedResetSource) Open(context.Context) (spi.Backend, error) {
	return &unsupportedResetBackend{}, nil
}

type unsupportedResetBackend struct{}

func (*unsupportedResetBackend) View(context.Context, func(spi.ReadTx) error) error    { return nil }
func (*unsupportedResetBackend) Update(context.Context, func(spi.WriteTx) error) error { return nil }
func (*unsupportedResetBackend) Close() error                                          { return nil }
