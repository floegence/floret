package composition

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v2/config"
	floretruntime "github.com/floegence/floret/v2/runtime"
)

var durableBasicFixtureLease = floretruntime.StoreLeasePolicy{
	TTL: 300 * time.Millisecond, RenewInterval: 100 * time.Millisecond, ClockSkewAllowance: 50 * time.Millisecond,
}

type durableBasicFixtureCatalog struct{}

func (durableBasicFixtureCatalog) ListFloretRoots(context.Context) ([]floretDurableRoot, error) {
	return []floretDurableRoot{{ThreadID: "interrupted-root", Profile: "durable-basic", SubAgentsEnabled: false}}, nil
}

type durableBasicCrashGateway struct{}

func (durableBasicCrashGateway) StreamModel(context.Context, floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	os.Exit(0)
	return nil, nil
}

func TestDurableBasicBlocksInterruptedRoot(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "floret.db")
	command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=TestDurableBasicInterruptedChild$")
	command.Env = append(os.Environ(), "FLORET_DURABLE_BASIC_CHILD=1", "FLORET_DURABLE_BASIC_DB="+databasePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("interrupted child: %v\n%s", err, output)
	}
	time.Sleep(durableBasicFixtureLease.TTL + durableBasicFixtureLease.ClockSkewAllowance + 100*time.Millisecond)

	_, err := openFloretComposition(
		context.Background(), databasePath, floretruntime.SQLiteStartupRequest{},
		[]floretruntime.SQLiteStoreOption{floretruntime.WithSQLiteStoreLeasePolicy(durableBasicFixtureLease)},
		durableBasicFixtureCatalog{},
		config.Config{Provider: config.ProviderFake, Model: "fake", FakeResponse: "done", SystemPrompt: "test"},
	)
	var recovery *FloretRecoveryRequiredError
	if !errors.As(err, &recovery) {
		t.Fatalf("startup error = %v, want FloretRecoveryRequiredError", err)
	}
	if len(recovery.Targets) != 1 || recovery.Targets[0].ThreadID != "interrupted-root" ||
		recovery.Targets[0].TurnID != "interrupted-turn" || recovery.Targets[0].RunID != "interrupted-run" {
		t.Fatalf("recovery targets = %#v", recovery.Targets)
	}
}

func TestDurableBasicInterruptedChild(t *testing.T) {
	if os.Getenv("FLORET_DURABLE_BASIC_CHILD") != "1" {
		t.Skip("helper process only")
	}
	databasePath := os.Getenv("FLORET_DURABLE_BASIC_DB")
	ctx := context.Background()
	startup, err := floretruntime.StartSQLiteStore(
		ctx, databasePath, floretruntime.SQLiteStartupRequest{},
		floretruntime.WithSQLiteStoreLeasePolicy(durableBasicFixtureLease),
	)
	if err != nil {
		t.Fatal(err)
	}
	var create *floretruntime.ThreadCreateHostBinder
	var turns *floretruntime.TurnExecutionHostBinder
	if err := floretruntime.ConfigureHostCapabilities(startup.Store, func(bootstrap *floretruntime.HostBootstrap) error {
		var bindErr error
		create, bindErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if bindErr != nil {
			return bindErr
		}
		turns, bindErr = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		return bindErr
	}); err != nil {
		t.Fatal(err)
	}
	creator, err := create.Bind("interrupted-root", "create-interrupted-root")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: "interrupted-root", CreateIntentID: "create-interrupted-root"}); err != nil {
		t.Fatal(err)
	}
	factory, err := turns.Bind("interrupted-root")
	if err != nil {
		t.Fatal(err)
	}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	hostOptions, err := floretruntime.NewTurnExecutionHostOptions(
		config.Config{SystemPrompt: "test", ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}},
		floretruntime.WithTurnModelGateway(durableBasicCrashGateway{}, floretruntime.ModelGatewayIdentity{
			Provider: "fixture", Model: "crash", StateCompatibilityKey: "fixture:crash:v1",
		}, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning}),
	)
	if err != nil {
		t.Fatal(err)
	}
	host, err := factory.NewHost(ctx, hostOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = host.RunTurn(ctx, floretruntime.RunTurnRequest{
		ThreadID: "interrupted-root", TurnID: "interrupted-turn", RunID: "interrupted-run",
		Input: floretruntime.TurnInput{Text: "crash after admission"},
	})
	t.Fatal("crash gateway returned")
}
