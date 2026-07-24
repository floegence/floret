package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	floretruntime "github.com/floegence/floret/runtime"
)

const envelopeSchemaVersion = "1"

const (
	exitSuccess      = 0
	exitMigration    = 2
	exitBusy         = 3
	exitUnsupported  = 4
	exitVerification = 5
	exitIO           = 6
	exitUsage        = 7
	exitCancelled    = 130
)

type outputEnvelope struct {
	SchemaVersion string `json:"schema_version"`
	Command       string `json:"command"`
	OperationID   string `json:"operation_id"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
	Result        any    `json:"result"`
}

type maintenanceAPI struct {
	inspect func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error)
	verify  func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreVerification, error)
	migrate func(context.Context, string, floretruntime.SQLiteStoreMigrationRequest, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error)
}

var publicMaintenanceAPI = maintenanceAPI{
	inspect: floretruntime.InspectSQLiteStore,
	verify:  floretruntime.VerifySQLiteStore,
	migrate: floretruntime.MigrateSQLiteStore,
}

type commandOptions struct {
	command string
	path    string
	json    bool
	apply   bool
	help    bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(execute(ctx, os.Args[1:], os.Stdout, os.Stderr, publicMaintenanceAPI, newOperationID))
}

func execute(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	api maintenanceAPI,
	operationID func() (string, error),
) int {
	options, parseErr := parseCommand(args)
	id, idErr := operationID()
	if idErr != nil {
		return writeOutcome(stdout, stderr, options.json, outputEnvelope{
			SchemaVersion: envelopeSchemaVersion, Command: options.command, Status: "failed", Reason: "io_error",
		}, exitIO)
	}
	if options.help {
		writeUsage(stderr, options.command)
		return exitSuccess
	}
	if parseErr != nil {
		writeUsage(stderr, options.command)
		return writeOutcome(stdout, stderr, options.json, outputEnvelope{
			SchemaVersion: envelopeSchemaVersion, Command: options.command, OperationID: id,
			Status: "failed", Reason: "invalid_request",
		}, exitUsage)
	}

	switch options.command {
	case "inspect":
		inspection, err := api.inspect(ctx, options.path)
		if err != nil {
			return writeErrorOutcome(stdout, stderr, options, id, err, nil)
		}
		code, status := classifyInspection("inspect", inspection)
		return writeOutcome(stdout, stderr, options.json, outputEnvelope{
			SchemaVersion: envelopeSchemaVersion, Command: options.command, OperationID: id,
			Status: status, Reason: reasonOrDefault(string(inspection.Reason), "inspection_complete"), Result: inspection,
		}, code)
	case "verify":
		verification, err := api.verify(ctx, options.path)
		if err != nil {
			return writeErrorOutcome(stdout, stderr, options, id, err, nil)
		}
		code, status := classifyVerification(verification)
		return writeOutcome(stdout, stderr, options.json, outputEnvelope{
			SchemaVersion: envelopeSchemaVersion, Command: options.command, OperationID: id,
			Status: status, Reason: verificationReason(verification, code), Result: verification,
		}, code)
	case "migrate":
		inspection, err := api.inspect(ctx, options.path)
		if err != nil {
			return writeErrorOutcome(stdout, stderr, options, id, err, nil)
		}
		mode := floretruntime.SQLiteStoreMigrationPlan
		if options.apply {
			mode = floretruntime.SQLiteStoreMigrationApply
		}
		result, err := api.migrate(ctx, options.path, floretruntime.SQLiteStoreMigrationRequest{
			OperationID: id, Mode: mode, ExpectedSchema: inspection.Observed,
			Progress: func(progress floretruntime.SQLiteStoreMaintenanceProgress) {
				writeProgress(stderr, options.json, progress)
			},
		})
		if err != nil {
			return writeErrorOutcome(stdout, stderr, options, id, err, result)
		}
		code, status := classifyMigration(result)
		return writeOutcome(stdout, stderr, options.json, outputEnvelope{
			SchemaVersion: envelopeSchemaVersion, Command: options.command, OperationID: id,
			Status: status, Reason: migrationReason(result, code), Result: result,
		}, code)
	default:
		panic("validated command was not handled")
	}
}

func parseCommand(args []string) (commandOptions, error) {
	options := commandOptions{}
	remaining := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg {
		case "--json":
			options.json = true
		case "-h", "--help":
			options.help = true
		default:
			remaining = append(remaining, arg)
		}
	}
	if len(remaining) > 0 {
		options.command = remaining[0]
		remaining = remaining[1:]
	}
	if options.help && options.command == "" {
		return options, nil
	}
	if options.command != "inspect" && options.command != "verify" && options.command != "migrate" {
		return options, errors.New("unknown command")
	}
	paths := make([]string, 0, 1)
	for _, arg := range remaining {
		if arg == "--apply" && options.command == "migrate" {
			options.apply = true
			continue
		}
		if strings.HasPrefix(arg, "-") {
			return options, errors.New("unknown option")
		}
		paths = append(paths, arg)
	}
	if options.help {
		return options, nil
	}
	if len(paths) != 1 || strings.TrimSpace(paths[0]) == "" {
		return options, errors.New("exactly one store path is required")
	}
	options.path = paths[0]
	return options, nil
}

func classifyInspection(command string, inspection floretruntime.SQLiteStoreInspection) (int, string) {
	switch inspection.State {
	case floretruntime.SQLiteStoreStateCurrent:
		if inspection.LeasePolicyState == floretruntime.SQLiteStoreLeasePolicyMismatch {
			return exitVerification, "failed"
		}
		return exitSuccess, "ready"
	case floretruntime.SQLiteStoreStateUpgradeable:
		return exitMigration, "action_required"
	case floretruntime.SQLiteStoreStateUnsupportedOlder, floretruntime.SQLiteStoreStateFuture:
		return exitUnsupported, "unsupported"
	case floretruntime.SQLiteStoreStateDrifted, floretruntime.SQLiteStoreStateCorrupt:
		return exitVerification, "failed"
	case floretruntime.SQLiteStoreStateBusy:
		return exitBusy, "blocked"
	case floretruntime.SQLiteStoreStatePermissionDenied, floretruntime.SQLiteStoreStateIOError:
		return exitIO, "failed"
	case floretruntime.SQLiteStoreStateMissing, floretruntime.SQLiteStoreStateEmpty:
		if command == "inspect" {
			return exitSuccess, "ready"
		}
		return exitVerification, "failed"
	default:
		return exitVerification, "failed"
	}
}

func classifyVerification(verification floretruntime.SQLiteStoreVerification) (int, string) {
	code, status := classifyInspection("verify", verification.Inspection)
	for _, check := range verification.Checks {
		if !check.Passed && code == exitSuccess {
			return exitVerification, "failed"
		}
	}
	return code, status
}

func classifyMigration(result floretruntime.SQLiteStoreMigrationResult) (int, string) {
	if result.Status == floretruntime.SQLiteStoreMaintenanceCancelled || result.Reason == floretruntime.SQLiteStoreReasonCancelled {
		return exitCancelled, "cancelled"
	}
	if result.Reason == floretruntime.SQLiteStoreReasonBusy || result.Before.State == floretruntime.SQLiteStoreStateBusy {
		return exitBusy, "blocked"
	}
	if result.Status == floretruntime.SQLiteStoreMaintenanceFailed {
		code, _ := classifyInspection("migrate", result.Before)
		if code == exitSuccess {
			code = exitVerification
		}
		return code, "failed"
	}
	if result.Status != floretruntime.SQLiteStoreMaintenanceReady {
		return exitVerification, "failed"
	}
	if result.Mode == floretruntime.SQLiteStoreMigrationPlan && result.Before.State == floretruntime.SQLiteStoreStateUpgradeable {
		return exitMigration, "action_required"
	}
	return classifyInspection("migrate", result.After)
}

func writeErrorOutcome(stdout, stderr io.Writer, options commandOptions, operationID string, err error, result any) int {
	code, status, reason := classifyError(err)
	if migration, ok := result.(floretruntime.SQLiteStoreMigrationResult); ok && migration.Before.State != "" {
		resultCode, resultStatus := classifyMigration(migration)
		if code == exitIO && resultCode != exitSuccess {
			code, status = resultCode, resultStatus
			reason = reasonOrDefault(string(migration.Reason), string(migration.Before.Reason))
		}
	}
	return writeOutcome(stdout, stderr, options.json, outputEnvelope{
		SchemaVersion: envelopeSchemaVersion, Command: options.command, OperationID: operationID,
		Status: status, Reason: reason, Result: result,
	}, code)
}

func classifyError(err error) (int, string, string) {
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if errors.As(err, &maintenance) {
		switch maintenance.Reason {
		case floretruntime.SQLiteStoreReasonCancelled:
			return exitCancelled, "cancelled", "cancelled"
		case floretruntime.SQLiteStoreReasonBusy:
			return exitBusy, "blocked", "busy"
		case floretruntime.SQLiteStoreReasonInvalidRequest:
			return exitUsage, "failed", "invalid_request"
		case floretruntime.SQLiteStoreReasonPermission:
			return exitIO, "failed", "permission_denied"
		case floretruntime.SQLiteStoreReasonCorrupt:
			return exitVerification, "failed", "corrupt"
		case floretruntime.SQLiteStoreReasonIO:
			return exitIO, "failed", "io_error"
		}
	}
	var unsupported *floretruntime.UnsupportedStoreSchemaError
	if errors.As(err, &unsupported) {
		return exitUnsupported, "unsupported", "unsupported_schema"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return exitCancelled, "cancelled", "cancelled"
	}
	return exitIO, "failed", "io_error"
}

func verificationReason(verification floretruntime.SQLiteStoreVerification, code int) string {
	if code == exitVerification {
		for _, check := range verification.Checks {
			if !check.Passed {
				return reasonOrDefault(check.Code, "verification_failed")
			}
		}
	}
	return reasonOrDefault(string(verification.Inspection.Reason), "verification_complete")
}

func migrationReason(result floretruntime.SQLiteStoreMigrationResult, code int) string {
	if result.Reason != "" {
		return string(result.Reason)
	}
	if code == exitMigration {
		return "migration_available"
	}
	if result.Mode == floretruntime.SQLiteStoreMigrationApply && result.Committed {
		return "migration_committed"
	}
	return "migration_not_required"
}

func reasonOrDefault(reason, fallback string) string {
	if strings.TrimSpace(reason) == "" {
		return fallback
	}
	return reason
}

func writeOutcome(stdout, stderr io.Writer, jsonOutput bool, envelope outputEnvelope, code int) int {
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(envelope); err != nil {
			fmt.Fprintln(stderr, "floret-store: failed to write structured output")
			return exitIO
		}
		return code
	}
	writeHumanOutcome(stderr, envelope)
	return code
}

func writeHumanOutcome(stderr io.Writer, envelope outputEnvelope) {
	fmt.Fprintf(stderr, "%s: %s (%s)\n", displayCommand(envelope.Command), envelope.Status, envelope.Reason)
	switch result := envelope.Result.(type) {
	case floretruntime.SQLiteStoreInspection:
		writeHumanInspection(stderr, result)
	case floretruntime.SQLiteStoreVerification:
		writeHumanInspection(stderr, result.Inspection)
		for _, check := range result.Checks {
			status := "passed"
			if !check.Passed {
				status = "failed"
			}
			fmt.Fprintf(stderr, "  check %-24s %s\n", check.Code, status)
		}
	case floretruntime.SQLiteStoreMigrationResult:
		fmt.Fprintf(stderr, "  mode:      %s\n", result.Mode)
		fmt.Fprintf(stderr, "  changed:   %t\n", result.Changed)
		fmt.Fprintf(stderr, "  committed: %t\n", result.Committed)
		fmt.Fprintf(stderr, "  rollback:  %t\n", result.RolledBack)
		for _, step := range result.Steps {
			fmt.Fprintf(stderr, "  step %s -> %s (%s)\n", schemaVersion(step.From), schemaVersion(step.To), step.Code)
		}
	}
}

func writeHumanInspection(stderr io.Writer, inspection floretruntime.SQLiteStoreInspection) {
	fmt.Fprintf(stderr, "  state:    %s\n", inspection.State)
	fmt.Fprintf(stderr, "  observed: %s\n", schemaVersion(inspection.Observed))
	fmt.Fprintf(stderr, "  current:  %s\n", schemaVersion(inspection.Current))
	if len(inspection.Actions) > 0 {
		actions := make([]string, len(inspection.Actions))
		for index, action := range inspection.Actions {
			actions[index] = string(action)
		}
		fmt.Fprintf(stderr, "  actions:  %s\n", strings.Join(actions, ", "))
	}
}

func schemaVersion(identity floretruntime.StoreSchemaIdentity) string {
	if identity.Version == "" {
		return "not available"
	}
	return identity.Version
}

func writeProgress(stderr io.Writer, jsonOutput bool, progress floretruntime.SQLiteStoreMaintenanceProgress) {
	if jsonOutput || progress.Status != floretruntime.SQLiteStoreMaintenanceRunning {
		return
	}
	if progress.Total > 0 {
		fmt.Fprintf(stderr, "migrate: %s (%d/%d)\n", progress.Phase, progress.Step, progress.Total)
		return
	}
	fmt.Fprintf(stderr, "migrate: %s\n", progress.Phase)
}

func displayCommand(command string) string {
	if command == "" {
		return "floret-store"
	}
	return command
}

func writeUsage(stderr io.Writer, command string) {
	switch command {
	case "inspect", "verify":
		fmt.Fprintf(stderr, "Usage: floret-store %s [--json] <store-path>\n", command)
	case "migrate":
		fmt.Fprintln(stderr, "Usage: floret-store migrate [--json] [--apply] <store-path>")
	default:
		fmt.Fprintln(stderr, "Usage: floret-store <inspect|verify|migrate> [--json] [--apply] <store-path>")
	}
}

func newOperationID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return "store-" + hex.EncodeToString(bytes[:]), nil
}
