// Command floret-store performs explicit offline Floret store operations.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/floegence/floret/v2/storage"
)

const usage = "usage: floret-store migrate-v2 --path <sqlite-path> --operation-id <id>"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if ctx == nil || stdout == nil || stderr == nil {
		return errors.New("floret-store migrate-v2 requires context and output streams")
	}
	if len(args) == 0 || args[0] != "migrate-v2" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("migrate-v2", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "SQLite store path")
	operationID := flags.String("operation-id", "", "migration operation identity")
	if err := flags.Parse(args[1:]); err != nil {
		return fmt.Errorf("%s: %w", usage, err)
	}
	if flags.NArg() != 0 || strings.TrimSpace(*path) == "" || strings.TrimSpace(*operationID) == "" {
		return errors.New(usage)
	}
	result, err := storage.MigrateV2(ctx, storage.MigrateV2Request{Path: *path, OperationID: *operationID})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}
