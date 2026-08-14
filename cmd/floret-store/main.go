// Command floret-store performs explicit offline Floret store operations.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const usage = "floret-store offline v2/v3 migration commands were removed in v4; runtime.Open performs the supported v4-to-v5 migration"

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
		return errors.New("floret-store requires context and output streams")
	}
	if len(args) == 0 {
		return errors.New(usage)
	}
	return errors.New(usage)
}
