package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("expected a writable control path and a candidate path")
	}
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.ThreadSQLite(os.Args[1])})
	if err != nil {
		log.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	host, err = runtime.Open(ctx, runtime.Options{Storage: storage.ThreadSQLite(os.Args[2])})
	if host != nil {
		_ = host.Shutdown(ctx)
		log.Fatal("released writer opened the candidate")
	}
	if err == nil || !strings.Contains(err.Error(), "unsupported Floret thread file format") {
		log.Fatalf("unexpected rejection: %v", err)
	}
}
