package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("expected the synthetic catalog path")
	}
	host, err := runtime.Open(context.Background(), runtime.Options{Storage: storage.SQLite(os.Args[1])})
	if host != nil {
		_ = host.Shutdown(context.Background())
		log.Fatal("old writer opened new catalog")
	}
	if !errors.Is(err, storage.ErrSQLiteTooNew) {
		log.Fatalf("unexpected rejection: %v", err)
	}
	fmt.Println("old writer rejected the new catalog as too new")
}
