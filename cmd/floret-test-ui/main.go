package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/floegence/floret/internal/testui"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8765", "HTTP listen address")
	root := flag.String("root", ".", "Floret repository root")
	storageMode := flag.String("storage", testui.StorageModeSQLite, "session storage mode: sqlite or memory")
	storagePath := flag.String("storage-path", "", "SQLite storage path (default: <root>/.floret-test-ui/floret.db)")
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve root: %v\n", err)
		os.Exit(1)
	}
	runner := testui.NewRunner(absRoot)
	runner.StorageMode = *storageMode
	runner.StoragePath = *storagePath
	server, err := testui.NewServer(runner)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create server: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := server.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close test UI: %v\n", err)
		}
	}()
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen on %s: %v\n", *addr, err)
		os.Exit(1)
	}
	fmt.Printf("Floret test UI listening on http://%s\n", listener.Addr().String())
	if err := http.Serve(listener, server.Handler()); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
