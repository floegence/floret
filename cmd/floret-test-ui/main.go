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
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve root: %v\n", err)
		os.Exit(1)
	}
	server, err := testui.NewServer(testui.NewRunner(absRoot))
	if err != nil {
		fmt.Fprintf(os.Stderr, "create server: %v\n", err)
		os.Exit(1)
	}
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
