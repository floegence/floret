package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("expected an empty fixture directory")
	}
	ctx := context.Background()
	root := os.Args[1]
	catalog, err := threadsqlite.CreateCatalog(ctx, filepath.Join(root, "floret_threads.sqlite"))
	if err != nil {
		log.Fatal(err)
	}
	files, err := threadsqlite.NewFiles(root, catalog.Store, 2, 1, time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	ref, err := threadsqlite.Allocate("fixture")
	if err != nil {
		log.Fatal(err)
	}
	if err := files.Create(ctx, ref); err != nil {
		log.Fatal(err)
	}
	if err := catalog.Publish(ctx, files, "fixture", []threadsqlite.Replacement{{Thread: ref.Thread, Next: ref.File}}); err != nil {
		log.Fatal(err)
	}
	if err := files.Close(ctx); err != nil {
		log.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		log.Fatal(err)
	}
}
