package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

type sample struct {
	Trial             int                    `json:"trial"`
	InspectionSeconds float64                `json:"inspection_seconds"`
	FirstSeconds      float64                `json:"first_seconds"`
	SubsequentSeconds float64                `json:"subsequent_seconds"`
	Phases            []runtime.StartupPhase `json:"phases"`
}

func main() {
	source := flag.String("source", "", "SQLite source; only public backup reads it")
	runs := flag.Int("runs", 3, "isolated trials")
	flag.Parse()
	if *source == "" {
		panic("source is required")
	}
	root, err := os.MkdirTemp("", "floret-startup-performance-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var samples []sample
	for index := 0; index < *runs; index++ {
		path := filepath.Join(root, fmt.Sprintf("trial-%d.sqlite", index))
		if err := storage.BackupSQLite(ctx, *source, path); err != nil {
			panic(err)
		}
		result := sample{Trial: index + 1}
		started := time.Now()
		inspection, err := runtime.InspectSQLite(ctx, path)
		if err != nil {
			panic(err)
		}
		result.InspectionSeconds = time.Since(started).Seconds()
		if inspection.MigrationRequired {
			if err := storage.BackupSQLite(ctx, path, path+".backup"); err != nil {
				panic(err)
			}
		}
		if _, err := storage.MaintainSQLite(ctx, path, storage.SQLiteMaintenancePolicy{MinimumFileBytes: 1 << 30, MinimumReclaimBytes: 256 << 20, MinimumReclaimRatio: 0.25, RetainedFreeBytes: 64 << 20}); err != nil {
			panic(err)
		}
		host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true, StartupProgress: runtime.StartupProgressFunc(func(phase runtime.StartupPhase) { result.Phases = append(result.Phases, phase) })})
		if err != nil {
			panic(err)
		}
		result.FirstSeconds = time.Since(started).Seconds()
		if err := host.Shutdown(ctx); err != nil {
			panic(err)
		}
		started = time.Now()
		if _, err := runtime.InspectSQLite(ctx, path); err != nil {
			panic(err)
		}
		if _, err := storage.MaintainSQLite(ctx, path, storage.SQLiteMaintenancePolicy{MinimumFileBytes: 1 << 30, MinimumReclaimBytes: 256 << 20, MinimumReclaimRatio: 0.25, RetainedFreeBytes: 64 << 20}); err != nil {
			panic(err)
		}
		host, err = runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true})
		if err != nil {
			panic(err)
		}
		result.SubsequentSeconds = time.Since(started).Seconds()
		if err := host.Shutdown(ctx); err != nil {
			panic(err)
		}
		samples = append(samples, result)
	}
	if err := json.NewEncoder(os.Stdout).Encode(samples); err != nil {
		panic(err)
	}
}
