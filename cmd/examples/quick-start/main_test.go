package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestQuickStart(t *testing.T) {
	if err := run(context.Background(), filepath.Join(t.TempDir(), "floret.db")); err != nil {
		t.Fatal(err)
	}
}
