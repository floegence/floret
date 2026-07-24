package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestExample(t *testing.T) {
	if err := run(context.Background(), filepath.Join(t.TempDir(), "floret.db")); err != nil {
		t.Fatal(err)
	}
}
