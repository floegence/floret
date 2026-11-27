//go:build floret_layout_cutover

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	cutover "github.com/floegence/floret/v7/scripts/testdata/thread-layout-cutover"
)

func main() {
	if len(os.Args) != 7 {
		fmt.Fprintln(os.Stderr, "usage: cleanup existing-db live live-scopes deleted deleted-scopes records")
		os.Exit(2)
	}
	var expected cutover.CleanupInventory
	for index, field := range []*int{&expected.Live, &expected.LiveScopes, &expected.Deleted, &expected.DeletedScopes} {
		value, err := strconv.Atoi(os.Args[2+index])
		if err != nil || value < 0 {
			os.Exit(2)
		}
		*field = value
	}
	records, err := strconv.ParseUint(os.Args[6], 10, 64)
	if err != nil {
		os.Exit(2)
	}
	removed, err := cutover.CleanupDeleted(context.Background(), os.Args[1], expected, records)
	if err != nil {
		fmt.Fprintln(os.Stderr, "offline cleanup refused:", err)
		os.Exit(1)
	}
	fmt.Printf("live=%d live_scopes=%d deleted_threads=%d cleared_scopes=%d removed_records=%d\n", expected.Live, expected.LiveScopes, expected.Deleted, expected.DeletedScopes, removed)
}
