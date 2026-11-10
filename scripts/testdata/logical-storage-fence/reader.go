// Command reader is compiled against exact released runtimes by the fence gate.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	_ "modernc.org/sqlite"
)

func records(file string) [32]byte {
	db, err := sql.Open("sqlite", file)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT namespace,key,value FROM floret_backend_records ORDER BY namespace,key")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var namespace string
		var key, value []byte
		if err := rows.Scan(&namespace, &key, &value); err != nil {
			panic(err)
		}
		fmt.Fprintf(hash, "%d:%s%d:", len(namespace), namespace, len(key))
		hash.Write(key)
		fmt.Fprintf(hash, "%d:", len(value))
		hash.Write(value)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}
func main() {
	if len(os.Args) != 2 {
		panic("database path required")
	}
	before := records(os.Args[1])
	host, err := runtime.Open(context.Background(), runtime.Options{Storage: storage.SQLite(os.Args[1]), DeferExecution: true})
	if host != nil {
		host.Shutdown(context.Background())
		panic("old reader admitted a newer domain")
	}
	if !errors.Is(err, runtime.ErrStoreTooNew) || !errors.Is(err, runtime.ErrUnsupportedSchema) {
		panic(fmt.Sprintf("expected newer-runtime classification, got %v", err))
	}
	if records(os.Args[1]) != before {
		panic("old reader changed canonical records")
	}
	fmt.Println("released reader: ErrStoreTooNew, all canonical records unchanged")
}
