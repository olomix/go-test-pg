package go_test_pg

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

func TestPgpool_WithStdEmpty(t *testing.T) {
	x := Pgpool{}
	db := x.WithStdEmpty(t)
	err := db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	dbName, err := queryDBName(db)
	if err != nil {
		t.Fatal(err)
	}

	match, err := regexp.MatchString(`template1_\d+`, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if !match {
		t.Fatalf("database name does not match RE: %v", dbName)
	}
}

// test fail if unreleased connections exists
func TestPgpool_WithStdEmpty_InuseConnections(t *testing.T) {
	x := Pgpool{}
	db, cleanupFn := x.newStdDBWithCleanup(t)
	if cleanupFn == nil {
		t.Fatal("cleanupFn is nil")
	}

	dbName, err := queryDBName(db)
	if err != nil {
		t.Error(err)
		if err := cleanupFn(); err != nil {
			t.Error(err)
		}
		t.FailNow()
	}

	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
		if err := cleanupFn(); err != nil {
			t.Log(err)
		}
		t.FailNow()
	}

	expectedErr := fmt.Sprintf(
		"unreleased connections exists: 1, can't drop database %v", dbName)
	err = cleanupFn()
	if err == nil || err.Error() != expectedErr {
		t.Error(err)
	}

	if err = tx.Rollback(); err != nil {
		t.Error(err)
	}
	if err = cleanupFn(); err != nil {
		t.Error(err)
	}
}

func TestName(t *testing.T) {
	var dbPool = Pgpool{
		BaseName:   "go_test_pg",
		SchemaFile: "./testdata/schema1.sql",
	}
	db := dbPool.WithEmpty(t)
	err := db.QueryRow(context.Background(), `SELECT id FROM table1`).Scan()
	if err != pgx.ErrNoRows {
		t.Fatalf("Wanot pgx.ErrNoRows error, got %v", err)
	}
}

func queryDBName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow(`SELECT current_database()`).Scan(&dbName)
	return dbName, errors.WithStack(err)
}
