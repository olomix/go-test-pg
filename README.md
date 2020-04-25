# go-test-pg — Helper tool to test go programs with PostgreSQL database

[![GoDoc](https://godoc.org/github.com/olomix/go-test-pg?status.svg)](https://godoc.org/github.com/olomix/go-test-pg)

The aim this package is to help test golang programs against PostgreSQL
database. It creates an empty database for each test and drops it when test
is complete.

As a side effect tool checks that all resources are released when test exits.
If any Rows is not closed or Conn is not released to pool, test fails.

`go-test-pg` requires schema file to initialize database with. It creates
template database with this schema. Then each temporary database for every test
creates from this template database. If the template database for this
schema is exists, it will be reused. The name of the template database 
is composed of `baseName` and md5 hashsum of schema file content. 

On complete, temporary databases would be dropped, template database will not
be dropped and would remain for future reuse.

Template database would be created only on first use. If you call `NewPool`
and do not call `With<something>` on it, real database would not be touched.


## Example usage

```go
package main

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	ptg "github.com/olomix/go-test-pg"
)

var dbpool ptg.Pgpool

func TestMain(m *testing.M) {
    var dbUri = flag.String(
        "db-uri",
        "postgres://localhost/postgres?sslmode=disable",
        "uri of postgres database",
    )
    var schemaFile = flag.String(
        "schema",
        "../schema.sql",
        "file with database schema",
    )
    flag.Parse()

    dbpool = ptg.NewPool(*dbUri, *schemaFile, "my-project")
    os.Exit(m.Run())
}

func TestX(t *testing.T) {
    dbPool, dbClear := dbpool.WithEmpty(t)
    defer dbClear()
    var dbName string
    err := dbPool.
        QueryRow(context.Background(), "select current_database()").
        Scan(&dbName)
    if err != nil {
        t.Fatal(err)
    }

    t.Log(dbName)
}
```
