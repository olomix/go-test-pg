# go-test-pg — Helper tool to test go programs with PostgreSQL database

[![GoDoc](https://godoc.org/github.com/olomix/go-test-pg/v2?status.svg)](https://godoc.org/github.com/olomix/go-test-pg/v2)

v2 depends on pgx/v5. For pgx/v4 support, use the 1.x.x version of this
package.

This package helps test Go programs against a PostgreSQL database. It
creates a fresh database for each test and drops it when the test completes.

The tool also checks that all resources are released when the test exits.
If any `Rows` are not closed or a `Conn` is not released to the pool, the
test fails.

`go-test-pg` uses a schema file to set up the database. It creates a
template database from this schema, and each test gets its own copy of
that template. If a template database for the schema already exists, it
is reused. The template name is composed of `baseName` and the MD5 hash
of the schema file content. If no schema file is provided, the default
PostgreSQL `template1` database is used.

When a test completes, its temporary database is dropped. The template
database is kept for future reuse.

The template database is created on first use. If you create a `Pgpool`
but never call any `With*` method, no database is touched.

Each method has a `Std` variant that returns `*sql.DB`. For example,
`WithFixtures` returns `*pgxpool.Pool` and `WithStdFixtures` returns
`*sql.DB`.

## Example usage

```go
package main

import (
	"context"
	"testing"

	ptg "github.com/olomix/go-test-pg/v2"
)

var dbpool = &ptg.Pgpool{SchemaFile: "../schema.sql"}

func TestX(t *testing.T) {
    dbPool := dbpool.WithEmpty(t)
    var dbName string
    err := dbPool.
        QueryRow(context.Background(), "SELECT current_database()").
        Scan(&dbName)
    if err != nil {
        t.Fatal(err)
    }

    t.Log(dbName)
}
```

The database connection is configured using standard PostgreSQL environment
variables (https://www.postgresql.org/docs/11/libpq-envars.html). The user
needs permissions to create databases.

To skip all database tests, set `Skip` to `true`:

```go
var dbpool = &ptg.Pgpool{Skip: true}
```

## Connection Options

You can customize connection settings using functional options. `WithPoolConfig`
gives you access to `*pgxpool.Config`, which includes `ConnConfig` for
connection-level settings.

### Per-test options

Pass options directly to any `With*` method:

```go
pool := dbpool.WithEmpty(t, ptg.WithPoolConfig(func(cfg *pgxpool.Config) {
    cfg.MaxConns = 2
}))
```

### Global options

Set `Options` on the `Pgpool` struct to apply to all tests sharing the pool:

```go
var dbpool = &ptg.Pgpool{
    SchemaFile: "../schema.sql",
    Options: []ptg.Option{
        ptg.WithPoolConfig(func(cfg *pgxpool.Config) {
            cfg.MaxConns = 5
        }),
    },
}
```

Per-test options are applied after global options and override them.

The `WithStd*` methods return `*sql.DB` and do not use a pgx pool internally,
only a pgx connection. Pool-level settings (e.g. `MaxConns`) will be ignored,
but connection-level settings via `cfg.ConnConfig` will be honored.
