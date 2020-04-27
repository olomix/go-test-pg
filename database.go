package go_test_pg

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

const defaultTimeout = 30 * time.Second

type Fixture struct {
	Query  string
	Params []interface{}
}

type Pgpool struct {
	// BaseName is the prefix of template and temporary databases.
	// Default is dbtestpg.
	BaseName string
	// Name of schema file. Required. Tests would fail if not set.
	SchemaFile string // schema file name
	// If true, skip all database tests.
	Skip bool

	m    sync.RWMutex
	err  error
	tmpl string
	rnd  *rand.Rand
}

// WithFixtures creates database from template database, and initializes it
// with fixtures from `fixtures` array
func (p *Pgpool) WithFixtures(
	t testing.TB,
	fixtures []Fixture,
) (*pgxpool.Pool, func()) {
	pool, clean := p.WithEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, f := range fixtures {
		if _, err := pool.Exec(ctx, f.Query, f.Params...); err != nil {
			clean()
			t.Fatalf(
				"can't load fixture at idx %v: %+v",
				i, errors.WithStack(err),
			)
		}
	}
	return pool, clean
}

// WithSQLs creates database from template database, and initializes it
// with fixtures from `sqls` array
func (p *Pgpool) WithSQLs(t testing.TB, sqls []string) (*pgxpool.Pool, func()) {
	pool, clean := p.WithEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, s := range sqls {
		if _, err := pool.Exec(ctx, s); err != nil {
			clean()
			t.Fatalf(
				"can't load fixture at idx %v: %+v",
				i, errors.WithStack(err),
			)
		}
	}
	return pool, clean
}

func (p *Pgpool) getTmpl(t testing.TB) string {
	t.Helper()

	if p.Skip {
		t.Skip("Skip database tests")
	}

	p.m.RLock()
	err := p.err
	tmpl := p.tmpl
	p.m.RUnlock()

	if err != nil {
		t.Fatal(err)
	}

	if tmpl != "" {
		return tmpl
	}
	p.m.Lock()
	p.rnd = rand.New(rand.NewSource(time.Now().UnixNano() + int64(os.Getpid())))
	p.tmpl, p.err = p.createTemplateDB()
	err = p.err
	p.m.Unlock()

	if err != nil {
		t.Fatalf("%+v", err)
	}

	return p.tmpl
}

func (p *Pgpool) createRndDB(t testing.TB) (*pgxpool.Pool, string) {
	tmpl := p.getTmpl(t)
	dbName := fmt.Sprintf("%v_%v", tmpl, p.rnd.Int31())

	err := p.createDB(dbName, tmpl)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		_ = dropDB(dbName)
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = dbName

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		_ = dropDB(dbName)
		t.Fatal()
	}

	return pool, dbName
}

func withNewConnection(
	dbName string,
	fn func(context.Context, *pgx.Conn) error,
) (err error) {
	var cfg *pgx.ConnConfig
	cfg, err = pgx.ParseConfig("")
	if err != nil {
		return errors.WithStack(err)
	}

	if dbName != "" {
		cfg.Database = dbName
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err2 := conn.Close(ctx)
		cancel()
		if err2 != nil {
			if err == nil {
				err = errors.WithStack(err2)
			} else {
				log.Printf("error closing DB connection: %v", err2)
			}
		}
	}()

	err = fn(ctx, conn)

	return err
}

func dropDB(dbName string) error {
	return withNewConnection(
		"",
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "DROP DATABASE "+quote(dbName))
			return errors.WithStack(err)
		},
	)
}

// WithEmpty creates empty database from template database, that was
// created from `schema` file.
func (p *Pgpool) WithEmpty(t testing.TB) (*pgxpool.Pool, func()) {
	pool, dbName := p.createRndDB(t)
	return pool, func() {
		acquiredConns := pool.Stat().AcquiredConns()
		if acquiredConns > 0 {
			t.Fatalf(
				"unreleased connections exists: %v, can't drop database %v",
				acquiredConns, dbName,
			)
		}
		pool.Close()
		err := dropDB(dbName)
		if err != nil {
			t.Errorf("Can't drop DB %v: %v", dbName, err)
		}
	}
}

func (p *Pgpool) createDB(name, tmplName string) error {
	query := `CREATE DATABASE ` + quote(name)
	if tmplName != "" {
		query += ` WITH TEMPLATE ` + quote(tmplName)
	}

	return withNewConnection(
		"",
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, query)
			return errors.WithStack(err)
		},
	)
}

func (p *Pgpool) createTemplateDB() (string, error) {
	if p.SchemaFile == "" {
		return "", errors.New("SchemaFile is empty")
	}
	schemaSql, err := ioutil.ReadFile(p.SchemaFile)
	if err != nil {
		return "", errors.WithStack(err)
	}
	checksum := md5.Sum(schemaSql)
	schemaHex := hex.EncodeToString(checksum[:])
	baseName := "dbtestpg"
	if p.BaseName != "" {
		baseName = p.BaseName
	}
	tmpl := fmt.Sprintf("%v_%v", baseName, schemaHex)

	var dbExists bool
	err = withNewConnection(
		"",
		func(ctx context.Context, conn *pgx.Conn) error {
			query := `
SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)
`
			err := conn.QueryRow(ctx, query, tmpl).Scan(&dbExists)
			if err != nil {
				return errors.WithStack(err)
			}
			if dbExists {
				return nil
			}
			_, err = conn.Exec(ctx, `CREATE DATABASE `+quote(tmpl))
			return errors.WithStack(err)
		},
	)
	if err != nil {
		return "", err
	}

	if dbExists {
		return tmpl, nil
	}

	err = withNewConnection(
		tmpl,
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err = conn.Exec(ctx, string(schemaSql))
			return errors.WithStack(err)
		},
	)

	if err != nil {
		_ = dropDB(tmpl)
		return "", err
	}

	return tmpl, nil
}

func quote(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
