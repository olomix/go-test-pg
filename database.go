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

type Pgpool interface {
	// WithFixtures creates database from template database, and initializes it
	// with fixtures from `fixtures` array
	WithFixtures(t testing.TB, fixtures []Fixture) (*pgxpool.Pool, func())
	// WithSQLs creates database from template database, and initializes it
	// with fixtures from `sqls` array
	WithSQLs(t testing.TB, sqls []string) (*pgxpool.Pool, func())
	// WithEmpty creates empty database from template database, that was
	// created from `schema` file.
	WithEmpty(t testing.TB) (*pgxpool.Pool, func())
}

type pgpool struct {
	m        sync.RWMutex
	err      error
	uri      string
	baseName string
	schema   string // schema file name
	tmpl     string
	rnd      *rand.Rand
}

func (p *pgpool) WithFixtures(
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

func (p *pgpool) WithSQLs(t testing.TB, sqls []string) (*pgxpool.Pool, func()) {
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

func (p *pgpool) getTmpl(t testing.TB) string {
	if p.uri == "" {
		t.Skip("database uri is not set")
	}
	t.Helper()
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
	p.tmpl, p.err = p.createTemplateDB()
	err = p.err
	p.m.Unlock()

	if err != nil {
		t.Fatalf("%+v", err)
	}

	return p.tmpl
}

func (p *pgpool) createRndDB(t testing.TB) (*pgxpool.Pool, string) {
	tmpl := p.getTmpl(t)
	dbName := fmt.Sprintf("%v_%v", tmpl, p.rnd.Int31())
	err := p.createDB(dbName, tmpl)

	cfg, err := pgxpool.ParseConfig(p.uri)
	if err != nil {
		_ = dropDB(p.uri, dbName)
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = dbName

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		_ = dropDB(p.uri, dbName)
		t.Fatal()
	}

	return pool, dbName
}

func withNewConnection(
	uri, dbName string,
	fn func(context.Context, *pgx.Conn) error,
) (err error) {
	var cfg *pgx.ConnConfig
	cfg, err = pgx.ParseConfig(uri)
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

func dropDB(uri, dbName string) error {
	return withNewConnection(
		uri, "",
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "DROP DATABASE "+quote(dbName))
			return errors.WithStack(err)
		},
	)
}

func (p *pgpool) WithEmpty(t testing.TB) (*pgxpool.Pool, func()) {
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
		err := dropDB(p.uri, dbName)
		if err != nil {
			t.Errorf("Can't drop DB %v: %v", dbName, err)
		}
	}
}

func (p *pgpool) createDB(name, tmplName string) error {
	query := `CREATE DATABASE ` + quote(name)
	if tmplName != "" {
		query += ` WITH TEMPLATE ` + quote(tmplName)
	}

	return withNewConnection(
		p.uri, "",
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, query)
			return errors.WithStack(err)
		},
	)
}

func (p *pgpool) createTemplateDB() (string, error) {
	schemaSql, err := ioutil.ReadFile(p.schema)
	if err != nil {
		return "", errors.WithStack(err)
	}
	checksum := md5.Sum(schemaSql)
	schemaHex := hex.EncodeToString(checksum[:])
	tmpl := fmt.Sprintf("%v_%v", p.baseName, schemaHex)

	var dbExists bool
	err = withNewConnection(
		p.uri, "",
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
		p.uri, tmpl,
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err = conn.Exec(ctx, string(schemaSql))
			return errors.WithStack(err)
		},
	)

	if err != nil {
		_ = dropDB(p.uri, tmpl)
		return "", err
	}

	return tmpl, nil
}

func quote(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// NewPool create new Pgpool interface. It won't connect to database
// until first reuse. `schema` file must exists and be valid SQL script.
func NewPool(dbUri, schema, baseName string) Pgpool {
	if dbUri != "" {
		if baseName == "" {
			panic("baseName is required if database uri is set")
		}
		if schema == "" {
			panic("schema file name is required if database uri is set")
		}
	}
	return &pgpool{
		uri:      dbUri,
		baseName: baseName,
		schema:   schema,
		rnd: rand.New(
			rand.NewSource(time.Now().UnixNano() + int64(os.Getpid())),
		),
	}
}
