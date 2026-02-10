package go_test_pg

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/pkg/errors"
)

const defaultTimeout = 30 * time.Second

type Fixture struct {
	Query  string
	Params []interface{}
}

type options struct {
	poolConfig func(*pgxpool.Config)
}

// Option configures database creation and connection behavior.
type Option func(*options)

// WithPoolConfig returns an Option that modifies the pgxpool.Config
// used to create the test database pool.
func WithPoolConfig(fn func(*pgxpool.Config)) Option {
	return func(o *options) {
		o.poolConfig = fn
	}
}

type Pgpool struct {
	// BaseName is the prefix of template and temporary databases.
	// Default is dbtestpg.
	BaseName string
	// Name of schema file. If empty, create empty database.
	SchemaFile string // schema file name
	// If true, skip all database tests.
	Skip bool
	// Options applied to all databases created by this pool.
	Options []Option

	m    sync.RWMutex
	err  error
	tmpl string
	rnd  *rand.Rand
}

// WithFixtures creates database from template database, and initializes it
// with fixtures from `fixtures` array
func (p *Pgpool) WithFixtures(t testing.TB, fixtures []Fixture, opts ...Option) *pgxpool.Pool {
	pool := p.WithEmpty(t, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, f := range fixtures {
		if _, err := pool.Exec(ctx, f.Query, f.Params...); err != nil {
			t.Fatalf(
				"can't load fixture at idx %v: %+v",
				i, errors.WithStack(err),
			)
		}
	}
	return pool
}

// WithStdFixtures creates database from template database, and initializes it
// with fixtures from `fixtures` array
func (p *Pgpool) WithStdFixtures(t testing.TB, fixtures []Fixture, opts ...Option) *sql.DB {
	db := p.WithStdEmpty(t, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, f := range fixtures {
		if _, err := db.ExecContext(ctx, f.Query, f.Params...); err != nil {
			t.Fatalf("can't load fixture at idx %v: %+v",
				i, errors.WithStack(err))
		}
	}
	return db
}

// WithSQLs creates database from template database, and initializes it
// with fixtures from `sqls` array
func (p *Pgpool) WithSQLs(t testing.TB, sqls []string, opts ...Option) *pgxpool.Pool {
	pool := p.WithEmpty(t, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, s := range sqls {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf(
				"can't load fixture at idx %v: %+v",
				i, errors.WithStack(err),
			)
		}
	}
	return pool
}

// WithStdSQLs creates database from template database, and initializes it
// with fixtures from `sqls` array
func (p *Pgpool) WithStdSQLs(t testing.TB, sqls []string, opts ...Option) *sql.DB {
	db := p.WithStdEmpty(t, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for i, s := range sqls {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("can't load fixture at idx %v: %+v",
				i, errors.WithStack(err))
		}
	}
	return db
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

// Register pgx.ConnConfig with std driver.
// Return connection string for database/sql and error.
func (p *Pgpool) registerStdConfig(t testing.TB,
	dbName string, opts ...Option) (string, error) {

	var o options
	for _, opt := range p.Options {
		opt(&o)
	}
	for _, opt := range opts {
		opt(&o)
	}

	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return "", errors.WithStack(err)
	}
	connConfig.Tracer = &tracelog.TraceLog{
		Logger:   newLogger(t),
		LogLevel: tracelog.LogLevelTrace,
	}
	connConfig.Database = dbName
	if o.poolConfig != nil {
		poolCfg := &pgxpool.Config{ConnConfig: connConfig}
		o.poolConfig(poolCfg)
		if connConfig.Database != dbName {
			return "", errors.New(
				"WithPoolConfig must not change the database name")
		}
	}
	return stdlib.RegisterConnConfig(connConfig), nil
}

func (p *Pgpool) createRndDB(t testing.TB) (string, error) {
	tmpl := p.getTmpl(t)
	dbName := fmt.Sprintf("%v_%v", tmpl, p.rnd.Int31())

	return dbName, p.createDB(dbName, tmpl)
}

func (p *Pgpool) createRndDBPool(
	t testing.TB, opts ...Option) (pool *pgxpool.Pool, dbName string) {

	var err error
	dbName, err = p.createRndDB(t)
	if err != nil {
		t.Fatal(err)
	}

	var o options
	for _, opt := range p.Options {
		opt(&o)
	}
	for _, opt := range opts {
		opt(&o)
	}

	var cfg *pgxpool.Config
	cfg, err = pgxpool.ParseConfig("")
	if err != nil {
		_ = dropDB(dbName)
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = dbName
	if o.poolConfig != nil {
		o.poolConfig(cfg)
		if cfg.ConnConfig.Database != dbName {
			_ = dropDB(dbName)
			t.Fatal("WithPoolConfig must not change the database name")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_ = dropDB(dbName)
		t.Fatal(err)
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
func (p *Pgpool) WithEmpty(t testing.TB, opts ...Option) *pgxpool.Pool {
	pool, dbName := p.createRndDBPool(t, opts...)
	t.Cleanup(func() {
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
	})
	return pool
}

// WithStdEmpty creates empty database from template database, that was
// created from `schema` file.
func (p *Pgpool) WithStdEmpty(t testing.TB, opts ...Option) *sql.DB {
	db, cleanupFn := p.newStdDBWithCleanup(t, opts...)
	if cleanupFn != nil {
		t.Cleanup(func() {
			if err := cleanupFn(); err != nil {
				t.Error(err)
			}
		})
	}
	return db
}

func (p *Pgpool) newStdDBWithCleanup(
	t testing.TB, opts ...Option) (db *sql.DB, cleanupFn func() error) {

	dbName, err := p.createRndDB(t)
	if err != nil {
		t.Fatal(err)
		return nil, nil
	}

	connString, err := p.registerStdConfig(t, dbName, opts...)
	if err != nil {
		_ = dropDB(dbName)
		t.Fatal(err)
		return nil, nil
	}

	db, err = sql.Open("pgx", connString)
	if err != nil {
		stdlib.UnregisterConnConfig(connString)
		_ = dropDB(dbName)
		t.Fatal(err)
		return nil, nil
	}

	cleanupFn = func() error {
		defer stdlib.UnregisterConnConfig(connString)
		stats := db.Stats()
		if stats.InUse > 0 {
			return errors.Errorf(
				"unreleased connections exists: %v, can't drop database %v",
				stats.InUse, dbName)
		}
		err := db.Close()
		if err != nil {
			return errors.Errorf("Can't close DB %v: %v", dbName, err)
		}
		err = dropDB(dbName)
		if err != nil {
			return errors.Errorf("Can't drop DB %v: %v", dbName, err)
		}
		return nil
	}
	return db, cleanupFn
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

// Creates template db, populates with SQLs from schema file and return name
// of the new database. If database is exists, just return its name.
func (p *Pgpool) createTemplateDB() (string, error) {
	if p.SchemaFile == "" {
		return "template1", nil
	}
	schemaSql, err := os.ReadFile(p.SchemaFile)
	if err != nil {
		return "", errors.WithStack(err)
	}
	checksum := md5.Sum(schemaSql)
	schemaHex := hex.EncodeToString(checksum[:])
	baseName := "dbtestpg"
	if p.BaseName != "" {
		baseName = p.BaseName
	}
	tmplDbName := fmt.Sprintf("%v_%v", baseName, schemaHex)

	// ID of the advisory lock. Lock would be taken on master database (not
	// on database we are going to create) and prevent from parallel creation
	// of the same database from separate processes.
	lockID := int64(binary.BigEndian.Uint64(checksum[:8]))

	err = withNewConnection(
		"",
		func(ctx context.Context, conn *pgx.Conn) error {
			var dbExists bool
			err := conn.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`,
				tmplDbName).Scan(&dbExists)
			if err != nil {
				return errors.WithStack(err)
			}
			if dbExists {
				return nil
			}

			// If we need to create a database, take an advisory lock on
			// master database to prevent parallel creation of databases
			// from several test processes.
			_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID)
			if err != nil {
				return errors.WithStack(err)
			}

			// Check again for database existence. Database may be created
			// in parallel process while waiting for the lock.
			err = conn.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`,
				tmplDbName).Scan(&dbExists)
			if err != nil {
				return errors.WithStack(err)
			}
			if dbExists {
				return nil
			}

			_, err = conn.Exec(ctx, `CREATE DATABASE `+quote(tmplDbName))
			if err != nil {
				return errors.WithStack(err)
			}

			err = withNewConnection(
				tmplDbName,
				func(ctx context.Context, conn *pgx.Conn) error {
					_, err = conn.Exec(ctx, string(schemaSql))
					return errors.WithStack(err)
				},
			)

			if err != nil {
				_ = dropDB(tmplDbName)
				return err
			}

			return nil
		},
	)

	if err != nil {
		return "", err
	}

	return tmplDbName, nil
}

func quote(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
