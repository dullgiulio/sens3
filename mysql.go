package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	queryPages      = "SELECT COUNT(*) FROM pages WHERE deleted=0 AND hidden=0 AND doktype=1"
	queryCachePages = "SELECT COUNT( DISTINCT tag ) FROM %s"
)

type mysql struct {
	entry           *dsnentry
	queryPages      string
	queryCachePages string
}

func newMysql(entry *dsnentry, cacheTable string) *mysql {
	return &mysql{
		entry:           entry,
		queryPages:      queryPages,
		queryCachePages: fmt.Sprintf(queryCachePages, cacheTable),
	}
}

func (m *mysql) count(query string) (int, error) {
	var cnt int
	row := m.entry.db.QueryRow(query)
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (m *mysql) pages() (int, error) {
	return m.count(m.queryPages)
}

func (m *mysql) cached() (int, error) {
	return m.count(m.queryCachePages)
}

type dsnentry struct {
	dsn string
	db  *sql.DB
	err error
}

func (e *dsnentry) connect() error {
	// Already tried, but there was an error.
	if e.err != nil {
		return e.err
	}
	// Already connected successfully.
	if e.db != nil {
		return nil
	}
	e.db, e.err = connectMysql(e.dsn)
	if e.err != nil {
		e.err = fmt.Errorf("cannot connect to database: %s", e.err)
	}
	return e.err
}

func connectMysql(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

type dsnmap map[string]*dsnentry

func (m dsnmap) String() string {
	if len(m) == 0 {
		return "name=user:password@tcp(localhost:3306)/database"
	}
	// TODO: make nicer, print key=val.dsn
	return fmt.Sprintf("[array %d]", len(m))
}

func (m dsnmap) Set(v string) error {
	parts := strings.SplitN(v, "=", 2)
	if _, ok := m[parts[0]]; ok {
		return fmt.Errorf("duplicated mysql DSN %s", parts[0])
	}
	m[parts[0]] = &dsnentry{dsn: parts[1]}
	return nil
}
