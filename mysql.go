package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	queryStatus     = "SHOW STATUS WHERE `variable_name` = '%s'"
	queryPages      = "SELECT COUNT(*) FROM pages WHERE deleted=0 AND hidden=0 AND doktype=1"
	queryCachePages = "SELECT COUNT( DISTINCT tag ) FROM %s"
	querySyslog     = "SELECT type, COUNT(type) FROM sys_log WHERE tstamp > %d GROUP BY type"
)

type mysql struct {
	entry           *dsnentry
	queryPages      string
	queryStatusConn string
	queryCachePages string
	lastSyslog      time.Time
}

type syslog struct {
	db, file, cache, ext, err, setting, login int
}

func newMysql(entry *dsnentry, cacheTable string) *mysql {
	return &mysql{
		entry:           entry,
		queryPages:      queryPages,
		queryStatusConn: fmt.Sprintf(queryStatus, "Threads_connected"),
		queryCachePages: fmt.Sprintf(queryCachePages, cacheTable),
	}
}

func (m *mysql) statusVar(query string) (int, error) {
	var (
		key string
		cnt int
	)
	row := m.entry.db.QueryRow(query)
	if err := row.Scan(&key, &cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (m *mysql) conn() (int, error) {
	return m.statusVar(m.queryStatusConn)
}

func (m *mysql) syslog() (*syslog, error) {
	if m.lastSyslog.IsZero() {
		m.lastSyslog = time.Now()
	}
	// TODO: Use prepared statements?
	rows, err := m.entry.db.Query(fmt.Sprintf(querySyslog, m.lastSyslog.Unix()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m.lastSyslog = time.Now()
	var s syslog
	for rows.Next() {
		var t, v int
		if err = rows.Scan(&t, &v); err != nil {
			return nil, err
		}
		switch t {
		case 1:
			s.db = v
		case 2:
			s.file = v
		case 3:
			s.cache = v
		case 4:
			s.ext = v
		case 5:
			s.err = v
		case 254:
			s.setting = v
		case 255:
			s.login = v
		}
	}
	return &s, nil
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
