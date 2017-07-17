package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	queryPages      = "SELECT COUNT(*) FROM pages WHERE deleted=0 AND hidden=0 AND doktype=1"
	queryCachePages = "SELECT COUNT( DISTINCT tag ) FROM %s"
)

type point struct {
	host    string
	product string
	stage   string
}

func (p *point) String() string {
	return fmt.Sprintf("host=%s,product=%s,stage=%s", p.host, p.product, p.stage)
}

type result struct {
	time  time.Time
	name  string
	value string
	point string
}

func newResult(name, value string, point *point) *result {
	return &result{
		time:  time.Now(),
		name:  name,
		value: value,
		point: point.String(),
	}
}

func (r *result) String() string {
	return fmt.Sprintf("%s,%s value=%s %d",
		r.name, r.point, r.value, r.time.UnixNano())
}

type proc string

func (p proc) cmdName(fi os.FileInfo) (string, error) {
	fname := filepath.Join(string(p), fi.Name(), "cmdline")
	f, err := os.Open(fname)
	if err != nil {
		return "", fmt.Errorf("cannot open cmdline file: %s", err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return "", fmt.Errorf("cannot read cmdline file: %s", err)
	}
	n := bytes.IndexByte(buf, 0)
	if n < 0 {
		n = len(buf)
	}
	return string(buf[:n]), nil
}

func (p proc) match(substr string) (int, error) {
	d, err := os.Open(string(p))
	if err != nil {
		return 0, fmt.Errorf("cannot open directory for listing: %s", err)
	}
	fis, err := d.Readdir(-1)
	if err != nil {
		d.Close()
		return 0, fmt.Errorf("list proc directory: %s", err)
	}
	d.Close()
	var matching int
	for i := range fis {
		name := fis[i].Name()
		if name[0] < '0' || name[0] > '9' {
			continue
		}
		arg0, err := p.cmdName(fis[i])
		if err != nil {
			return 0, err
		}
		if strings.Contains(arg0, substr) {
			matching++
		}
	}
	return matching, nil
}

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

type results struct {
	point *point
	ch    chan *result
}

func newResults(p *point) *results {
	return &results{
		point: p,
		ch:    make(chan *result),
	}
}

func (r *results) collect() {
	for r := range r.ch {
		// TODO: collect many (?) and submit to influxdb
		fmt.Println(r.String())
	}
}

func (r *results) taskProc(d time.Duration, dir, match string) *task {
	name := "http_processes"
	return newTask(name, d, func() error {
		proc := proc(dir)
		n, err := proc.match(match)
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), r.point)
		return nil
	})
}

func (r *results) taskMysqlPages(d time.Duration, db *mysql) *task {
	name := "mysql_pages"
	return newTask(name, d, func() error {
		n, err := db.pages()
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), r.point)
		return nil
	})
}

func (r *results) taskMysqlCachedPages(d time.Duration, db *mysql) *task {
	name := "mysql_cached_pages"
	return newTask(name, d, func() error {
		n, err := db.cached()
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), r.point)
		return nil
	})
}

type dsnentry struct {
	dsn string
	db  *sql.DB
	err error
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

func (m dsnmap) connect(name string) error {
	entry, ok := m[name]
	if !ok {
		return fmt.Errorf("database connection named %s has not been set with -mysql", name)
	}
	// Already tried, but there was an error.
	if entry.err != nil {
		return entry.err
	}
	// Already connected successfully.
	if entry.db != nil {
		return nil
	}
	entry.db, entry.err = connectMysql(entry.dsn)
	return entry.err
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

type checks map[string]map[string]string // check name : [options]

func parseChecks(s string) (checks, error) {
	parts := strings.Split(s, ",")
	cs := checks(make(map[string]map[string]string))
	var (
		name string
		opts map[string]string
	)
	for i := range parts {
		if name == "" {
			name = parts[i]
			// TODO: check that the check is not already in the map
			continue
		}
		if !strings.ContainsRune(parts[i], '=') {
			cs[name] = opts
			name = parts[i]
			opts = nil
			continue
		}
		if opts == nil {
			opts = make(map[string]string)
		}
		optparts := strings.SplitN(parts[i], "=", 2)
		// TODO: check that this option is not already in the map
		opts[optparts[0]] = optparts[1]
	}
	if name != "" && opts != nil {
		// TODO: check that the check is not already in the map
		cs[name] = opts
	}
	return cs, nil
}

type products map[string]checks

func (p products) parse(s string) error {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("error: %s should be in format PRODUCT.STAGE:CHECK,ARGS...,CHECK,ARGS...", s)
	}
	if _, ok := p[parts[0]]; ok {
		return fmt.Errorf("error: system %s specified more than once", parts[0])
	}
	cs, err := parseChecks(parts[1])
	if err != nil {
		return fmt.Errorf("error for system %s: %s", parts[0], err)
	}
	p[parts[0]] = cs
	return nil
}

// TODO: Parse: sens3 -mysql portal='DSN' portal.dev:pages,every=10m,db=portal,cached,every=15m,db=portal,proc,every=5m,match=httpd ...

func main() {
	dsnmap := dsnmap(make(map[string]*dsnentry))
	flag.Var(dsnmap, "mysql", "Named DSN to connect to database")
	//nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("error: %s", err)
	}

	prods := products(make(map[string]checks))
	args := flag.Args()
	for _, arg := range args {
		if err := prods.parse(arg); err != nil {
			log.Fatal(err)
		}
	}
	ts := make([]*task, 0)
	for name, checks := range prods {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			log.Fatalf("error: %s should be in format SYSTEM.STAGE", name)
		}
		point := &point{hostname, parts[0], parts[1]}
		results := newResults(point)
		for cname, opts := range checks {
			// TODO: check that check type (cname) exists
			// TODO: parse "every" argument commont to all tasks
			// TODO: db arg calls connect() on dsnentry
			// TODO: create task and append to ts
			ts = append(ts, results.taskBlah(every, arg, arg))
		}
	}

	/*
		ts = append(ts, results.taskProc(10*time.Minute, "/proc", "httpd"))
		ts = append(ts, results.taskMysqlPages(25*time.Minute, db))
		ts = append(ts, results.taskMysqlCachedPages(30*time.Minute, db))
		// TODO: write test to count HTTP requests from logs?

		sched := newScheduler(ts, *nworkers)
		go sched.schedule()
		results.collect()
	*/
}
