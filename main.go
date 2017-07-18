package main

import (
	"bytes"
	"database/sql"
	"errors"
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
	dbs dsnmap
	ch  chan *result
}

func newResults(dbs dsnmap) *results {
	return &results{
		dbs: dbs,
		ch:  make(chan *result),
	}
}

func (r *results) collect() {
	for r := range r.ch {
		// TODO: collect many (?) and submit to influxdb
		fmt.Println(r.String())
	}
}

func (r *results) taskProc(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "http_processes"
	dir, ok := opts["dir"]
	if !ok {
		dir = "/proc"
	}
	match, ok := opts["match"]
	if !ok {
		match = "httpd"
	}
	return newTask(name, d, p, func(p *point) error {
		proc := proc(dir)
		n, err := proc.match(match)
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), p)
		return nil
	}), nil
}

func (r *results) taskMysqlPages(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "mysql_pages"
	dbname, ok := opts["db"]
	if !ok {
		return nil, fmt.Errorf("required option 'db'")
	}
	dbent, ok := r.dbs[dbname]
	if !ok {
		return nil, fmt.Errorf("database connection %s not defined; define it using -mysql", dbname)
	}
	if err := dbent.connect(); err != nil {
		return nil, err
	}
	// TODO: this is ugly that an unneeded arg is defaulted
	db := newMysql(dbent, "")
	return newTask(name, d, p, func(p *point) error {
		n, err := db.pages()
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), p)
		return nil
	}), nil
}

func (r *results) taskMysqlCachedPages(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "mysql_cached_pages"
	dbname, ok := opts["db"]
	if !ok {
		return nil, fmt.Errorf("required option 'db'")
	}
	dbent, ok := r.dbs[dbname]
	if !ok {
		return nil, fmt.Errorf("database connection %s not defined; define it using -mysql", dbname)
	}
	if err := dbent.connect(); err != nil {
		return nil, err
	}
	table, ok := opts["table"]
	if !ok {
		table = "cf_cache_pages_tags"
	}
	db := newMysql(dbent, table)
	return newTask(name, d, p, func(p *point) error {
		n, err := db.cached()
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), p)
		return nil
	}), nil
}

func (r *results) taskCountNewlines(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "requests"
	dir, ok := opts["dir"]
	if !ok {
		return nil, fmt.Errorf("required option 'dir'")
	}
	match, ok := opts["match"]
	if !ok {
		return nil, fmt.Errorf("required option 'match'")
	}
	rlog, err := newRlog(dir, match)
	if err != nil {
		return nil, fmt.Errorf("cannot init log file reader: %s", err)
	}
	return newTask(name, d, p, func(p *point) error {
		n, err := rlog.count()
		if err != nil {
			return err
		}
		r.ch <- newResult(name, fmt.Sprintf("%d", n), p)
		return nil
	}), nil
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
			if _, ok := cs[name]; ok {
				return nil, fmt.Errorf("check %s repeated", name)
			}
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
		if _, ok := opts[optparts[0]]; ok {
			return nil, fmt.Errorf("check %s: option %s repeated", name, optparts[0])
		}
		opts[optparts[0]] = optparts[1]
	}
	if name != "" {
		if _, ok := cs[name]; ok {
			return nil, fmt.Errorf("check %s repeated", name)
		}
		cs[name] = opts
	}
	return cs, nil
}

type products map[string]checks

func (p products) parse(s string) error {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%s should be in format PRODUCT.STAGE:CHECK,ARGS...,CHECK,ARGS...", s)
	}
	if _, ok := p[parts[0]]; ok {
		return fmt.Errorf("system %s specified more than once", parts[0])
	}
	cs, err := parseChecks(parts[1])
	if err != nil {
		return fmt.Errorf("cannot parse checks for system %s: %s", parts[0], err)
	}
	p[parts[0]] = cs
	return nil
}

type taskMaker func(every time.Duration, point *point, opts map[string]string) (*task, error)

type tasks map[string]taskMaker

func (ts tasks) setup(task string, p *point, opts map[string]string) (*task, error) {
	fn, ok := ts[task]
	if !ok {
		return nil, errors.New("not a valid task")
	}
	val, ok := opts["every"]
	if !ok {
		return nil, errors.New("options 'every' is required")
	}
	every, err := time.ParseDuration(val)
	if err != nil {
		return nil, fmt.Errorf("option 'every' is invalid: %s", err)
	}
	t, err := fn(every, p, opts)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func initProducts(prods products, res *results) ([]*task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("cannot read hostname: %s", err)
	}
	ts := make([]*task, 0)
	var tsmap tasks = map[string]taskMaker{
		"proc":   res.taskProc,
		"pages":  res.taskMysqlPages,
		"cached": res.taskMysqlCachedPages,
		"reqs":   res.taskCountNewlines,
	}
	for name, checks := range prods {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s should be in format SYSTEM.STAGE", name)
		}
		point := &point{hostname, parts[0], parts[1]}
		for cname, opts := range checks {
			t, err := tsmap.setup(cname, point, opts)
			if err != nil {
				return nil, fmt.Errorf("%s: task %s: %s", name, cname, err)
			}
			ts = append(ts, t)
		}
	}
	return ts, nil
}

func main() {
	dsnmap := dsnmap(make(map[string]*dsnentry))
	flag.Var(dsnmap, "mysql", "Named DSN to connect to database")
	nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	flag.Parse()

	prods := products(make(map[string]checks))
	args := flag.Args()
	for _, arg := range args {
		if err := prods.parse(arg); err != nil {
			log.Fatal(err)
		}
	}
	results := newResults(dsnmap)
	ts, err := initProducts(prods, results)
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	if len(ts) == 0 {
		log.Fatalf("no tasks to run, exiting")
	}

	sched := newScheduler(ts, *nworkers)
	go sched.schedule()
	results.collect()
}
