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
	db              *sql.DB
	queryPages      string
	queryCachePages string
}

func newMysql(dsn, cacheTable string) (*mysql, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	m := &mysql{
		db:              db,
		queryPages:      queryPages,
		queryCachePages: fmt.Sprintf(queryCachePages, cacheTable),
	}
	if err := m.db.Ping(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *mysql) count(query string) (int, error) {
	var cnt int
	row := m.db.QueryRow(query)
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

func (m *mysql) close() {
	m.db.Close()
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

func main() {
	// TODO: these should be per group of task
	product := flag.String("product", "", "Name of product monitored")
	stage := flag.String("stage", "prod", "Stage of product monitored")
	cacheTable := flag.String("mysql-cached-table", "cf_cache_pages_tags", "Name of Caching Framework cache_pages_tags table")
	dsn := flag.String("mysql-dsn", "user:password@tcp(localhost:3306)/database", "DSN to connect to database")
	nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	flag.Parse()

	if *product == "" {
		log.Fatalf("error: -product is mandatory")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	// TODO: table names should be given in tasks, not here.
	db, err := newMysql(*dsn, *cacheTable)
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	point := &point{hostname, *product, *stage}
	results := newResults(point)

	// TODO: make this dynamic based on args
	ts := make([]*task, 0)
	// TODO: Interval times should be from arguments
	ts = append(ts, results.taskProc(10*time.Minute, "/proc", "httpd"))
	ts = append(ts, results.taskMysqlPages(25*time.Minute, db))
	ts = append(ts, results.taskMysqlCachedPages(30*time.Minute, db))
	// TODO: write test to count HTTP requests from logs?

	sched := newScheduler(ts, *nworkers)
	go sched.schedule()
	results.collect()
}
