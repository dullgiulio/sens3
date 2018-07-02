package main

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

func taskProc(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dir, ok := opts["dir"]
	if !ok {
		dir = "/proc"
	}
	match, ok := opts["match"]
	if !ok {
		match = "httpd"
	}
	return newTask(name, d, func(t *task) error {
		proc := proc(dir)
		n, err := proc.match(match)
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

func taskLoadavg(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	lavg := loadavg(runtime.NumCPU())
	return newTask(name, d, func(t *task) error {
		n, err := lavg.last()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

func (m dsnmap) mysqlInit(opts map[string]string) (*dsnentry, error) {
	dbname, ok := opts["db"]
	if !ok {
		return nil, fmt.Errorf("required option 'db'")
	}
	dbent, ok := m[dbname]
	if !ok {
		return nil, fmt.Errorf("database connection %s not defined; define it using -mysql", dbname)
	}
	if err := dbent.connect(); err != nil {
		return nil, err
	}
	return dbent, nil
}

func (m dsnmap) taskMysqlPages(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dbent, err := m.mysqlInit(opts)
	if err != nil {
		return nil, err
	}
	// TODO: this is ugly that an unneeded arg is defaulted
	db := newMysql(dbent, "")
	return newTask(name, d, func(t *task) error {
		n, err := db.pages()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

func (m dsnmap) taskMysqlSyslog(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dbent, err := m.mysqlInit(opts)
	if err != nil {
		return nil, err
	}
	// TODO: this is ugly that an unneeded arg is defaulted
	db := newMysql(dbent, "")
	db.lastSyslog = time.Now().Add(-d)
	return newTask(name, d, func(t *task) error {
		s, err := db.syslog()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, s.db, p, "type=db")
		ch <- newResult(t.name, s.file, p, "type=file")
		ch <- newResult(t.name, s.cache, p, "type=cache")
		ch <- newResult(t.name, s.ext, p, "type=ext")
		ch <- newResult(t.name, s.err, p, "type=err")
		ch <- newResult(t.name, s.setting, p, "type=settings")
		ch <- newResult(t.name, s.login, p, "type=login")
		return nil
	}), nil
}

func (m dsnmap) taskMysqlCachedPages(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dbent, err := m.mysqlInit(opts)
	if err != nil {
		return nil, err
	}
	table, ok := opts["table"]
	if !ok {
		table = "cf_cache_pages_tags"
	}
	db := newMysql(dbent, table)
	return newTask(name, d, func(t *task) error {
		n, err := db.cached()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

func (m dsnmap) taskMysqlConn(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dbent, err := m.mysqlInit(opts)
	if err != nil {
		return nil, err
	}
	db := newMysql(dbent, "")
	return newTask(name, d, func(t *task) error {
		n, err := db.conn()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

func taskCountNewlines(name string, d time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	dir, ok := opts["dir"]
	if !ok {
		return nil, fmt.Errorf("required option 'dir'")
	}
	match, ok := opts["match"]
	if !ok {
		match = "access_log"
	}
	contains := []byte(opts["contains"])
	rlog, err := newRlog(dir, match, contains)
	if err != nil {
		return nil, fmt.Errorf("cannot init log file reader: %s", err)
	}
	return newTask(name, d, func(t *task) error {
		n, err := rlog.count()
		if err != nil {
			return err
		}
		ch <- newResult(t.name, n, p, "")
		return nil
	}), nil
}

type taskMaker func(name string, every time.Duration, ch chan<- *result, point *point, opts map[string]string) (*task, error)

type tasks map[string]taskMaker

func makeTasks(dsn dsnmap) tasks {
	return map[string]taskMaker{
		"proc":   taskProc,
		"pages":  dsn.taskMysqlPages,
		"cached": dsn.taskMysqlCachedPages,
		"dbconn": dsn.taskMysqlConn,
		"syslog": dsn.taskMysqlSyslog,
		"logcat": taskCountNewlines,
		"load":   taskLoadavg,
	}
}

func (ts tasks) setup(task string, every time.Duration, ch chan<- *result, p *point, opts map[string]string) (*task, error) {
	fn, ok := ts[task]
	if !ok {
		return nil, errors.New("not a valid task")
	}
	var err error
	val, ok := opts["every"]
	if ok {
		every, err = time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("option 'every' is invalid: %s", err)
		}
	}
	name, ok := opts["name"]
	if !ok {
		name = task
	}
	t, err := fn(name, every, ch, p, opts)
	if err != nil {
		return nil, err
	}
	return t, nil
}
