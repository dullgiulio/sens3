package main

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

func taskProc(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "http_processes"
	dir, ok := opts["dir"]
	if !ok {
		dir = "/proc"
	}
	match, ok := opts["match"]
	if !ok {
		match = "httpd"
	}
	return newTask(name, d, p, func() (string, error) {
		proc := proc(dir)
		n, err := proc.match(match)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}), nil
}

func taskLoadavg(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "loadavg"
	lavg := loadavg(runtime.NumCPU())
	return newTask(name, d, p, func() (string, error) {
		n, err := lavg.last()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}), nil
}

func (m dsnmap) taskMysqlPages(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "mysql_pages"
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
	// TODO: this is ugly that an unneeded arg is defaulted
	db := newMysql(dbent, "")
	return newTask(name, d, p, func() (string, error) {
		n, err := db.pages()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}), nil
}

func (m dsnmap) taskMysqlCachedPages(d time.Duration, p *point, opts map[string]string) (*task, error) {
	name := "mysql_cached_pages"
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
	table, ok := opts["table"]
	if !ok {
		table = "cf_cache_pages_tags"
	}
	db := newMysql(dbent, table)
	return newTask(name, d, p, func() (string, error) {
		n, err := db.cached()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}), nil
}

func taskCountNewlines(d time.Duration, p *point, opts map[string]string) (*task, error) {
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
	return newTask(name, d, p, func() (string, error) {
		n, err := rlog.count()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}), nil
}

type taskMaker func(every time.Duration, point *point, opts map[string]string) (*task, error)

type tasks map[string]taskMaker

func makeTasks(dsn dsnmap) tasks {
	return map[string]taskMaker{
		"proc":   taskProc,
		"pages":  dsn.taskMysqlPages,
		"cached": dsn.taskMysqlCachedPages,
		"reqs":   taskCountNewlines,
		"load":   taskLoadavg,
	}
}

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
