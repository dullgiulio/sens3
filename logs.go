package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type rlog struct {
	dir   string
	match string
	fname string
	pos   int64
}

func newRlog(dir, match string) (*rlog, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("invalid directory for log files: %s", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("invalid directory for log files: %s is not a directory", dir)
	}
	r := &rlog{
		dir:   dir,
		match: match,
	}
	latestFile, err := r.lastMod()
	if err != nil {
		return nil, fmt.Errorf("error finding logfile to work on: %s", err)
	}
	r.fname = latestFile
	fi, err = os.Stat(r.fname)
	if err != nil {
		return nil, fmt.Errorf("cannot stat candidate logfile: %s", err)
	}
	r.pos = fi.Size()
	return r, nil
}

func (r *rlog) lastMod() (string, error) {
	files, err := ioutil.ReadDir(r.dir)
	if err != nil {
		return "", fmt.Errorf("cannot determine last modified file: %s", err)
	}
	var (
		last string
		mod  time.Time
	)
	for _, fi := range files {
		fmod := fi.ModTime()
		name := fi.Name()
		if !strings.HasPrefix(name, r.match) {
			continue
		}
		if !mod.After(fmod) {
			mod = fmod
			last = name
		}
	}
	if last == "" {
		return "", fmt.Errorf("there are no files starting with %s in directory %s", r.match, r.dir)
	}
	return filepath.Join(r.dir, last), nil
}

func (r *rlog) countLines(fname string, offset int64) (int, int64, error) {
	fh, err := os.Open(fname)
	if err != nil {
		return 0, 0, fmt.Errorf("cannot open logfile for reading: %s", err)
	}
	defer fh.Close()
	if r.pos > 0 {
		pos, err := fh.Seek(offset, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("cannot seek to previous position: %s", err)
		}
		r.pos = pos
	}
	var cnt int
	s := bufio.NewScanner(fh)
	for s.Scan() {
		cnt++
		bs := s.Bytes()
		offset += int64(len(bs))
	}
	if err := s.Err(); err != nil {
		return 0, 0, fmt.Errorf("cannot scan logfile lines: %s", err)
	}
	return cnt, offset, nil
}

func (r *rlog) count() (int, error) {
	latestFile, err := r.lastMod()
	if err != nil {
		return 0, fmt.Errorf("error finding logfile to work on: %s", err)
	}
	if latestFile != r.fname {
		r.pos = 0
		r.fname = latestFile
	}
	fi, err := os.Stat(r.fname)
	if err != nil {
		return 0, fmt.Errorf("cannot stat candidate logfile: %s", err)
	}
	// XXX: this is not perfect but should handle files that are rotated. Should also check inode.
	if fi.Size() < r.pos {
		r.pos = 0
	}
	cnt, off, err := r.countLines(r.fname, r.pos)
	if err != nil {
		return 0, fmt.Errorf("error counting lines in logfile: %s", err)
	}
	r.pos = off
	return cnt, nil
}
