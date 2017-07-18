package main

import (
	"fmt"
	"os"
)

type rlog struct {
	fname string
	pos   int64
	finfo os.FileInfo
}

func newRlog(fname string) (*rlog, error) {
	fi, err := os.Stat(fname)
	if err != nil {
		return nil, err
	}
	// TODO: if it's a directory, find last matching some given pattern
	if fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a file", fname)
	}
	fmt.Printf("see any inode? %#v\n", fi.Sys())
	return &rlog{
		fname: fname,
		pos:   fi.Size(),
		finfo: fi,
	}, nil
}

func (r *rlog) fileChanged() bool {
	// TODO
	return false
}

func (r *rlog) count() (int, error) {
	if r.fileChanged() {
		r.pos = 0
		// TODO: find newest file matching pattern, set r.fname
	}
	fh, err := os.Open(r.fname)
	if err != nil {
		return 0, err
	}
	if r.pos > 0 {
		pos, err := fh.Seek(r.pos, 0)
		if err != nil {
			return 0, err
		}
		r.pos = pos
	}
	var (
		cnt int
		off int
	)
	s := bufio.NewScanner(fh)
	for s.Scan() {
		cnt++
		bs := s.Bytes()
		off += len(bs)
	}
	r.pos += off
	if err := s.Err(); err != nil {
		return 0, err
	}
	return cnt, nil
}
