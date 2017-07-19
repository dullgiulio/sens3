package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

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
