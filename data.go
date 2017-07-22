package main

import (
	"fmt"
	"io"
	"time"
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
	value int
	name  string
	point string
}

func newResult(name string, value int, point *point) *result {
	return &result{
		time:  time.Now(),
		name:  name,
		value: value,
		point: point.String(),
	}
}

func (r *result) String() string {
	return fmt.Sprintf("%s,%s value=%d %d",
		r.name, r.point, r.value, r.time.UnixNano())
}

type results struct {
	w  io.Writer
	ch chan *result
}

func newResults(w io.Writer) *results {
	return &results{
		w:  w,
		ch: make(chan *result),
	}
}

func (r *results) collect() {
	for res := range r.ch {
		fmt.Fprintln(r.w, res.String())
	}
}
