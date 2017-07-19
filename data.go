package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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

type collector interface {
	collect(<-chan *result)
}

type batchCollector struct {
	endpoint string
	nbatch   int
	batchi   int // current position in batch slice
	tbatch   time.Duration
	batch    []*result
}

func newBatchCollector(endp string, nbatch int, tbatch time.Duration) *batchCollector {
	return &batchCollector{
		endpoint: endp,
		nbatch:   nbatch,
		tbatch:   tbatch,
		batch:    make([]*result, nbatch),
	}
}

func (b *batchCollector) collect(ch <-chan *result) {
	var skipTick bool // avoid flushing because of full and then timeout
	tick := time.Tick(b.tbatch)
	for {
		select {
		case res := <-ch:
			if b.batchi >= b.nbatch {
				b.flush()
				skipTick = true
			}
			b.batch[b.batchi] = res
			b.batchi++
		case <-tick:
			if skipTick {
				skipTick = false
				continue
			}
			b.flush()
		}
	}
}

func (b *batchCollector) flush() {
	var buf bytes.Buffer
	for i := 0; i < b.batchi; i++ {
		fmt.Fprintln(&buf, b.batch[i].String())
		b.batch[i] = nil
	}
	b.batchi = 0
	resp, err := http.Post(b.endpoint, "text/plain", &buf)
	if err != nil {
		log.Printf("influxdb: error posting data: %s", err)
		return
	}
	if resp.StatusCode != 204 {
		log.Printf("influxdb: error posting data: expected status 204, got %s", resp.Status)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

type printCollector struct {
	w io.Writer
}

func (p printCollector) collect(ch <-chan *result) {
	for r := range ch {
		fmt.Fprintln(p.w, r.String())
	}
}

type results struct {
	sinks []chan *result
	ch    chan *result
}

func newResults(cols []collector) *results {
	r := &results{
		sinks: make([]chan *result, len(cols)),
		ch:    make(chan *result),
	}
	for i := range cols {
		ch := make(chan *result)
		r.sinks[i] = ch
		go cols[i].collect(ch)
	}
	return r
}

func (r *results) collect() {
	for res := range r.ch {
		for i := range r.sinks {
			r.sinks[i] <- res
		}
	}
}
