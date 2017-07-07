package main

import (
	"log"
	"sort"
	"time"
)

type taskFn func(p *point) error

type task struct {
	name   string
	point  *point
	fn     taskFn
	repeat time.Duration
	left   time.Duration
}

func newTask(name string, interval time.Duration, p *point, fn taskFn) *task {
	return &task{
		name:   name,
		point:  p,
		fn:     fn,
		repeat: interval,
	}
}

type taskByTimeLeft []*task

func (a taskByTimeLeft) Len() int           { return len(a) }
func (a taskByTimeLeft) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a taskByTimeLeft) Less(i, j int) bool { return a[i].left < a[j].left }

type scheduler struct {
	ts []*task
	ch chan *task
}

func newScheduler(ts []*task, nworkers int) *scheduler {
	s := &scheduler{
		ts: ts,
		ch: make(chan *task, 0),
	}
	if len(ts) == 0 {
		return s
	}
	for i := 0; i < nworkers; i++ {
		go s.work()
	}
	return s
}

func (s *scheduler) work() {
	for task := range s.ch {
		if err := task.fn(task.point); err != nil {
			log.Printf("error: %s: %s", task.name, err)
		}
	}
}

func (s *scheduler) schedule() {
	if len(s.ts) == 0 {
		return
	}
	for {
		sort.Sort(taskByTimeLeft(s.ts))
		wait := s.ts[0].left
		for i := range s.ts {
			s.ts[i].left -= wait
			if s.ts[i].left <= 0 {
				s.ts[i].left = s.ts[i].repeat
				s.ch <- s.ts[i]
			}
		}
		time.Sleep(wait)
	}
}
