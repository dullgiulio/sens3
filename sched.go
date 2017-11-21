package main

import (
	"log"
	"sort"
	"time"
)

// taskFn is a function that is called by the scheduler to perform a task.
type taskFn func(*task) error

// task represents a named task with a function and a repeat interval.
type task struct {
	name   string
	fn     taskFn
	repeat time.Duration
	left   time.Duration
}

// newTask creates a new named task to be run every interval by calling function fn.
func newTask(name string, interval time.Duration, fn taskFn) *task {
	return &task{
		name:   name,
		fn:     fn,
		repeat: interval,
	}
}

type taskByTimeLeft []*task

func (a taskByTimeLeft) Len() int           { return len(a) }
func (a taskByTimeLeft) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a taskByTimeLeft) Less(i, j int) bool { return a[i].left < a[j].left }

// scheduler represents a set of tasks to be scheduled.
type scheduler struct {
	ts []*task
	ch chan *task
}

// newScheduler creates a scheduler for a slice of tasks and starts
// a nworkers workers goroutines to execute the tasks.
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

// work executes the tasks when they are scheduled and logs errors using standard logging.
// work is called by the scheduler on creation and shouldn't be called by scheduler users.
func (s *scheduler) work() {
	for task := range s.ch {
		if err := task.fn(task); err != nil {
			log.Printf("error: %s: %s", task.name, err)
		}
	}
}

// schedule runs forever scheduling tasks or sleeping.
func (s *scheduler) schedule() {
	if len(s.ts) == 0 {
		return
	}
	for {
		sort.Sort(taskByTimeLeft(s.ts))
		wait := s.ts[0].left
		if wait > 0 {
			time.Sleep(wait)
		}
		for i := range s.ts {
			s.ts[i].left -= wait
			if s.ts[i].left <= 0 {
				s.ts[i].left = s.ts[i].repeat
				s.ch <- s.ts[i]
			}
		}
	}
}
