package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	defaultInfluxURL = "http://HOST:8086/write?db=MY_DB"
)

func configure() (*scheduler, *results, error) {
	dsnmap := dsnmap(make(map[string]*dsnentry))
	flag.Var(dsnmap, "mysql", "Named DSN to connect to database")
	nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	every := flag.Duration("default-every", 10*time.Second, "Default repetition duration for tasks that don't specify the 'every' flag")
	hostname := flag.String("hostname", "", "Hostname to send in measurements to identify this machine; default is autodetect")
	flag.Parse()
	hname, err := os.Hostname()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read hostname: %v", err)
	} else {
		*hostname = hname
	}
	prods := products(make(map[string]checks))
	args := flag.Args()
	for _, arg := range args {
		if err := prods.parse(arg); err != nil {
			log.Fatal(err)
			return nil, nil, fmt.Errorf("cannot parse argument '%s': %v", arg, err)
		}
	}
	results := newResults(os.Stdout)
	ts, err := initProducts(*hostname, *every, prods, dsnmap)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot init tasks: %v", err)
	}
	if len(ts) == 0 {
		return nil, nil, fmt.Errorf("no tasks to run, exiting")
	}
	return newScheduler(results.ch, ts, *nworkers), results, nil
}

func main() {
	sched, results, err := configure()
	if err != nil {
		log.Fatalf("sens3: cannot start: %v", err)
	}
	go sched.schedule()
	results.collect()
}
