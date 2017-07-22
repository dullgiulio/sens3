package main

import (
	"flag"
	"log"
	"os"
	"time"
)

const (
	defaultInfluxURL = "http://HOST:8086/write?db=MY_DB"
)

func main() {
	dsnmap := dsnmap(make(map[string]*dsnentry))
	flag.Var(dsnmap, "mysql", "Named DSN to connect to database")
	nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	every := flag.Duration("default-every", 10*time.Second, "Default repetition duration for tasks that don't specify the 'every' flag")
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot read hostname: %s", err)
	}

	prods := products(make(map[string]checks))
	args := flag.Args()
	for _, arg := range args {
		if err := prods.parse(arg); err != nil {
			log.Fatal(err)
		}
	}

	results := newResults(os.Stdout)
	ts, err := initProducts(hostname, *every, prods, dsnmap)
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	if len(ts) == 0 {
		log.Fatalf("no tasks to run, exiting")
	}

	sched := newScheduler(results.ch, ts, *nworkers)
	go sched.schedule()
	results.collect()
}
