package main

import (
	"flag"
	"log"
	"os"
	"time"
)

func main() {
	dsnmap := dsnmap(make(map[string]*dsnentry))
	flag.Var(dsnmap, "mysql", "Named DSN to connect to database")
	nworkers := flag.Int("workers", 1, "Number of parallel task runners")
	influxdb := flag.String("influxdb", "http://localhost:8086/write?db=mydb", "Address of InfluxDB write endpoint")
	verbose := flag.Bool("verbose", false, "Print measurements to stdout")
	nbatch := flag.Int("influx-nbatch", 20, "Max number of measurements to cache")
	tbatch := 10 * time.Second // TODO: make flag influx-time
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

	var collectors []collector
	if *influxdb != "" {
		collectors = append(collectors, newBatchCollector(*influxdb, *nbatch, tbatch))
	}
	if *verbose {
		collectors = append(collectors, printCollector{os.Stdout})
	}

	results := newResults(collectors)
	ts, err := initProducts(hostname, prods, dsnmap)
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
