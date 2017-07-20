package main

import (
	"fmt"
	"strings"
	"time"
)

type checks map[string]map[string]string // check name : [options]

func parseChecks(s string) (checks, error) {
	parts := strings.Split(s, ",")
	cs := checks(make(map[string]map[string]string))
	var (
		name string
		opts map[string]string
	)
	for i := range parts {
		if name == "" {
			name = parts[i]
			if _, ok := cs[name]; ok {
				return nil, fmt.Errorf("check %s repeated", name)
			}
			continue
		}
		if !strings.ContainsRune(parts[i], '=') {
			cs[name] = opts
			name = parts[i]
			opts = nil
			continue
		}
		if opts == nil {
			opts = make(map[string]string)
		}
		optparts := strings.SplitN(parts[i], "=", 2)
		if _, ok := opts[optparts[0]]; ok {
			return nil, fmt.Errorf("check %s: option %s repeated", name, optparts[0])
		}
		opts[optparts[0]] = optparts[1]
	}
	if name != "" {
		if _, ok := cs[name]; ok {
			return nil, fmt.Errorf("check %s repeated", name)
		}
		cs[name] = opts
	}
	return cs, nil
}

type products map[string]checks

func initProducts(hostname string, every time.Duration, prods products, dsn dsnmap) ([]*task, error) {
	ts := make([]*task, 0)
	tsmap := makeTasks(dsn)
	for name, checks := range prods {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s should be in format SYSTEM.STAGE", name)
		}
		point := &point{hostname, parts[0], parts[1]}
		for cname, opts := range checks {
			t, err := tsmap.setup(cname, every, point, opts)
			if err != nil {
				return nil, fmt.Errorf("%s: task %s: %s", name, cname, err)
			}
			ts = append(ts, t)
		}
	}
	return ts, nil
}

func (p products) parse(s string) error {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%s should be in format PRODUCT.STAGE:CHECK,ARGS...,CHECK,ARGS...", s)
	}
	if _, ok := p[parts[0]]; ok {
		return fmt.Errorf("system %s specified more than once", parts[0])
	}
	cs, err := parseChecks(parts[1])
	if err != nil {
		return fmt.Errorf("cannot parse checks for system %s: %s", parts[0], err)
	}
	p[parts[0]] = cs
	return nil
}
