# sens3 - Custom system sensors for InfluxDB

sens3 is a collection of custom basic sensors that outputs data to stdout in InfluxDB format.

sens3 has an internal scheduler and can handle several groups of tests with different check intervals (per test.)

Tests are specified in the command line in the following syntax:
```
<group-name>.<group-stage>:[<measurement>[,conf-key=conf-value],<measurement>...]
```

For example:
```
chat.prod:logcat,dir=/var/log,match=access_log,logcat,dir=/var/log,match=error_log,proc,every=5m,match=httpd
```

## Measurements

Available measurements and their optiosn with default values.

* proc (name=procs, every=1m, dir=/proc, match=httpd)
* load (name=load, every=1m)
* pages (name=pages, every=1m, db=null)
* cached (name=cached, every=1m, table=cf_cache_pages_tags, db=null)
* dbconn (name=dbconn, every=1m, db=null)
* logcat (name=logcat, every=1m, dir=empty, match=access_log)
* syslog (name=syslog, every=1m, db=null)

## Usage

Typical usage is running sens3 via [dullgiulio/influxin](https://github.com/dullgiulio/influxin) that allows
automatic restarts and forwarding of the gathered data to InfluxDB in batches.

## Building

```
$ go get github.com/dullgiulio/sens3
```
