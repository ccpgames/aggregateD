[![Build Status](https://travis-ci.org/ccpgames/ccp-aggregateD.svg?branch=master)](https://travis-ci.org/ccpgames/ccp-aggregateD)

aggregateD
===========

aggregateD is a network daemon which listens for metrics including gauges, counters, histograms, sets and events, sent over http and sends aggregates to InfluxDB. InfluxDB is a promising, but young time series database, aggregateD is intended to bring dogstatsD like functionality to Influx. It also serves to abstract database details such as credentials, db names and location away from clients and to load balance metrics over an InfluxDB cluster.  


Why HTTP?
---------

Http is used in lieu of UDP (which is used in dogstatsD and regular statsD) as it allows the client to detect if aggregateD is down and temporarily cache metrics (or send to an alternative host) until it becomes available again. Http is also the standard protocol for exchanging JSON, which is used for convenience as it is the interchange format used by InfluxDB.

Support for UDP and raw TCP may be added in the future.

Usage and Configuration
-----------------------

Usuage:
  ./aggregated -config aggregated.json

aggregateD requires a minimal config in order to specify the InfluxDB server and its credentials. Config can either be provided as a json file or as a yaml file. An example config is as follows:
  ```json
  {
    "influxHost":   "127.0.0.1",
    "influxPort":     "8086",
    "influxUsername": "root",
    "influxPassword": "root",
    "influxDatabase": "metrics"
  }
  ```

aggregateD exposes two web service endpoints: /events and /metrics on port 8083 by default. aggregateD accepts json encoded metrics which take the form of:

  ```json
  {
  	"name":      "requests",
  	"host":      "httpd.example.com",
  	"timestamp": "Wed, 13 May 2015 14:56:25 +0000",
  	"type":      "gauge",
  	"value":     67,
  	"sampling":  1,
  	"tags":      {"exampleTag1": 5, "exampleTag2": "value"}
  }
  ```
Similarly, events are represented in the following format:

  ```json
  {
    "name":           "timeout",
    "text":           "a worker thread timedout",
    "host":           "node4.example.com",
    "alerttype":      "warning",
    "priority":       "normal",
    "timestamp":      "Wed, 13 May 2015 14:56:25 +0000",
    "aggregationKey": "worker-timeout",
    "sourceType":     "default",
    "tags":           {"exampleTag1": 5, "exampleTag2": "value"},
  }
  ```
