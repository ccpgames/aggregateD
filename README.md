[![Build Status](https://travis-ci.org/ccpgames/ccp-aggregateD.svg?branch=master)](https://travis-ci.org/ccpgames/ccp-aggregateD)

aggregateD is a statistics aggregtion daemon inspired by dogstatsD and offers a similar features set. Rather than following the statsD protocol which isn't really human readable or particularly extensible and instead of using UDP which risk metric loss, aggregateD makes use of json over http. InfluxDB provides storage for the aggregated metrics. 

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
  	"host":     "httpd.example.com",
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
