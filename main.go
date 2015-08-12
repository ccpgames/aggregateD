package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/ccpgames/aggregateD/config"
	"github.com/ccpgames/aggregateD/input"
	"github.com/ccpgames/aggregateD/output"
)

type (
	//eventKey is used as the key in the map of events, this is needed as
	//the datadog docs specify that events are aggregated based on
	//‘hostname/event_type/source_type/aggregation_key’ and therefore
	//a single string key is insuffient to refer to events
	eventKey struct {
		Name           string
		AggregationKey string
	}

	metricKey struct {
		Name string
		Tags string
	}
)

var (
	metricsIn = make(chan input.Metric, 10000)
	eventsIn  = make(chan input.Event, 10000)
	buckets   = make(map[metricKey]*output.Bucket)
	events    = make(map[eventKey]*output.Bucket)

	configuration config.Configuration

	aggregators = map[string]func(input.Metric, metricKey){
		"gauge":     gaugeAggregator,
		"set":       setAggregator,
		"counter":   counterAggregator,
		"histogram": histogramAggregator,
	}
)

func aggregate() {
	t := time.NewTicker(time.Duration(configuration.FlushInterval) * time.Second)
	for {
		select {
		case <-t.C:
			flush()
		case receivedMetric := <-metricsIn:
			aggregateMetric(receivedMetric)
		case receivedEvent := <-eventsIn:
			aggregateEvent(receivedEvent)
		}
	}
}

//aggregate metrics into a single bucket, makes use of aggregators
//to aggregate different metric types
func aggregateMetric(receivedMetric input.Metric) {
	//if a handler exists to aggregate the metric, do so
	//otherwise ignore the metric
	if receivedMetric.Name == "" {
		fmt.Println("Invalid metric name")
		return
	} else if receivedMetric.Timestamp == "" {
		fmt.Println("Invalid timestamp")
		return
	} else if receivedMetric.Type == "" {
		fmt.Println("Invalid Type")
		return
	}

	if handler, ok := aggregators[receivedMetric.Type]; ok {
		key := *(new(metricKey))
		key.Name = receivedMetric.Name

		//this is a bit of a hack, in order to compare tags and ensure that metrics with
		//distinct tags are not aggregated, tags are used as part of the key. Unfortunately
		//go doesn't allow for maps to be used in a key, therefore we serialise the map
		//to a json string and use that instead of the map. Sorry.
		jsonMap, _ := json.Marshal(receivedMetric.Tags)
		key.Tags = string(jsonMap)

		_, ok := buckets[key]

		//if bucket doesn't exist, create one
		if !ok {
			buckets[key] = new(output.Bucket)
			buckets[key].Name = receivedMetric.Name
			buckets[key].Fields = make(map[string]interface{})
			buckets[key].Tags = make(map[string]string)
		}

		//aggregate tags
		//this results in the aggregated metric having the tags from the last metric
		//maybe not best, think about alternative approaches
		for k, v := range receivedMetric.Tags {
			buckets[key].Tags[k] = v
		}

		handler(receivedMetric, key)

		//create a meta-metric couting the number of metrics that are processed
		//it's useful for debug purposes and tracking the performance of aggregateD
		if configuration.ReportMetaStats {
			//ensure that metametrics aren't reported as regular metrics
			if receivedMetric.Name != "aggregated_metric_count" {
				metastats := new(input.Metric)
				metastats.Name = "aggregated_metric_count"
				metastats.Sampling = 1
				metastats.Type = "counter"
				metastats.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
				metastats.Value = 1

				metricsIn <- *metastats
			}
		}
	}
}

//aggregate multiple events into one bucket
func aggregateEvent(receivedEvent input.Event) {
	if receivedEvent.Name == "" {
		fmt.Println("Invalid event title")
		return
	} else if receivedEvent.Timestamp == "" {
		fmt.Println("Invalid timestamp")
		return
	} else if receivedEvent.Text == "" {
		fmt.Println("Invalid Type")
		return
	}

	//an eventKey is used to index the map of events
	//this allows the event name and the aggregation key to index events
	//such that events with different aggregation keys are not aggregated
	key := *(new(eventKey))
	key.Name = receivedEvent.Name
	key.AggregationKey = receivedEvent.AggregationKey

	_, ok := events[key]

	if !ok {
		events[key] = new(output.Bucket)
		events[key].Name = receivedEvent.Name
		events[key].Fields = make(map[string]interface{})
		events[key].Tags = make(map[string]string)
	}

	events[key].Fields["name"] = receivedEvent.Name
	events[key].Fields["text"] = receivedEvent.Text
	events[key].Fields["host"] = receivedEvent.Host
	events[key].Fields["aggregation_key"] = receivedEvent.AggregationKey
	events[key].Fields["priority"] = receivedEvent.Priority
	events[key].Fields["alert_type"] = receivedEvent.AlertType

	events[key].Timestamp = receivedEvent.Timestamp

	for k, v := range receivedEvent.Tags {
		events[key].Tags[k] = v
	}
}

//write out aggregated buckets to one or more outputs and
func flush() {
	var bucketArray []output.Bucket

	if len(buckets) > 0 {
		for _, v := range buckets {
			bucketArray = append(bucketArray, *v)
		}
	}

	if len(events) > 0 {
		for _, v := range events {
			bucketArray = append(bucketArray, *v)
		}
	}

	if len(configuration.InfluxConfig.InfluxHost) > 0 {
		client := output.ConfigureInfluxDB(configuration.InfluxConfig)
		output.WriteInfluxDB(bucketArray, &client, configuration.InfluxConfig)
	}

	if len(configuration.JSONOutputURL.String()) > 0 {
		output.WriteJSON(bucketArray, configuration.JSONOutputURL)
	}

	buckets = make(map[metricKey]*output.Bucket)
	events = make(map[eventKey]*output.Bucket)

}

func main() {
	configFilePath := flag.String("config", "", "configuration file")
	flag.Parse()

	configFile, err := config.ReadConfig(*configFilePath)

	if err != nil {
		panic("Unable to read config")
	}

	configuration = config.ParseConfig(configFile, metricsIn, eventsIn)
	aggregate()
}
