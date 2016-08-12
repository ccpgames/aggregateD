package main

import (
	"encoding/json"
	"flag"
	"log"
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

	//metricKey is used to index the map of metrics, this is used in liue of metric
	//names as doing so would risk unrelated metrics which share the same name being aggregated
	//the metric key ensures that metrics with the same name, tags and secondary data are aggregated.
	metricKey struct {
		Name          string
		Tags          string
		SecondaryData string
	}

	//Main represents the top level program execution which predominantly
	//includes the aggregation process. Inputs and outputs are done by other modules
	Main struct {
		metricsIn     chan input.Metric
		eventsIn      chan input.Event
		metricBuckets map[metricKey][]timestampedBucket
		eventBuckets  map[eventKey]*output.Bucket
		aggregators   map[string]func(input.Metric, *output.Bucket)
	}

	timestampedBucket struct {
		StartTimestamp int
		EndTimestamp   int
		MetricBucket   *output.Bucket
	}
)

var (
	configuration config.Configuration
)

func (m *Main) aggregate() {
	t := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case <-t.C:
			m.flush()
		case receivedMetric := <-m.metricsIn:
			m.aggregateMetric(receivedMetric)
		case receivedEvent := <-m.eventsIn:
			m.aggregateEvent(receivedEvent)
		}
	}
}

//aggregate metrics into a single bucket, makes use of aggregators
//to aggregate different metric types
func (m *Main) aggregateMetric(receivedMetric input.Metric) {
	//if a handler exists to aggregate the metric, do so
	//otherwise ignore the metric
	if receivedMetric.Name == "" {
		log.Printf("Invalid metric recieved from %s, missing name", receivedMetric.SecondaryData["source"])
		return
	} else if receivedMetric.Type == "" {
		log.Printf("Invalid metric recieved from %s, missing type", receivedMetric.SecondaryData["source"])
		return
	}

	/*this is a bit of a hack, in order to compare tags and ensure that metrics with
	distinct tags and secondary data are not aggregated they are used as part of the key. Unfortunately
	go doesn't allow for maps to be used in a key, therefore we serialise the map
	to a json string and use that instead of the map. Sorry. */
	if handler, handlerOK := m.aggregators[receivedMetric.Type]; handlerOK {
		key := *(new(metricKey))
		key.Name = receivedMetric.Name

		jsonTagMap, _ := json.Marshal(receivedMetric.Tags)
		jsonSecondaryDataMap, _ := json.Marshal(receivedMetric.SecondaryData)

		key.Tags = string(jsonTagMap)
		key.SecondaryData = string(jsonSecondaryDataMap)

		_, outerBucketSliceOK := m.metricBuckets[key]
		var outerBucket timestampedBucket

		//if this metric isn't know create a new bucket for it
		if !outerBucketSliceOK {
			outerBucket = *new(timestampedBucket)
			m.metricBuckets[key] = *new([]timestampedBucket)
		}

		innerBucket, innerBucketOK := getBucket(int(receivedMetric.Timestamp), m.metricBuckets[key])

		//if metric falls outside the time range we already have, make a new timestamped bucket
		//i.e. no inner bucket means no outer bucket
		if !innerBucketOK {
			innerBucket = new(output.Bucket)
			outerBucket.StartTimestamp = int(receivedMetric.Timestamp)
			//tempoary for testing, change 10 to config specified variable
			outerBucket.EndTimestamp = int(receivedMetric.Timestamp) + 10
			innerBucket.Name = receivedMetric.Name
			innerBucket.Fields = receivedMetric.SecondaryData
			innerBucket.Tags = receivedMetric.Tags
			outerBucket.MetricBucket = innerBucket
			m.metricBuckets[key] = append(m.metricBuckets[key], outerBucket)
		}

		handler(receivedMetric, innerBucket)

	}
}

//aggregate multiple events into one bucket
func (m *Main) aggregateEvent(receivedEvent input.Event) {
	if receivedEvent.Name == "" {
		log.Printf("Invalid event recieved from %s, missing name", receivedEvent.Tags["source"])
		return
	} else if receivedEvent.Text == "" {
		log.Printf("Invalid event recieved from %s, missing text", receivedEvent.Tags["source"])
		return
	}

	//an eventKey is used to index the map of events
	//this allows the event name and the aggregation key to index events
	//such that events with different aggregation keys are not aggregated
	key := *(new(eventKey))
	key.Name = receivedEvent.Name
	key.AggregationKey = receivedEvent.AggregationKey

	_, ok := m.eventBuckets[key]

	if !ok {
		m.eventBuckets[key].Timestamp = parseTimestamp(receivedEvent.Timestamp)
		m.eventBuckets[key] = new(output.Bucket)
		m.eventBuckets[key].Name = receivedEvent.Name
		m.eventBuckets[key].Fields = make(map[string]interface{})
		m.eventBuckets[key].Tags = receivedEvent.Tags
	}

	m.eventBuckets[key].Fields["name"] = receivedEvent.Name
	m.eventBuckets[key].Fields["text"] = receivedEvent.Text
	m.eventBuckets[key].Fields["host"] = receivedEvent.Host
	m.eventBuckets[key].Fields["aggregation_key"] = receivedEvent.AggregationKey
	m.eventBuckets[key].Fields["priority"] = receivedEvent.Priority
	m.eventBuckets[key].Fields["alert_type"] = receivedEvent.AlertType

	m.eventBuckets[key].Timestamp = parseTimestamp(receivedEvent.Timestamp)

	for k, v := range receivedEvent.Tags {
		m.eventBuckets[key].Tags[k] = v
	}
}

//write out aggregated buckets to one or more outputs and clear the metric and event
//dictionaries
func (m *Main) flush() {
	var outputBuckets []output.Bucket

	for _, v := range m.metricBuckets {
		for i := range v {
			outputBuckets = append(outputBuckets, *v[i].MetricBucket)
		}
	}

	for _, event := range m.eventBuckets {
		outputBuckets = append(outputBuckets, *event)
	}

	if len(configuration.InfluxConfig.InfluxURL) > 0 {

		if len(outputBuckets) > 0 {
			log.Printf("Writing %d points to InfluxDB", len(outputBuckets))
			influxdbErr := output.WriteToInfluxDB(outputBuckets, configuration.InfluxConfig)

			if influxdbErr != nil {
				if len(configuration.RedisOutputURL.String()) > 0 {
					log.Printf("InfluxDB write failed, attempting to write %d points to Redis", len(outputBuckets))
					redisErr := output.WriteRedis(outputBuckets, configuration.RedisOutputURL)
					if redisErr != nil {
						log.Println("WARNING: Redis write failed, metrics have been dropped")
					}
				}
			}
		}
	}

	if len(configuration.JSONOutputURL.String()) > 0 {
		output.WriteJSON(outputBuckets, configuration.JSONOutputURL)
	}

	m.metricBuckets = make(map[metricKey][]timestampedBucket)
	m.eventBuckets = make(map[eventKey]*output.Bucket)

}

/*parseTimestamp parses a UNIX timestamp from a float to
a Go time.Time type */
func parseTimestamp(timestamp float64) time.Time {
	if timestamp > 0 {
		return time.Unix(int64(timestamp), 0)
	}
	return time.Now()

}

func getBucket(timestamp int, buckets []timestampedBucket) (*output.Bucket, bool) {

	for i := range buckets {
		if timestamp >= buckets[i].StartTimestamp && timestamp <= buckets[i].EndTimestamp {
			return buckets[i].MetricBucket, true
		}
	}

	return nil, false
}

func main() {
	log.Print("Starting aggregateD")

	configFilePath := flag.String("config", "", "configuration file")
	flag.Parse()

	configFile, err := config.ReadConfig(*configFilePath)

	if err != nil {
		panic("Unable to read config")
	}

	m := new(Main)

	m.aggregators = map[string]func(input.Metric, *output.Bucket){
		"gauge":     m.gaugeAggregator,
		"set":       m.setAggregator,
		"counter":   m.counterAggregator,
		"histogram": m.histogramAggregator,
	}

	m.metricsIn = make(chan input.Metric, 10000)
	m.eventsIn = make(chan input.Event, 10000)
	m.metricBuckets = make(map[metricKey][]timestampedBucket)
	m.eventBuckets = make(map[eventKey]*output.Bucket)

	configuration = config.ParseConfig(configFile, m.metricsIn, m.eventsIn)
	log.Print("Begining aggregation")
	m.aggregate()
}
