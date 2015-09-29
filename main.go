package main

import (
	"encoding/json"
	"flag"
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
)

var (
	metricsIn     = make(chan input.Metric, 10000)
	eventsIn      = make(chan input.Event, 10000)
	metricBuckets = make(map[metricKey]*output.Bucket)
	eventBuckets  = make(map[eventKey]*output.Bucket)

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
		//fmt.Println("Invalid metric name")
		return
	} else if receivedMetric.Timestamp == "" {
		//fmt.Println("Invalid timestamp")
		return
	} else if receivedMetric.Type == "" {
		//fmt.Println("Invalid Type")
		return
	}

	/*this is a bit of a hack, in order to compare tags and ensure that metrics with
	distinct tags and secondary data are not aggregated they are used as part of the key. Unfortunately
	go doesn't allow for maps to be used in a key, therefore we serialise the map
	to a json string and use that instead of the map. Sorry. */
	if handler, handlerOK := aggregators[receivedMetric.Type]; handlerOK {
		key := *(new(metricKey))
		key.Name = receivedMetric.Name

		jsonTagMap, _ := json.Marshal(receivedMetric.Tags)
		jsonSecondaryDataMap, _ := json.Marshal(receivedMetric.SecondaryData)

		key.Tags = string(jsonTagMap)
		key.SecondaryData = string(jsonSecondaryDataMap)

		_, bucketOK := metricBuckets[key]

		//if bucket doesn't exist, create one
		if !bucketOK {
			metricBuckets[key] = new(output.Bucket)
			metricBuckets[key].Name = receivedMetric.Name
			metricBuckets[key].Fields = receivedMetric.SecondaryData
			metricBuckets[key].Tags = receivedMetric.Tags
		}

		handler(receivedMetric, key)

	}
}

//aggregate multiple events into one bucket
func aggregateEvent(receivedEvent input.Event) {
	if receivedEvent.Name == "" {
		//fmt.Println("Invalid event title")
		return
	} else if receivedEvent.Timestamp == "" {
		//fmt.Println("Invalid timestamp")
		return
	} else if receivedEvent.Text == "" {
		//fmt.Println("Invalid Type")
		return
	}

	//an eventKey is used to index the map of events
	//this allows the event name and the aggregation key to index events
	//such that events with different aggregation keys are not aggregated
	key := *(new(eventKey))
	key.Name = receivedEvent.Name
	key.AggregationKey = receivedEvent.AggregationKey

	_, ok := eventBuckets[key]

	if !ok {
		eventBuckets[key] = new(output.Bucket)
		eventBuckets[key].Name = receivedEvent.Name
		eventBuckets[key].Fields = make(map[string]interface{})
		eventBuckets[key].Tags = receivedEvent.Tags

	}

	eventBuckets[key].Fields["name"] = receivedEvent.Name
	eventBuckets[key].Fields["text"] = receivedEvent.Text
	eventBuckets[key].Fields["host"] = receivedEvent.Host
	eventBuckets[key].Fields["aggregation_key"] = receivedEvent.AggregationKey
	eventBuckets[key].Fields["priority"] = receivedEvent.Priority
	eventBuckets[key].Fields["alert_type"] = receivedEvent.AlertType

	eventBuckets[key].Timestamp = receivedEvent.Timestamp

	for k, v := range receivedEvent.Tags {
		eventBuckets[key].Tags[k] = v
	}
}

//write out aggregated buckets to one or more outputs and clear the metric and event
//dictionaries
func flush() {

	if len(configuration.InfluxConfig.InfluxHost) > 0 {
		outputBuckets := make([]output.Bucket, 0, len(metricBuckets)+len(eventBuckets))

		for _, metric := range metricBuckets {
			outputBuckets = append(outputBuckets, *metric)
		}

		for _, event := range eventBuckets {
			outputBuckets = append(outputBuckets, *event)
		}

		output.WriteToInfluxDB(outputBuckets, configuration.InfluxConfig, configuration.InfluxConfig.InfluxDefaultDB)
	}

	if len(configuration.JSONOutputURL.String()) > 0 {
		var bucketArray []output.Bucket
		for _, v := range metricBuckets {
			bucketArray = append(bucketArray, *v)
		}

		for _, v := range eventBuckets {
			bucketArray = append(bucketArray, *v)
		}

		output.WriteJSON(bucketArray, configuration.JSONOutputURL)
	}

	metricBuckets = make(map[metricKey]*output.Bucket)
	eventBuckets = make(map[eventKey]*output.Bucket)

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
