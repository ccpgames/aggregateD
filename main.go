package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/spf13/viper"
)

type metric struct {
	Name      string
	Host      string
	Timestamp string
	Type      string
	Value     float64
	Sampling  float64
	Tags      map[string]string
}

type bucket struct {
	Name      string
	Timestamp string
	Tags      map[string]string
	Values    []float64 //intermediate values for histograms, only fields are sent to influxdb
	Fields    map[string]interface{}
}

type event struct {
	Name           string
	Text           string
	Host           string
	AggregationKey string
	Priority       string
	Timestamp      string
	AlertType      string
	Tags           map[string]string
	SourceType     string
}

//eventKey is used as the key in the map of events, this is needed as
//the datadog docs specify that events are aggregated based on
//‘hostname/event_type/source_type/aggregation_key’ and therefore
//a single string key is insuffient to refer to events
type eventKey struct {
	Name           string
	Host           string
	SourceType     string
	AggregationKey string
}

type influxDBConfig struct {
	influxHost     string
	influxPort     string
	influxUsername string
	influxPassword string
	influxDatabase string
}

var (
	metricsIn     = make(chan metric, 10000)
	eventsIn      = make(chan event, 10000)
	flushInterval = 10 //flag.Int64("flush-interval", 10, "Flush interval")
	aggregators   = make(map[string]func(metric))
	influxConfig  influxDBConfig
	buckets       = make(map[string]*bucket)
	events        = make(map[eventKey]*bucket)
)

func aggregate() {
	t := time.NewTicker(time.Duration(flushInterval) * time.Second)
	for {
		select {
		case <-t.C:
			flush()
		case receivedMetric := <-metricsIn:
			processMetric(receivedMetric)
		case receivedEvent := <-eventsIn:
			processEvent(receivedEvent)
		}
	}
}

func processMetric(receivedMetric metric) {
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
		_, ok := buckets[receivedMetric.Name]

		//if bucket doesn't exist, create one
		if !ok {
			buckets[receivedMetric.Name] = new(bucket)
			buckets[receivedMetric.Name].Name = receivedMetric.Name
			buckets[receivedMetric.Name].Fields = make(map[string]interface{})
			buckets[receivedMetric.Name].Tags = make(map[string]string)
		}

		//aggregate tags
		//this results in the aggregated metric having the tags from the last metric
		//maybe not best, think about alternative approaches
		for k, v := range receivedMetric.Tags {
			buckets[receivedMetric.Name].Tags[k] = v
		}

		handler(receivedMetric)
	}
}

func processEvent(receivedEvent event) {
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

	key := *(new(eventKey))
	key.SourceType = receivedEvent.SourceType
	key.Host = receivedEvent.Host
	key.Name = receivedEvent.Name
	key.AggregationKey = receivedEvent.AggregationKey

	_, ok := events[key]

	if !ok {
		events[key] = new(bucket)
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

func flush() {
	client := configureInfluxDB(influxConfig)
	var bucketArray []bucket

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

	writeInfluxDB(bucketArray, &client, influxConfig)
	buckets = make(map[string]*bucket)
	events = make(map[eventKey]*bucket)

}

//http handler function, unmarshalls json encoded metric into metric struct
func receiveMetric(response http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var receivedMetric metric
	err := decoder.Decode(&receivedMetric)

	if err == nil {
		metricsIn <- receivedMetric
	} else {
		fmt.Println("error parsing metric")
		fmt.Println(err)
	}
}

func receiveEvent(response http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var receivedEvent event
	err := decoder.Decode(&receivedEvent)

	if err == nil {
		eventsIn <- receivedEvent

	} else {
		fmt.Println("error parsing event")
		fmt.Println(err)
	}
}

func main() {
	var (
		config = flag.String("config", "./aggregated", "configuration file")
		port   = flag.String("port", "8082", "Port to listen on for metrics and events, default 8082")
	)

	viper.SetConfigName(*config)
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatal("No configuration file found, exiting")
	}

	influxConfig = influxDBConfig{
		influxHost:     viper.GetString("influxHost"),
		influxPort:     viper.GetString("influxPort"),
		influxUsername: viper.GetString("influxUsername"),
		influxPassword: viper.GetString("influxPassword"),
		influxDatabase: viper.GetString("influxDatabase"),
	}

	viper.SetDefault("flushInterval", 10)
	flushInterval = viper.GetInt("flushInterval")

	registerAggregators()
	go aggregate()
	http.HandleFunc("/metrics", receiveMetric)
	http.HandleFunc("/events", receiveEvent)

	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
