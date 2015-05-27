package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
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

var (
	metricsIn     = make(chan metric, 10000)
	eventsIn      = make(chan event, 10000)
	flushInterval = 10 //flag.Int64("flush-interval", 10, "Flush interval")
	influxConfig  influxDBConfig
	buckets       = make(map[string]*bucket)
	events        = make(map[eventKey]*bucket)

	aggregators = map[string]func(metric){
		"gauge":     gaugeAggregator,
		"set":       setAggregator,
		"counter":   counterAggregator,
		"histogram": histogramAggregator,
	}
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

func parseConfig(config string) {
	//viper accepts config file without extension, so remove extension
	config = config[0:strings.Index(config, ".")]
	viper.SetConfigName(config)
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatal(err)
	}

	if viper.GetBool("outputInfluxDB") {
		influxConfig = influxDBConfig{
			influxHost:     viper.GetString("influxHost"),
			influxPort:     viper.GetString("influxPort"),
			influxUsername: viper.GetString("influxUsername"),
			influxPassword: viper.GetString("influxPassword"),
			influxDatabase: viper.GetString("influxDatabase"),
		}
	} else {
		panic("No outputs defined")
	}

	if viper.GetBool("inputJSON") {
		viper.SetDefault("HTTPPort", "8003")
		go serveHTTP(viper.GetString("HTTPPort"))
	}

	if viper.GetBool("inputDogStatsD") {
		viper.SetDefault("UDPPort", "8125")
		go serveUDP(viper.GetString("UDPPort"))
	}

	viper.SetDefault("flushInterval", 10)

	flushInterval = viper.GetInt("flushInterval")
}

func main() {
	config := flag.String("config", " ", "configuration file")
	flag.Parse()

	parseConfig(*config)
	aggregate()
}
