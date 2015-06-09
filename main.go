package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ccpgames/aggregateD/input"
	"github.com/ccpgames/aggregateD/output"
	"github.com/spf13/viper"
)

// type metric struct {
// 	Name      string
// 	Host      string
// 	Timestamp string
// 	Type      string
// 	Value     float64
// 	Sampling  float64
// 	Tags      map[string]string
// }

// type event struct {
// 	Name           string
// 	Text           string
// 	Host           string
// 	AggregationKey string
// 	Priority       string
// 	Timestamp      string
// 	AlertType      string
// 	Tags           map[string]string
// 	SourceType     string
// }

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
	metricsIn       = make(chan input.Metric, 10000)
	eventsIn        = make(chan input.Event, 10000)
	flushInterval   = 10 //flag.Int64("flush-interval", 10, "Flush interval")
	buckets         = make(map[string]*output.Bucket)
	events          = make(map[eventKey]*output.Bucket)
	outputURL       string
	reportMetaStats bool
	influxConfig    output.InfluxDBConfig

	aggregators = map[string]func(input.Metric){
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

func processMetric(receivedMetric input.Metric) {
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
			buckets[receivedMetric.Name] = new(output.Bucket)
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

		buckets[receivedMetric.Name].Tags["Source"] = receivedMetric.Host

		handler(receivedMetric)

		//create a meta-metric couting the number of metrics that are processed
		if reportMetaStats {
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

func processEvent(receivedEvent input.Event) {
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

	if len(influxConfig.InfluxHost) > 0 {
		client := output.ConfigureInfluxDB(influxConfig)
		output.WriteInfluxDB(bucketArray, &client, influxConfig)
	}

	if len(outputURL) > 0 {
		output.WriteJSON(bucketArray, outputURL)
	}

	buckets = make(map[string]*output.Bucket)
	events = make(map[eventKey]*output.Bucket)

}

func parseConfig(config string) {
	//viper accepts config file without extension, so remove extension
	config = config[0:strings.Index(config, ".")]
	viper.SetConfigName(config)
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatal(err)
	}

	outputUndefined := true

	if viper.GetBool("outputInfluxDB") {
		influxConfig = output.InfluxDBConfig{
			InfluxHost:     viper.GetString("influxHost"),
			InfluxPort:     viper.GetString("influxPort"),
			InfluxUsername: viper.GetString("influxUsername"),
			InfluxPassword: viper.GetString("influxPassword"),
			InfluxDatabase: viper.GetString("influxDatabase"),
		}
		outputUndefined = false
	}

	if viper.GetBool("outputJSON") {
		outputURL = viper.GetString("outputURL")
		outputUndefined = false
	}

	if outputUndefined {
		panic("No outputs defined")
	}

	if viper.GetBool("reportMetaStats") {
		reportMetaStats = true
	}

	if viper.GetBool("inputJSON") {
		viper.SetDefault("HTTPPort", "8003")
		go input.ServeHTTP(viper.GetString("HTTPPort"), metricsIn, eventsIn)
	}

	if viper.GetBool("inputDogStatsD") {
		viper.SetDefault("UDPPort", "8125")
		go input.ServeUDP(viper.GetString("UDPPort"), metricsIn, eventsIn)
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
