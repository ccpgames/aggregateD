package input

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
)

type (
	/*Metric represents a single time series point

	Type is one of: histogram, counter, gauge or set

	SecondaryData represents values other than the primary value which should
	be treated as data and not metadata by the backend storage

	Tags are KV metadata
	*/
	Metric struct {
		Name          string
		Host          string
		Timestamp     float64
		Type          string
		Sampling      float64
		Value         float64
		SecondaryData map[string]interface{}
		Tags          map[string]string
	}

	//MetricBatch represent a batch of individual metrics that have been sent together
	MetricBatch struct {
		Batch []Metric
		Size  int32
	}

	//Event represents a single event instance
	Event struct {
		Name           string
		Text           string
		Host           string
		AggregationKey string
		Priority       string
		Timestamp      float64
		AlertType      string
		Tags           map[string]string
		SourceType     string
	}

	metricsHTTPHandler struct {
		metricsIn chan Metric
	}

	eventsHTTPHandler struct {
		eventsIn chan Event
	}

	metricsBatchHTTPHandler struct {
		metricsIn chan Metric
	}
)

//http handler function, unmarshalls json encoded metric into metric struct
func (handler *metricsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var receivedMetric Metric
	err := decoder.Decode(&receivedMetric)

	sourceAddress := r.RemoteAddr
	sourceIP, _, _ := net.SplitHostPort(r.RemoteAddr)
	log.Printf("Received metric from %s\n", sourceIP)

	if err == nil {
		parseMetric(receivedMetric, sourceIP, handler.metricsIn)
	} else {
		log.Println(err)
		log.Printf("Unable to decode metric from, %s", sourceAddress)
	}

	r.Body.Close()
}

func (handler *eventsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//unmarshall json encoded events into event struct
	decoder := json.NewDecoder(r.Body)
	var receivedEvent Event
	err := decoder.Decode(&receivedEvent)
	sourceAddress := r.RemoteAddr
	sourceIP := sourceAddress[0:strings.Index(r.RemoteAddr, ":")]

	if err == nil {
		if receivedEvent.Tags == nil {
			receivedEvent.Tags = make(map[string]string)
		}

		//append source address to metric
		receivedEvent.Tags["source"] = sourceIP
		handler.eventsIn <- receivedEvent
	} else {
		//if unable to parse the metric, drop it. This could be a problem for out of date clients.
		log.Println(err)
		log.Printf("Unable to decode event from, %s", sourceAddress)
	}

	r.Body.Close()
}

func (handler *metricsBatchHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var receivedMetricBatch MetricBatch

	if r.Body == nil {
		http.Error(w, "Empty request body", 400)
		return
	}

	err := json.NewDecoder(r.Body).Decode(&receivedMetricBatch)
	if err != nil {
		http.Error(w, "Malformed batch", 400)
		log.Println("Malformed batch")
		return
	}

	sourceAddress := r.RemoteAddr
	sourceIP, _, _ := net.SplitHostPort(r.RemoteAddr)

	log.Printf("Received metric batch from %s\n", sourceIP)
	if err == nil {
		if len(receivedMetricBatch.Batch) == 0 {
			log.Printf("metric batch from %s is empty\n", sourceIP)
		} else {
			for i := range receivedMetricBatch.Batch {
				parseMetric(receivedMetricBatch.Batch[i], sourceIP, handler.metricsIn)
			}
		}
	} else {
		log.Println(err)
		log.Printf("Unable to decode metric batch from, %s", sourceAddress)
	}

}

func parseMetric(receivedMetric Metric, sourceIP string, metricsIn chan Metric) {
	//add an aditional field specifing the host which forwarded aggregateD the metric
	//this might often be the same as the client specified host field but in situations
	//where the client is behind NAT, i.e many EVE clients this information is useful.

	if receivedMetric.SecondaryData == nil {
		receivedMetric.SecondaryData = make(map[string]interface{})
	}
	receivedMetric.SecondaryData["source"] = sourceIP

	//ensure that no secondary values are nil, clients should not
	//submit nil values but if they do they should not be sent to Influx
	for k := range receivedMetric.SecondaryData {
		if receivedMetric.SecondaryData[k] == nil {
			receivedMetric.SecondaryData[k] = 0.0
		}
	}
	metricsIn <- receivedMetric
}

//ServeHTTP exposes /events and /metrics and proceses JSON encoded events
func ServeHTTP(port string, metricsIn chan Metric, eventsIn chan Event) {
	server := http.NewServeMux()

	metricsHandler := new(metricsHTTPHandler)
	metricsHandler.metricsIn = metricsIn

	eventsHandler := new(eventsHTTPHandler)
	eventsHandler.eventsIn = eventsIn

	metricsBatchHandler := new(metricsBatchHTTPHandler)
	metricsBatchHandler.metricsIn = metricsIn

	server.Handle("/metrics", metricsHandler)
	server.Handle("/events", eventsHandler)
	server.Handle("/metrics_batch", metricsBatchHandler)

	log.Printf("Accepting json metrics on port %s", port)

	log.Fatal(http.ListenAndServe(":"+port, server))
}
