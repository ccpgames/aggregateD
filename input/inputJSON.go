package input

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

//Metric represnts a single time series point
type Metric struct {
	Name      string
	Host      string
	Timestamp string
	Type      string
	Value     float64
	Sampling  float64
	Tags      map[string]string
}

//Event represents a single event instance
type Event struct {
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

type metricsHTTPHandler struct {
	metricsIn chan Metric
}

type eventsHTTPHandler struct {
	eventsIn chan Event
}

//http handler function, unmarshalls json encoded metric into metric struct
func (handler *metricsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var receivedMetric Metric
	err := decoder.Decode(&receivedMetric)

	if err == nil {
		receivedMetric.Host = r.Host
		handler.metricsIn <- receivedMetric
	} else {
		fmt.Println("error parsing metric")
		fmt.Println(err)
	}

	r.Body.Close()
}

//unmarshall json encoded events into event struct
func (handler *eventsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var receivedEvent Event
	err := decoder.Decode(&receivedEvent)

	if err == nil {
		receivedEvent.Host = r.Host
		handler.eventsIn <- receivedEvent

	} else {
		fmt.Println("error parsing event")
		fmt.Println(err)
	}

	r.Body.Close()
}

//ServeHTTP exposes /events and /metrics and proceses JSON encoded events
func ServeHTTP(port string, metricsIn chan Metric, eventsIn chan Event) {
	server := http.NewServeMux()

	metricsHandler := new(metricsHTTPHandler)
	metricsHandler.metricsIn = metricsIn

	eventsHandler := new(eventsHTTPHandler)
	eventsHandler.eventsIn = eventsIn

	server.Handle("/metrics", metricsHandler)
	server.Handle("/events", eventsHandler)

	log.Fatal(http.ListenAndServe(":"+port, server))
}
