package input

import (
	"encoding/json"
	"log"
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
)

//http handler function, unmarshalls json encoded metric into metric struct
func (handler *metricsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var receivedMetric Metric
	err := decoder.Decode(&receivedMetric)
	sourceAddress := r.RemoteAddr
	sourceIP := sourceAddress[0:strings.Index(r.RemoteAddr, ":")]

	if err == nil {
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

		handler.metricsIn <- receivedMetric
	} else {
		log.Println(err)
		log.Printf("Unable to decode metric from, %s", sourceAddress)
	}

	r.Body.Close()
}

//unmarshall json encoded events into event struct
func (handler *eventsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var receivedEvent Event
	err := decoder.Decode(&receivedEvent)
	sourceAddress := r.RemoteAddr
	sourceIP := sourceAddress[0:strings.Index(r.RemoteAddr, ":")]

	if err == nil {
		if receivedEvent.Tags == nil {
			receivedEvent.Tags = make(map[string]string)
		}

		receivedEvent.Tags["source"] = sourceIP
		handler.eventsIn <- receivedEvent
	} else {
		log.Println(err)
		log.Printf("Unable to decode event from, %s", sourceAddress)
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
