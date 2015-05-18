package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func serveHTTP(port string) {
	http.HandleFunc("/metrics", receiveJSONMetric)
	http.HandleFunc("/events", receiveJSONEvent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

//http handler function, unmarshalls json encoded metric into metric struct
func receiveJSONMetric(response http.ResponseWriter, request *http.Request) {
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

func receiveJSONEvent(response http.ResponseWriter, request *http.Request) {
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
