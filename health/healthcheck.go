package health

import (
	"log"
	"net/http"

	"github.com/ccpgames/aggregateD/output"
)

type healthHTTPHandler struct {
	influxdbConfig output.InfluxDBConfig
}

func (handler *healthHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, err := http.Get(handler.influxdbConfig.InfluxURL + "/ping")
	log.Println(handler.influxdbConfig.InfluxURL)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Unable to write to InfluxDB"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("aggregateD is healthy"))
}

//Serve exposes /health
func Serve(influxdbConfig output.InfluxDBConfig) {
	server := http.NewServeMux()
	handler := new(healthHTTPHandler)
	handler.influxdbConfig = influxdbConfig
	server.Handle("/health", handler)
	log.Printf("Serving Healthcheck on port 8000")
	log.Fatal(http.ListenAndServe(":8000", server))
}
