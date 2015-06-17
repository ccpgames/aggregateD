package input

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"
)

//TestValidMessage tests the ability of aggregateD recieve properly
//encoded JSON messages over HTTP
func TestValidMessage(t *testing.T) {
	metricsIn := make(chan Metric, 10000)
	eventsIn := make(chan Event, 10000)

	testMetric := new(Metric)
	testMetric.Host = "fakehost.example.org"
	testMetric.Name = "fakemetric"
	testMetric.Sampling = 1
	testMetric.Tags = make(map[string]string)
	testMetric.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
	testMetric.Type = "counter"
	testMetric.Value = rand.Float64()
	go ServeHTTP("8080", metricsIn, eventsIn)

	jsonStr, _ := json.Marshal(testMetric)
	client := &http.Client{}
	request, _ := http.NewRequest("POST", "http://127.0.0.1:8080/metrics", strings.NewReader(string(jsonStr)))
	request.Header.Set("Content-Type", "application/json")
	client.Do(request)

	time.Sleep(300 * time.Millisecond)
	receivedMetric := <-metricsIn

	if receivedMetric.Value != testMetric.Value {
		t.Error("Values do not match")
	}

	if receivedMetric.Name != testMetric.Name {
		t.Error("Names do not match")
	}

}
