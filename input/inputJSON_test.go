package input

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	metricsIn = make(chan Metric, 10000)
	eventsIn  = make(chan Event, 10000)
)

func JSONInputTestMain(m *testing.M) {
	os.Exit(m.Run())
}

//TestValidMessage tests the ability of aggregateD recieve properly
//encoded JSON messages over HTTP
func TestValidMetric(t *testing.T) {
	go ServeHTTP("8080", metricsIn, eventsIn)

	testMetric := new(Metric)
	testMetric.Host = "fakehost.example.org"
	testMetric.Name = "fakemetric"
	testMetric.Sampling = 1
	testMetric.Tags = make(map[string]string)
	testMetric.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
	testMetric.Type = "counter"
	testMetric.Value = rand.Float64()

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

//TestMultipleMessages ensures that aggregateD can recieve multiple
//metrics and events correctly and without loss or alternation
func TestMultipleMetrics(t *testing.T) {
	sentValuesSum := 0.0
	for i := 0; i < 1; i++ {
		testMetric := new(Metric)
		testMetric.Host = "fakehost.example.org"
		testMetric.Name = "fakemetric"
		testMetric.Sampling = 1
		testMetric.Tags = make(map[string]string)
		testMetric.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
		testMetric.Type = "counter"

		value := rand.Float64()
		sentValuesSum += value
		testMetric.Value = value

		jsonStr, _ := json.Marshal(testMetric)
		client := &http.Client{}
		request, _ := http.NewRequest("POST", "http://127.0.0.1:8080/metrics", strings.NewReader(string(jsonStr)))
		request.Header.Set("Content-Type", "application/json")
		client.Do(request)
	}
	time.Sleep(300 * time.Millisecond)

	recievedValuesSum := 0.0
	for i := 0; i < 1; i++ {
		receivedMetric := <-metricsIn
		recievedValuesSum += receivedMetric.Value
	}

	if recievedValuesSum != sentValuesSum {
		t.Error("Total value of sent metrics does not equal recieved metrics")
	}

}

func TestMultipleEvents(t *testing.T) {
	sentTextLengthSum := 0
	for i := 0; i < 1; i++ {
		testEvent := new(Event)
		testEvent.Host = "fakehost.example.org"
		testEvent.Name = "fakemetric"
		testEvent.Priority = "high"
		testEvent.AlertType = "warning"
		testEvent.SourceType = "test"
		testEvent.Text = "something has failed"
		testEvent.Tags = make(map[string]string)
		testEvent.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
		testEvent.AggregationKey = "tests"

		hasher := md5.New()
		randValue := string(rand.Int())

		hasher.Write([]byte(randValue))

		text := hex.EncodeToString(hasher.Sum(nil))
		testEvent.Text = text
		sentTextLengthSum += len(testEvent.Text)

		jsonStr, _ := json.Marshal(testEvent)
		client := &http.Client{}
		request, _ := http.NewRequest("POST", "http://127.0.0.1:8080/events", strings.NewReader(string(jsonStr)))
		request.Header.Set("Content-Type", "application/json")
		client.Do(request)
	}
	time.Sleep(300 * time.Millisecond)

	recievedTextLengthSum := 0
	for i := 0; i < 1; i++ {
		receivedEvent := <-eventsIn
		recievedTextLengthSum += len(receivedEvent.Text)
	}

	if sentTextLengthSum != recievedTextLengthSum {
		t.Error("Total length of recieved event text does not equal recieved event text")
	}

}
