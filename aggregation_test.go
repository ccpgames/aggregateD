package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ccpgames/aggregateD/input"
)

func TestHistogramAggregator(t *testing.T) {
	for i := 0.0; i < 101; i++ {
		histogram1 := input.Metric{
			Name:      "latency",
			Timestamp: "2015-05-12T14:49:32",
			Type:      "histogram",
			Value:     i,
		}
		aggregateMetric(histogram1)
	}

	aggregatedCount := buckets["latency"].Fields["count"]
	aggregatedAvg := buckets["latency"].Fields["avg"]
	aggregatedMax := buckets["latency"].Fields["max"]
	aggregatedMedian := buckets["latency"].Fields["median"]
	aggregated95Percentile := buckets["latency"].Fields["95percentile"]

	if aggregatedCount != 101.0 {
		t.Error("Expected count of 101, got ", aggregatedCount)
	}

	if aggregatedAvg != 50.0 {
		t.Error("Expected average of 50, got ", aggregatedAvg)
	}

	if aggregatedMax != 100.0 {
		t.Error("Expected max of 100, got ", aggregatedMax)
	}

	if aggregatedMedian != 50.0 {
		t.Error("Expected median of 50, got ", aggregatedMax)
	}

	if aggregated95Percentile != 95.0 {
		t.Error("Expected the 95th percentile to be 95, got ", aggregated95Percentile)
	}

}
func TestGaugeAggregator(t *testing.T) {
	for i := 0.0; i < 100; i++ {
		gauge1 := input.Metric{
			Name:      "load",
			Timestamp: "2015-05-12T14:49:32",
			Type:      "gauge",
			Value:     i,
		}
		aggregateMetric(gauge1)
	}

	gauge2 := input.Metric{
		Name:      "load",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "gauge",
		Value:     1,
	}

	aggregateMetric(gauge2)

	aggregatedValue := buckets["load"].Fields["value"]

	if aggregatedValue != 1.0 {
		t.Error("Expected 1, got ", aggregatedValue)
	}
}

func TestCounterAggregator(t *testing.T) {
	counter1 := input.Metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:32",
		Type:      "counter",
		Value:     10,
	}

	counter2 := input.Metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "counter",
		Value:     50,
	}

	counter3 := input.Metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "counter",
		Value:     -9,
	}
	aggregateMetric(counter1)
	aggregateMetric(counter2)
	aggregateMetric(counter3)

	aggregatedValue := buckets["requests"].Fields["value"]

	if aggregatedValue != 51.0 {
		t.Error("Expected 51, got ", aggregatedValue)
	}
}

func TestAggregateMetrics(t *testing.T) {
	expectedCounterValue := 0.0
	expectedGaugeValue := 0.0

	counter := new(input.Metric)
	counter.Host = "fakehost.example.org"
	counter.Name = "fakecounter"
	counter.Sampling = 1
	counter.Tags = make(map[string]string)
	counter.Type = "counter"

	gauge := new(input.Metric)
	gauge.Host = "fakehost.example.org"
	gauge.Name = "fakegauge"
	gauge.Sampling = 1
	gauge.Tags = make(map[string]string)
	gauge.Type = "gauge"

	for i := 0; i < 100; i++ {
		counter.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
		gauge.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")

		counter.Value = float64(rand.Int())
		gauge.Value = rand.Float64()
		expectedCounterValue += counter.Value
		expectedGaugeValue = gauge.Value

		aggregateMetric(*counter)
		aggregateMetric(*gauge)

	}

	counterValue := buckets["fakecounter"].Fields["value"]
	if counterValue != expectedCounterValue {
		t.Error("Actual value does not match expected value of aggregated counter")
	}

	gaugeValue := buckets["fakegauge"].Fields["value"]
	if gaugeValue != expectedGaugeValue {
		fmt.Println(gaugeValue)
		fmt.Println(expectedGaugeValue)

		t.Error("Actual value does not match expected value of aggregated gauge")
	}
}
