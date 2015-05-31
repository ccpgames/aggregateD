package main

import "testing"

func TestHistogramAggregation(t *testing.T) {
	for i := 0.0; i < 101; i++ {
		histogram1 := metric{
			Name:      "latency",
			Timestamp: "2015-05-12T14:49:32",
			Type:      "histogram",
			Value:     i,
		}
		processMetric(histogram1)
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
func TestGaugeAggregation(t *testing.T) {
	for i := 0.0; i < 100; i++ {
		gauge1 := metric{
			Name:      "load",
			Timestamp: "2015-05-12T14:49:32",
			Type:      "gauge",
			Value:     i,
		}
		processMetric(gauge1)
	}

	gauge2 := metric{
		Name:      "load",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "gauge",
		Value:     1,
	}

	processMetric(gauge2)

	aggregatedValue := buckets["load"].Fields["value"]

	if aggregatedValue != 1.0 {
		t.Error("Expected 1, got ", aggregatedValue)
	}
}

func TestCounterAggregation(t *testing.T) {
	counter1 := metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:32",
		Type:      "counter",
		Value:     10,
	}

	counter2 := metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "counter",
		Value:     50,
	}

	counter3 := metric{
		Name:      "requests",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "counter",
		Value:     -9,
	}
	processMetric(counter1)
	processMetric(counter2)
	processMetric(counter3)

	aggregatedValue := buckets["requests"].Fields["value"]

	if aggregatedValue != 51.0 {
		t.Error("Expected 51, got ", aggregatedValue)
	}
}
