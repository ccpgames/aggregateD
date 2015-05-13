package main

import "testing"

func TestCounterAggregation(t *testing.T) {
	registerAggregators()

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

	aggregatedValue := buckets["requests"].Fields["counter"]

	if aggregatedValue != 51.0 {
		t.Error("Expected 51, got ", aggregatedValue)
	}
}
