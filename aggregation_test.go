package main

import "testing"

func TestGaugeAggregation(t *testing.T) {
	registerAggregators()

	gauge1 := metric{
		Name:      "load",
		Timestamp: "2015-05-12T14:49:32",
		Type:      "gauge",
		Value:     1000,
	}

	gauge2 := metric{
		Name:      "load",
		Timestamp: "2015-05-12T14:49:31",
		Type:      "gauge",
		Value:     1,
	}

	processMetric(gauge1)
	processMetric(gauge2)

	aggregatedValue := buckets["load"].Fields["gauge"]

	if aggregatedValue != 1.0 {
		t.Error("Expected 1, got ", aggregatedValue)
	}
}

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
