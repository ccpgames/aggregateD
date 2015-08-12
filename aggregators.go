package main

import (
	"sort"
	"strconv"

	"github.com/ccpgames/aggregateD/input"
)

func gaugeAggregator(receivedMetric input.Metric, key metricKey) {
	_, ok := buckets[key].Fields["value"]

	if !ok {
		buckets[key].Fields["value"] = 0
	}

	buckets[key].Timestamp = receivedMetric.Timestamp
	buckets[key].Fields["value"] = receivedMetric.Value
}

func counterAggregator(receivedMetric input.Metric, key metricKey) {
	_, ok := buckets[key].Fields["value"]

	if !ok {
		buckets[key].Fields["value"] = 0.0
	}

	//to avoid the metric being lost, if sampling is undefined set it to 1
	//unless the client is misbehaving, this shouldn't happen
	if receivedMetric.Sampling == 0 {
		receivedMetric.Sampling = 1
	}

	//updating the value is broken down into several lines in order to make dealing
	//with type coersion easier
	sampledValue := receivedMetric.Value * (1 / receivedMetric.Sampling)
	previousValue := buckets[key].Fields["value"].(float64)
	buckets[key].Fields["value"] = sampledValue + previousValue
	buckets[key].Timestamp = receivedMetric.Timestamp
}

func setAggregator(receivedMetric input.Metric, key metricKey) {
	k := strconv.FormatFloat(float64(receivedMetric.Value), 'f', 2, 32)
	buckets[key].Fields[k] = receivedMetric.Value
}

func histogramAggregator(receivedMetric input.Metric, key metricKey) {
	histogram := buckets[key]
	histogram.Timestamp = receivedMetric.Timestamp
	histogram.Values = append(histogram.Values, receivedMetric.Value)
	sort.Float64s(histogram.Values)
	count := float64(len(histogram.Values))

	total := 0.0
	for _, x := range histogram.Values {
		total += x
	}

	//calculate stats from the values
	//go doesn't seem to have a decent stats library so none is used
	average := total / count
	median := histogram.Values[len(histogram.Values)/2]
	max := histogram.Values[int(count-1)]
	index := float64(0.95) * count
	percentile95 := histogram.Values[int(index)]

	histogram.Fields["count"] = count
	histogram.Fields["avg"] = average
	histogram.Fields["median"] = median
	histogram.Fields["max"] = max
	histogram.Fields["95percentile"] = percentile95

}
