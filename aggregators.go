package main

import (
	"sort"
	"strconv"

	"github.com/ccpgames/aggregateD/input"
	"github.com/ccpgames/aggregateD/output"
)

func (m *Main) gaugeAggregator(receivedMetric input.Metric, bucket *output.Bucket) {
	_, ok := bucket.Fields["value"]

	if !ok {
		bucket.Fields["value"] = 0
	}

	bucket.Timestamp = parseTimestamp(receivedMetric.Timestamp)
	bucket.Fields["value"] = receivedMetric.Value
}

func (m *Main) counterAggregator(receivedMetric input.Metric, bucket *output.Bucket) {
	_, ok := bucket.Fields["value"]

	if !ok {
		bucket.Fields["value"] = 0.0
	}

	//to avoid the metric being lost, if sampling is undefined set it to 1
	//unless the client is misbehaving, this shouldn't happen
	if receivedMetric.Sampling == 0 {
		receivedMetric.Sampling = 1
	}

	//updating the value is broken down into several lines in order to make dealing
	//with type coersion easier
	sampledValue := receivedMetric.Value * (1 / receivedMetric.Sampling)
	previousValue := bucket.Fields["value"].(float64)
	bucket.Fields["value"] = sampledValue + previousValue
	bucket.Timestamp = parseTimestamp(receivedMetric.Timestamp)

}

func (m *Main) setAggregator(receivedMetric input.Metric, bucket *output.Bucket) {
	k := strconv.FormatFloat(float64(receivedMetric.Value), 'f', 2, 32)
	bucket.Fields[k] = receivedMetric.Value
}

func (m *Main) histogramAggregator(receivedMetric input.Metric, bucket *output.Bucket) {
	bucket.Timestamp = parseTimestamp(receivedMetric.Timestamp)

	bucket.Values = append(bucket.Values, receivedMetric.Value)
	sort.Float64s(bucket.Values)
	count := float64(len(bucket.Values))

	total := 0.0
	for _, x := range bucket.Values {
		total += x
	}

	//calculate stats from the values
	//go doesn't seem to have a decent stats library so none is used
	average := total / count
	median := bucket.Values[len(bucket.Values)/2]
	max := bucket.Values[int(count-1)]
	min := bucket.Values[0]
	index := float64(0.95) * count
	percentile95 := bucket.Values[int(index)]

	bucket.Fields["count"] = count
	bucket.Fields["avg"] = average
	bucket.Fields["median"] = median
	bucket.Fields["max"] = max
	bucket.Fields["min"] = min
	bucket.Fields["95percentile"] = percentile95

}
