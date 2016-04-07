package main

import (
	"sort"
	"strconv"

	"github.com/ccpgames/aggregateD/input"
)

func (m *Main) gaugeAggregator(receivedMetric input.Metric, key metricKey) {
	_, ok := m.metricBuckets[key].Fields["value"]

	if !ok {
		m.metricBuckets[key].Fields["value"] = 0
	}

	m.metricBuckets[key].Timestamp = parseTimestamp(receivedMetric.Timestamp)
	m.metricBuckets[key].Fields["value"] = receivedMetric.Value
}

func (m *Main) counterAggregator(receivedMetric input.Metric, key metricKey) {
	_, ok := m.metricBuckets[key].Fields["value"]

	if !ok {
		m.metricBuckets[key].Fields["value"] = 0.0
	}

	//to avoid the metric being lost, if sampling is undefined set it to 1
	//unless the client is misbehaving, this shouldn't happen
	if receivedMetric.Sampling == 0 {
		receivedMetric.Sampling = 1
	}

	//updating the value is broken down into several lines in order to make dealing
	//with type coersion easier
	sampledValue := receivedMetric.Value * (1 / receivedMetric.Sampling)
	previousValue := m.metricBuckets[key].Fields["value"].(float64)
	m.metricBuckets[key].Fields["value"] = sampledValue + previousValue
	m.metricBuckets[key].Timestamp = parseTimestamp(receivedMetric.Timestamp)

}

func (m *Main) setAggregator(receivedMetric input.Metric, key metricKey) {
	k := strconv.FormatFloat(float64(receivedMetric.Value), 'f', 2, 32)
	m.metricBuckets[key].Fields[k] = receivedMetric.Value
}

func (m *Main) histogramAggregator(receivedMetric input.Metric, key metricKey) {
	histogram := m.metricBuckets[key]
	histogram.Timestamp = parseTimestamp(receivedMetric.Timestamp)

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
	min := histogram.Values[0]
	index := float64(0.95) * count
	percentile95 := histogram.Values[int(index)]

	histogram.Fields["count"] = count
	histogram.Fields["avg"] = average
	histogram.Fields["median"] = median
	histogram.Fields["max"] = max
	histogram.Fields["min"] = min
	histogram.Fields["95percentile"] = percentile95

}
