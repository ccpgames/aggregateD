package input

import (
	"testing"
)

func TestValidMessage(t *testing.T) {
	metric, _ := parseStatDMetric("example1:1|c|@0.1")

	if metric.Name == "example1" {
		t.Error("Metric name should be example1, got", metric.Name)
	}
}

func TestSplitMessages(t *testing.T) {
	//example string taken from statsD github
	metrics := "gorets:1|c\nglork:320|ms\ngaugor:333|g\nuniques:765|s"

	result := splitStatsDMessages(metrics)
	if len(result) != 4 {
		t.Error("Exected 4 split messages got", len(result))
	}
}
