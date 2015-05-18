package main

import (
	"strconv"
	"strings"
	"time"
)

func parseDogStatsDMetric(message string) metric {
	name := message[0:strings.Index(message, ":")]
	value := message[strings.Index(message, ":")+1 : strings.Index(message, "|")]
	floatValue, _ := strconv.ParseFloat(value, 64)
	metricType := message[strings.Index(message, "|")+1 : strings.Index(message, "@")-1]
	sampleRate := message[strings.Index(message, "@")+1 : strings.Index(message, "#")-1]
	floatSampleRate, _ := strconv.ParseFloat(sampleRate, 64)

	tags := message[strings.Index(message, "#")+1 : len(message)]
	tagMap := make(map[string]string)
	finished := false

	for !finished {
		comma := strings.Index(tags, ",")
		tagEnd := 0

		if comma == -1 {
			finished = true
			tagEnd = len(tags)
		} else {
			tagEnd = comma
		}

		tag := tags[0:tagEnd]
		colon := strings.Index(tag, ":")

		//check if tag is a kv tag
		if colon != -1 {
			key := tag[0:colon]
			value := tag[colon+1 : len(tag)]
			tagMap[key] = value
		} else {
			//this is a bit hacky, but all tags are reprented as a map
			//might be worth having a list of tags too (maybe in the map)
			tagMap[tag] = tag
		}

		tags = tags[comma+1 : len(tags)]

	}

	t := time.Now()

	parsedMetric := metric{
		Name:      name,
		Timestamp: t.Format(time.RFC1123),
		Type:      metricType,
		Value:     floatValue,
		Sampling:  floatSampleRate,
		Tags:      tagMap,
	}

	return parsedMetric
}
