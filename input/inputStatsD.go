package input

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

//ServeStatD serves the statsad protocol via UDP.
func ServeStatD(port string, metricsIn chan Metric) string {
	var buf [1024]byte
	addr, err := net.ResolveUDPAddr("udp", ":"+port)

	if err != nil {
		panic(err)
	}

	sock, err := net.ListenUDP("udp", addr)

	if err != nil {
		panic(err)
	}

	for {
		rlen, _, _ := sock.ReadFromUDP(buf[:])
		message := string(buf[:rlen])
		//a single statsD message can contain multiple metrics
		//split and then interate through each to parse and submit
		//for aggregategation
		metrics := splitStatsDMessages(message)

		for _, metric := range metrics {
			parsedMetric, err := parseStatDMetric(metric)

			if err != nil {
				//add tag to metric denoting its point of origin
				parsedMetric.Timestamp = time.Now().Format("2006-01-02 15:04:05 -0700")
				metricsIn <- parsedMetric
			}
		}

	}
}

//split statsd messages that contain multiple metrics into its respective parts
func splitStatsDMessages(messages string) []string {
	var splitMessages []string
	newline := strings.Index(messages, "\n")

	for newline != -1 {
		message := messages[0 : newline-1]
		splitMessages = append(splitMessages, message)
		messages = messages[newline+1 : len(messages)]
		newline = strings.Index(messages, "\n")
	}

	if len(messages) > 0 {
		splitMessages = append(splitMessages, messages)
	}

	return splitMessages
}

func parseStatDMetric(message string) (Metric, error) {
	var metric Metric

	colonIndex := strings.Index(message, ":")
	ibarIndex := strings.Index(message, "|")
	atIndex := strings.Index(message, "@")

	if colonIndex == -1 || ibarIndex == -1 {
		return Metric{}, errors.New("unable to parse name from statsD message")
	}

	metric.Name = string(message[colonIndex-1])
	stringValue := message[colonIndex+1 : ibarIndex]
	floatValue, err := strconv.ParseFloat(stringValue, 64)
	metric.Value = floatValue

	if err != nil {
		return Metric{}, errors.New("unable to parse value from statsD message")
	}

	metricType := ""

	if atIndex == -1 {
		metricType = message[ibarIndex+1 : len(message)]
		metric.Sampling = 1
	} else {
		metricType = message[ibarIndex+1 : atIndex-1]
		sampleRate := message[atIndex+1 : len(message)]
		floatSampleRate, err := strconv.ParseFloat(sampleRate, 64)

		if err != nil {
			return Metric{}, errors.New("unable to parse StatsD value")
		}

		metric.Sampling = floatSampleRate
	}

	switch string(metricType[:len(metricType)]) {
	case "ms":
		metric.Type = "timer"
	case "g":
		metric.Type = "gauge"
	case "c":
		metric.Type = "counter"
	default:
		fmt.Println(metricType)
		err = fmt.Errorf("invalid metric type: %q", metricType)
		return metric, err
	}

	return metric, nil
}
