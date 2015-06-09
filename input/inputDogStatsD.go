package input

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

//ServeUDP serves the dogstatsD protocol over UDP
func ServeUDP(port string, metricsIn chan Metric, eventsIn chan Event) string {
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

		if strings.Index(message, "_e") == -1 {
			metric, _ := parseDogStatsDMetric(message)
			metricsIn <- metric
		} else {
			//parseDogStatDEvent(message)
		}
	}

}
func parseDogStatsDMetric(message string) (Metric, error) {
	//function to parse a metric struct from a dogstatsd message which takes the
	//form of:
	//metric.name:value|type|@sample_rate|#tag1:value,tag2

	//the indicies of the various delimiters
	colonIndex := strings.Index(message, ":")
	ibarIndex := strings.Index(message, "|")
	atIndex := strings.Index(message, "@")
	hashIndex := strings.Index(message, "#")
	tagMap := make(map[string]string)

	if colonIndex == -1 || ibarIndex == -1 || atIndex == -1 {
		return Metric{}, errors.New("unable to parse DogStatsD message")
	}

	//if there is no hash, there are no tags. therefore set hashIndex as the
	//end of the message and set finished to true so tag parsing never occurs
	if hashIndex == -1 {
		hashIndex = len(message)
	} else {
		tags := message[hashIndex+1 : len(message)]
		tagMap = parseTags(tags)
	}

	name := message[0:colonIndex]
	value := message[colonIndex+1 : ibarIndex]
	floatValue, _ := strconv.ParseFloat(value, 64)
	metricType := message[ibarIndex+1 : atIndex-1]
	sampleRate := message[atIndex+1 : hashIndex-1]

	if sampleRate == "" {
		return Metric{}, errors.New("unable to parse DogStatsD message")
	}

	floatSampleRate, _ := strconv.ParseFloat(sampleRate, 64)

	t := time.Now()

	parsedMetric := Metric{
		Name:      name,
		Timestamp: t.Format(time.RFC1123),
		Type:      metricType,
		Value:     floatValue,
		Sampling:  floatSampleRate,
		Tags:      tagMap,
	}

	return parsedMetric, nil
}

// func parseDogStatsDEvent(message string) {
// 	//_e{title.length,text.length}:title|text|d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2
//
// 	//a := message[strings.Index(message, "p:")+2 : len(message)]
//
// 	titleLengthString := message[strings.Index(message, "{")+1 : strings.Index(message, ",")]
// 	titleLength, _ := strconv.ParseInt(titleLengthString, 0, 64)
//
// 	title := message[strings.Index(message, ":") : strings.Index(message, ":")+titleLength]
// 	panic(titleLength)
// }

func parseTags(tags string) map[string]string {
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

	return tagMap
}
