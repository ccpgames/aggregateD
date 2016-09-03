package output

import (
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type (
	//InfluxDBConfig describes the configuration details for Influx connection
	InfluxDBConfig struct {
		InfluxURL       string
		InfluxUsername  string
		InfluxPassword  string
		InfluxDefaultDB string
	}

	//Bucket is a struct representing an aggregated series of metrics.
	//It closely resembles the InfluxDB client.Point but has some other useful
	//fields
	Bucket struct {
		Name      string            `json:"name"`
		Timestamp time.Time         `json:"timestamp"`
		Tags      map[string]string `json:"tags"`
		//intermediate values for histograms, only fields are sent to influxdb
		Values []float64              `json:"-"`
		Fields map[string]interface{} `json:"fields"`
	}
)

//WriteToInfluxDB takes a map of bucket slices, indexed by database and writes
//each of those slices to InfluxDB as batch points
func WriteToInfluxDB(buckets []Bucket, config InfluxDBConfig) error {
	c, clientErr := client.NewHTTPClient(client.HTTPConfig{
		Addr:     config.InfluxURL,
		Username: config.InfluxUsername,
		Password: config.InfluxPassword,
	})

	if clientErr != nil {
		return clientErr
	}

	points, pointErr := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  config.InfluxDefaultDB,
		Precision: "s",
	})

	if pointErr != nil {
		return pointErr
	}

	for k := range buckets {
		bucket := buckets[k]

		point, err := client.NewPoint(
			bucket.Name,
			bucket.Tags,
			bucket.Fields,
			bucket.Timestamp)

		if err != nil {
			log.Printf("Malformed point, {%s, %s, %s %s} excluded from batch", bucket.Name, bucket.Tags, bucket.Fields, bucket.Timestamp)
		} else {
			points.AddPoint(point)
		}
	}
	log.Printf("Writing %d points to InfluxDB", len(points.Points()))

	writeError := c.Write(points)

	if writeError != nil {
		log.Println(writeError)
		return writeError
	}

	return nil
}
