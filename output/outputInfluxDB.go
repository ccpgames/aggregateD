package output

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/client"
)

type (
	//InfluxDBConfig describes the configuration details for Influx connection
	InfluxDBConfig struct {
		InfluxHost      string
		InfluxPort      string
		InfluxUsername  string
		InfluxPassword  string
		InfluxDefaultDB string
	}

	//Bucket is a struct representing an aggregated series of metrics.
	//It closely resembles the InfluxDB client.Point but has some other useful
	//fields
	Bucket struct {
		Name      string            `json:"name"`
		Timestamp string            `json:"timestamp"`
		Tags      map[string]string `json:"tags"`
		//intermediate values for histograms, only fields are sent to influxdb
		Values []float64              `json:"-"`
		Fields map[string]interface{} `json:"fields"`
	}
)

//WriteToInfluxDB takes a map of bucket slices, indexed by database and writes
//each of those slices to InfluxDB as batch points
func WriteToInfluxDB(buckets []Bucket, config InfluxDBConfig, database string) error {
	client := configureInfluxDB(config)
	return writeInfluxDB(buckets, &client, database)
}

//ConfigureInfluxDB takes a struct describing the influx config and returns a Influx connection
func configureInfluxDB(config InfluxDBConfig) client.Client {

	influxURL, err := url.Parse(fmt.Sprintf("http://%s:%s", config.InfluxHost, config.InfluxPort))
	if err != nil {
		log.Fatal(err)
	}

	conf := client.Config{
		URL:      *influxURL,
		Username: config.InfluxUsername,
		Password: config.InfluxPassword,
	}

	influxConnection, err := client.NewClient(conf)

	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	return *influxConnection
}

//WriteInfluxDB commits the buckets to InfluxDB
//This should be compatable with the 0.9x releases of InfluxDB, as the 0.9 series is
//still in beta, it is prone to change which might break this function as was the
//case when Name was changed to Measurement in client.Point
func writeInfluxDB(buckets []Bucket, influxConnection *client.Client, database string) error {
	var (
		points      = make([]client.Point, len(buckets))
		pointsIndex = 0
	)

	for k := range buckets {
		bucket := buckets[k]
		timestamp, _ := time.Parse("YYYY-MM-DD HH:MM:SS.mmm", bucket.Timestamp)

		points[pointsIndex] = client.Point{
			Measurement: bucket.Name,
			Tags:        bucket.Tags,
			Fields:      bucket.Fields,
			Time:        timestamp,
		}
		pointsIndex++
	}

	pointsBatch := client.BatchPoints{
		Points:          points,
		Database:        database,
		RetentionPolicy: "default",
	}

	fmt.Println("WRITING")
	_, err := influxConnection.Write(pointsBatch)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
