package config

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/url"

	"github.com/ccpgames/aggregateD/health"
	"github.com/ccpgames/aggregateD/input"
	"github.com/ccpgames/aggregateD/output"
	"github.com/spf13/viper"
)

//Configuration encapsulates all config options for aggregated
type Configuration struct {
	InfluxConfig        output.InfluxDBConfig
	JSONOutputURL       url.URL
	RedisOutputURL      url.URL
	FlushInterval       int
	AggregationInterval int
}

//ReadConfig takes a file path as a string and returns a string representing
//the contents of that file
func ReadConfig(configFile string) ([]byte, error) {
	//viper accepts config file without extension, so remove extension
	if configFile == "" {
		panic("No config file provided")
	}

	f, err := ioutil.ReadFile(configFile)

	if err != nil {
		log.Fatal(err)
	}

	return f, err
}

//ParseConfig reads in a config file entitled in yaml format and starts
//the appropriate input listeners and returns a
//Configuration struct representing the parsed configuration
func ParseConfig(rawConfig []byte, metricsIn chan input.Metric, eventsIn chan input.Event) Configuration {
	parsedConfig := new(Configuration)
	outputUndefined := true
	inputUndefied := true

	viper.SetConfigType("yaml")
	viper.ReadConfig(bytes.NewBuffer(rawConfig))

	if viper.GetBool("outputInfluxDB") {
		parsedConfig.InfluxConfig = output.InfluxDBConfig{
			InfluxURL:       viper.GetString("influx.url"),
			InfluxUsername:  viper.GetString("influx.username"),
			InfluxPassword:  viper.GetString("influx.password"),
			InfluxDefaultDB: viper.GetString("influx.defaultDB"),
		}
		outputUndefined = false
	}

	if viper.GetBool("redisOnInfluxFail") {
		redisURL, err := url.Parse(viper.GetString("redisOutputURL"))

		if err != nil {
			panic("malformed redis URL")
		}

		parsedConfig.RedisOutputURL = *redisURL

	}
	if (len(parsedConfig.InfluxConfig.InfluxURL)) == 0 {
		panic("InfluxDB URL undefined")
	}

	if (len(parsedConfig.InfluxConfig.InfluxUsername)) == 0 {
		panic("InfluxDB username undefined")
	}

	if (len(parsedConfig.InfluxConfig.InfluxPassword)) == 0 {
		panic("InfluxDB password undefined")
	}

	if (len(parsedConfig.InfluxConfig.InfluxDefaultDB)) == 0 {
		panic("InfluxDB default db undefined")
	}

	if viper.GetBool("outputJSON") {
		u, err := url.Parse(viper.GetString("JSONOutputURL"))
		if err != nil {
			log.Fatal(err)
		}
		parsedConfig.JSONOutputURL = *u
		outputUndefined = false
	}

	//if there is no where defined to submit metrics to, exit
	if outputUndefined {
		panic("No outputs defined")
	}

	if viper.GetBool("inputJSON") {
		viper.SetDefault("HTTPPort", "8003")
		go input.ServeHTTP(viper.GetString("HTTPPort"), metricsIn, eventsIn)
		inputUndefied = false
	}

	if viper.GetBool("inputDogStatsD") {
		viper.SetDefault("UDPPort", "8125")
		go input.ServeDogStatsD(viper.GetString("UDPPort"), metricsIn, eventsIn)
		inputUndefied = false
	}

	if viper.GetBool("inputStatsD") {
		viper.SetDefault("UDPPort", "8125")
		go input.ServeStatD(viper.GetString("UDPPort"), metricsIn)
		inputUndefied = false
	}

	if inputUndefied {
		panic("No inputs defined")
	}

	if viper.GetBool("healthCheck") {
		go health.Serve(parsedConfig.InfluxConfig)
	}

	//default write interval is 60 seconds
	viper.SetDefault("flushInterval", 60)
	parsedConfig.FlushInterval = viper.GetInt("flushInterval")

	viper.SetDefault("aggregationInterval", 10)
	parsedConfig.AggregationInterval = viper.GetInt("aggregationInterval")

	return *parsedConfig
}
