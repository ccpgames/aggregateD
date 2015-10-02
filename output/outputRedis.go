package output

import (
	"encoding/json"
	"net/url"
)
import "github.com/mediocregopher/radix.v2/redis"

//WriteRedis writes buckets to a Redis list. Buckets are encoded
//as JSON. Returns err if a write fails.
func WriteRedis(buckets []Bucket, redisURL url.URL) error {
	redisClient, redisErr := redis.Dial("tcp", "localhost:6379")
	if redisErr != nil {
		return redisErr
	}

	for bucket := range buckets {
		jsonBucket, jsonErr := json.Marshal(bucket)

		if jsonErr == nil {
			redisClient.PipeAppend("APPEND", "aggregated-failed", jsonBucket)
		}

	}

	response := redisClient.ReadResp()
	responseErr := response.Err

	if responseErr != nil {
		return responseErr
	}

	return nil

}
