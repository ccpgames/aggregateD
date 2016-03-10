package output

import (
	"encoding/json"
	"net/url"
)
import "github.com/mediocregopher/radix.v2/redis"

//WriteRedis writes buckets to a Redis list. Buckets are encoded
//as JSON. Returns err if a write fails.
func WriteRedis(buckets []Bucket, redisURL url.URL) error {
	redisClient, redisErr := redis.Dial("tcp", redisURL.String())
	if redisErr != nil {
		return redisErr
	}

	for bucket := range buckets {
		jsonBucket, jsonErr := json.Marshal(bucket)

		if jsonErr == nil {
			resp := redisClient.Cmd("RPUSH", "aggregated", jsonBucket)
			if resp.Err != nil {
				return resp.Err
			}
		}
	}

	return nil

}
