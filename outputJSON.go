package main

import (
	"bytes"
	"encoding/json"
	"net/http"
)

func writeJSON(buckets []bucket, url string) {
	for i := range buckets {
		jsonStr, _ := json.Marshal(buckets[i])
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		client.Do(req)
	}
}
