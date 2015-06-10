package output

import (
	"bytes"
	"encoding/json"
	"net/http"
)

//WriteJSON POSTs the json encoded bucket to the defined URL
func WriteJSON(buckets []Bucket, url string) {
	for i := range buckets {
		jsonStr, _ := json.Marshal(buckets[i])
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
		req.Close = true
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		client.Do(req)
		req.Body.Close()
	}
}
