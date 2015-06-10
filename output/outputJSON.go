package output

import (
	"encoding/json"
	"net/http"
	"strings"
)

//WriteJSON POSTs the json encoded bucket to the defined URL
func WriteJSON(buckets []Bucket, url string) {
	for i := range buckets {
		jsonStr, _ := json.Marshal(buckets[i])
		client := &http.Client{}
		request, _ := http.NewRequest("PUT", url, strings.NewReader(string(jsonStr)))
		request.Header.Set("Content-Type", "application/json")
		request.ContentLength = 23
		response, err := client.Do(request)

		if err == nil {
			defer response.Body.Close()

		}
	}
}
