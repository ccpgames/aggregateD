package output

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

//WriteJSON POSTs the json encoded bucket to the defined URL
//This is mostly intended to be used for diaganostic output but
//can also be used to forward metrics to other services, it is
//configured by setting outputJSON to true and outputURL to a
//valid URL in the configuration file
func WriteJSON(buckets []Bucket, url string) {
	for i := range buckets {
		jsonStr, _ := json.Marshal(buckets[i])
		client := &http.Client{}
		request, _ := http.NewRequest("PUT", url, strings.NewReader(string(jsonStr)))
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)

		if err == nil {
			defer response.Body.Close()
			fmt.Println(response)
		} else {
			fmt.Println(err)
		}
	}
}
