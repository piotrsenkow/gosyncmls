package api

import (
	"encoding/json"
	"fmt"
	"github.com/piotrsenkow/gosyncmls/models"
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/piotrsenkow/gosyncmls/utils"
	"github.com/spf13/viper"
	"io"
	"net/http"
)

var httpClient *http.Client

type countingReader struct {
	r io.Reader
	n *int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	*cr.n += int64(n)
	return n, err
}

func InitializeHttpClient() {
	httpClient = &http.Client{}
}

func MakeRequestAndUpdateCounters(url string) (models.ApiResponse, error) {
	resp, downloadSize, err := MakeRequest2(url)
	if err != nil {
		utils.LogEvent("Error", "Error: "+err.Error())
	}
	services.GlobalRateTracker.DataDownloaded += downloadSize
	services.GlobalRateTracker.IncrementRequestsThisHour()
	services.GlobalRateTracker.IncrementRequestsToday()

	downloadedGB := float64(services.GlobalRateTracker.DataDownloaded) / float64(1024*1024*1024) // Convert bytes to GB
	utils.LogEvent("info", "Able to make a request within rate limits")
	utils.LogEvent("info", fmt.Sprintf("Requests this hour: %d. Requests today: %d", services.GlobalRateTracker.RequestsThisHour, services.GlobalRateTracker.RequestsToday))
	utils.LogEvent("info", fmt.Sprintf("Downloaded %.3fGB this hour.", downloadedGB))
	return resp, err
}

func MakeRequest2(url string) (models.ApiResponse, int64, error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		utils.LogEvent("error", "Error: "+err.Error())
		return models.ApiResponse{}, 0, err
	}

	// Add Bearer token for authentication
	//APIBearerToken := os.Getenv("API_BEARER_TOKEN")
	APIBearerToken := viper.GetString("API_BEARER_TOKEN")
	if APIBearerToken == "" {
		utils.LogEvent("fatal", "Fatal error: `API_BEARER_TOKEN` not set in environment variables.")
	}

	req.Header.Add("Authorization", "Bearer "+APIBearerToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return models.ApiResponse{}, 0, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			utils.LogEvent("trace", "Trace: "+err.Error())
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		utils.LogEvent("error", fmt.Sprintf("Received non-200 response from %s. Status: %d. Body: %s", url, resp.StatusCode, string(bodyBytes)))
		return models.ApiResponse{}, 0, fmt.Errorf("received non-200 response status: %d", resp.StatusCode)
	}

	var bytesRead int64
	cr := &countingReader{r: resp.Body, n: &bytesRead}

	var apiResp models.ApiResponse

	dec := json.NewDecoder(cr)
	err = dec.Decode(&apiResp)
	if err != nil {
		return models.ApiResponse{}, 0, err
	}

	return apiResp, bytesRead, nil
}
