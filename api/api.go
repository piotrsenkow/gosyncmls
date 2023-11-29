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

// countingReader is a helper struct that counts the number of bytes read.
type countingReader struct {
	r io.Reader
	n *int64
}

// Read is a helper function that reads from the reader and updates the number of bytes read.
func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	*cr.n += int64(n)
	return n, err
}

// InitializeHttpClient is a helper function that initializes the http client.
func InitializeHttpClient() {
	httpClient = &http.Client{}
}

// MakeRequestAndUpdateCounters is a helper function that calls helper function MakeRequest2() and updates the global rate tracker counters + total bytes downloaded.
func MakeRequestAndUpdateCounters(url string) (models.ApiResponse, error) {
	resp, downloadSize, err := MakeRequest2(url)
	if err != nil {
		utils.LogEvent("Error", "Error: "+err.Error())
	}
	services.GlobalRateTracker.DataDownloaded += downloadSize
	services.GlobalRateTracker.IncrementRequestsThisHour()
	services.GlobalRateTracker.IncrementRequestsToday()

	downloadedGB := float64(services.GlobalRateTracker.DataDownloaded) / float64(1024*1024*1024) // Convert bytes to GB
	utils.LogEvent("info", fmt.Sprintf("Requests this hour: %d. Requests today: %d. Downloaded %.3fGB this hour.", services.GlobalRateTracker.RequestsThisHour, services.GlobalRateTracker.RequestsToday, downloadedGB))
	return resp, err
}

// MakeRequest2 is a helper function that makes a request to the MLSGrid API and returns the response and the number of bytes downloaded.
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
