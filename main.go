package main

import (
	"fmt"
	"github.com/cenkalti/backoff/v4"
	_ "github.com/lib/pq"
	"github.com/piotrsenkow/gosyncmls/api"
	"github.com/piotrsenkow/gosyncmls/database"
	"github.com/piotrsenkow/gosyncmls/models"
	"github.com/piotrsenkow/gosyncmls/utils"
	"golang.org/x/time/rate"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	initialUrl         = "https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000"
	MaxRequestsPerHour = 7200
	MaxRequestsPerDay  = 40000
	MaxDownloadPerHour = 4 * 1024 * 1024 * 1024 // 4GB in bytes
)

var (
	requestsThisHour int
	requestsToday    int

	// Defining limiters as package-level variables
	perSecondLimiter *rate.Limiter
	perHourLimiter   *rate.Limiter
	perDayLimiter    *rate.Limiter
	hourTicker       *time.Ticker
	dayTicker        *time.Ticker
	dataDownloaded   int64 = 0
	// Buffered channel allowing up to 5 workers.
	processDataSem = make(chan struct{}, 5)
)

// Implement the withRetry function
func withRetry(attempts int, sleep time.Duration, fn func() error) error {
	for i := 0; ; i++ {
		err := fn()
		if err == nil {
			return nil // success
		}

		if i >= (attempts - 1) {
			return err // return the last error
		}

		utils.LogEvent("warn", fmt.Sprintf("Attempt %d failed; retrying in %v", i+1, sleep))
		time.Sleep(sleep)
		sleep *= 2
	}
}

func canMakeRequest() bool {
	if !isWithinRateLimit(perDayLimiter, "day") {
		return false
	}
	if !isWithinRateLimit(perHourLimiter, "hour") {
		return false
	}
	if !isWithinRateLimit(perSecondLimiter, "second") {
		return false
	}
	if dataDownloaded >= MaxDownloadPerHour {
		utils.LogEvent("warn", "4 GB hourly download limit reached!")
	}
	return requestsThisHour <= MaxRequestsPerHour && dataDownloaded <= MaxDownloadPerHour &&
		requestsToday <= MaxRequestsPerDay
}

func isWithinRateLimit(limiter *rate.Limiter, period string) bool {
	reserve := limiter.Reserve()
	if !reserve.OK() {
		delay := reserve.Delay()
		utils.LogEvent("warn", fmt.Sprintf("Rate limited per %s, waiting for %v", period, delay))
		time.Sleep(delay)
		return false
	}
	return true
}

func handleApiErrors(fn func() error) {
	// What to do if API request failed
	operation := func() error {
		err := fn()
		if err != nil {
			// Log the error
			utils.LogEvent("info", "Backoff operation entered.")
			utils.LogEvent("error", "API Request Failed! "+err.Error())
		}
		return err
	}

	// Exponential backoff
	bo := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, bo)
	if err != nil {
		utils.LogEvent("error", "Failed after many retries using backoff")
	}
}

func startTickers() {
	// Anonymous function running on a GoThread that handles resetting of hourly / daily requests using time tickers
	go func() {
		for {
			select {
			case <-hourTicker.C:
				requestsThisHour = 0
				dataDownloaded = 0
				utils.LogEvent("info", "Hourly counter reset")
			case <-dayTicker.C:
				requestsToday = 0
				utils.LogEvent("info", "Daily counter reset")
			}
		}
	}()
}

func setupSignalHandlers() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		utils.LogEvent("info", "Received signal: "+sig.String())

		// Close database and API connections
		err := database.Db.Close()
		if err != nil {
			utils.LogEvent("trace", "Trace: "+err.Error())
		}
		os.Exit(0)
	}()
}

func initialize() {
	// Initialize logger
	utils.InitializeLogger()

	// Initialize the HTTP client
	api.InitializeHttpClient()

	// Initialize limiters
	perSecondLimiter = rate.NewLimiter(1.95, 2)
	perHourLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerHour)/3600, MaxRequestsPerHour)
	perDayLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerDay)/86400, MaxRequestsPerDay)

	// Tracking hours and days using tickers
	hourTicker = time.NewTicker(1 * time.Hour)
	dayTicker = time.NewTicker(24 * time.Hour)

	//// Initialize the database connection
	_, err := database.InitializeDb()
	if err != nil {
		utils.LogEvent("fatal", "Failed to connect to the database: "+err.Error())
		os.Exit(0)
	}
}

func makeRequestAndUpdateCounters(url string) (models.ApiResponse, error) {
	resp, downloadSize, err := api.MakeRequest2(url)
	dataDownloaded += downloadSize
	requestsThisHour++
	requestsToday++

	downloadedGB := float64(dataDownloaded) / float64(1024*1024*1024) // Convert bytes to GB
	utils.LogEvent("info", "Able to make a request within rate limits")
	utils.LogEvent("info", fmt.Sprintf("Requests this hour: %d. Requests today: %d", requestsThisHour, requestsToday))
	utils.LogEvent("info", fmt.Sprintf("Downloaded %.3fGB this hour.", downloadedGB))
	return resp, err
}

func main() {
	initialize()
	setupSignalHandlers()
	startTickers()

	handleApiErrors(func() error {
		//var nextUrl string = initialUrl
		timestamp, err := database.GetLastModificationTimestamp()
		if err != nil {
			utils.LogEvent("info", "Couldn't get last modification timestamp ")
		}
		var nextUrl string = database.ConstructUpdateURL(timestamp)

		for {
			if nextUrl == "" {
				utils.LogEvent("warn", "Initial import complete!")
				break
			}

			if canMakeRequest() {
				err := withRetry(3, 2*time.Second, func() error {
					resp, err := makeRequestAndUpdateCounters(nextUrl)
					nextUrl = resp.NextLink
					if err != nil {
						utils.LogEvent("error", "Error: "+err.Error())
						return err
					}
					// Acquire Semaphore token to process data
					processDataSem <- struct{}{}
					utils.LogEvent("info", "Process data worker token acquired.")
					// Processing logic, 5 threads max
					go func(response models.ApiResponse) {
						database.ProcessData(response.Data)
						// Release the semaphore token once processData completes
						<-processDataSem
						utils.LogEvent("info", "Process data worker finished. Released a token.")
					}(resp)
					return nil
				})
				if err != nil {
					utils.LogEvent("error", "Error: "+err.Error())
					// You might choose to break out of the loop, wait for a longer duration, or alert someone.
				}
			} else {
				timestamp, err := database.GetLastModificationTimestamp()
				if err != nil {
					utils.LogEvent("info", "Couldn't get last modification timestamp ")
				}
				nextUrl = database.ConstructUpdateURL(timestamp)
				utils.LogEvent("info", "Sleeping for a minute. Will check if can make a request after.")
				time.Sleep(1 * time.Minute)
			}
		}
		return nil
	})
}

// https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true%20and%20ModificationTimestamp%20gt%202023-10-05T16:47:03.961Z&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000
