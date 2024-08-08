package cmd

import (
	"fmt"
	"github.com/piotrsenkow/gosyncmls/api"
	"github.com/piotrsenkow/gosyncmls/database"
	"github.com/piotrsenkow/gosyncmls/models"
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/piotrsenkow/gosyncmls/utils"
	"github.com/spf13/cobra"
	"os"
	"sync"
	"time"
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Use the update command after the initial-sync stage is complete",
	Long:  "Use update after the initial sync for replication queries to the MLSGrid api. Here listings can be added/updated/and deleted from your local database destinations.",
	Run: func(cmd *cobra.Command, args []string) {

		var nextUrl string
		var processDataSem = make(chan struct{}, threads)
		var wg sync.WaitGroup

		fmt.Printf("Starting update with %d threads...\n", threads)

		timestamp, err := database.GetLastModificationTimestamp()
		if err != nil {
			utils.LogEvent("info", "Couldn't get last modification timestamp ")
		}
		nextUrl = database.ConstructUpdateURL(timestamp)
		for {
			if nextUrl == "" {
				utils.LogEvent("info", "Waiting for all process data jobs to complete...")
				wg.Wait()
				utils.LogEvent("info", "Update complete. Exiting with exit code 0.")
				os.Exit(0)

			}

			if services.CanMakeRequest() {
				err := utils.WithRetry(3, 2*time.Second, func() error {
					// in order for withRetry to work its necessary that makeRequestAndUpdateCounters helper function returns an err or nil.
					resp, err := api.MakeRequestAndUpdateCounters(nextUrl)
					if err != nil {
						return err
					}
					// only if we are able to make request and update counters should we try to update nextUrl, or we will lose info.
					nextUrl = resp.NextLink

					// Processing logic will only begin if semaphore token process worker is available
					utils.LogEvent("info", "Waiting to acquire a process data worker token...")
					processDataSem <- struct{}{}
					utils.LogEvent("info", "Process data worker token acquired.")

					// Increment the WaitGroup counter
					wg.Add(1)

					// process data in a go routine
					go func(response models.ApiResponse) {
						defer wg.Done() // Decrement the counter when the goroutine completes
						database.ProcessData(response.Data)
						// release the semaphore token once completes
						<-processDataSem
						utils.LogEvent("info", "Process data job complete. Releasing a token...")
					}(resp)

					return nil
				})
				if err != nil {
					utils.LogEvent("error", "Broken outside of withRetry loop, sleeping for 10 seconds before trying to make another request... Error: "+err.Error())
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				utils.LogEvent("warn", "Can't make a request at the moment.")
				timestamp, err := database.GetLastModificationTimestamp()
				if err != nil {
					utils.LogEvent("info", "Couldn't get last modification timestamp.")
				}
				nextUrl = database.ConstructUpdateURL(timestamp)
				utils.LogEvent("info", "Sleeping for 10 seconds before trying to make another request...")
				time.Sleep(10 * time.Second)
			}
		}

	},
}
