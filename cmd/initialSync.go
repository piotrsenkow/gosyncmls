package cmd

import (
	"fmt"
	"github.com/piotrsenkow/gosyncmls/api"
	"github.com/piotrsenkow/gosyncmls/database"
	"github.com/piotrsenkow/gosyncmls/models"
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/piotrsenkow/gosyncmls/utils"
	"github.com/spf13/cobra"
	"time"
)

var initialSyncCmd = &cobra.Command{
	Use:   "initial-sync",
	Short: "Initial data download of an MLSGrid source to one or more local database destinations",
	Run: func(cmd *cobra.Command, args []string) {
		const initialUrl = "https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000"
		var nextUrl string
		var processDataSem = make(chan struct{}, threads)

		fmt.Printf("Beginning the initial download with %d threads...\n", threads)

		hasData, err := database.CheckIfPropertiesTableHasData()
		if err != nil {
			utils.LogEvent("error", "Error encountered querying properties table: "+err.Error())
		}
		// If table already has data it means import process was interrupted, now we have to add last max modification timestamp to query to pick up where we left off to finish the initial import sync.
		if hasData {
			timestamp, err := database.GetLastModificationTimestamp()
			if err != nil {
				utils.LogEvent("info", "Couldn't get last modification timestamp ")
			}
			nextUrl = database.ConstructUpdateURL(timestamp)
		} else {
			nextUrl = initialUrl
		}

		// infinite for loop runs until nextUrl is empty (no nextUrl present in api response AKA update complete + up-to-date) and we then break out
		for {
			if nextUrl == "" {
				utils.LogEvent("warn", "Initial-sync complete. Please verify that the latest modification_timestamp matches today's date. Switch to using the gosyncmls `start update` command from now on.")
				break
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

					// will begin processing logic if semaphore token process worker is available
					processDataSem <- struct{}{}
					utils.LogEvent("info", "Process data worker token acquired.")
					// process data in a go routine
					go func(response models.ApiResponse) {
						database.ProcessData(response.Data)
						// release the semaphore token once completes
						<-processDataSem
						utils.LogEvent("info", "Process data worker finished. Released a token.")
					}(resp)
					return nil
				})
				if err != nil {
					utils.LogEvent("error", "Broken outside of withRetry loop, error: "+err.Error())
					// have to determine what to do if we retry too many times.
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
