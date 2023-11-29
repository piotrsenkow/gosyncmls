package main

import (
	_ "github.com/lib/pq"
	"github.com/piotrsenkow/gosyncmls/api"
	"github.com/piotrsenkow/gosyncmls/cmd"
	"github.com/piotrsenkow/gosyncmls/database"
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/piotrsenkow/gosyncmls/utils"
	"os"
	"os/signal"
	"syscall"
)

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

func initializeDependencies() {
	// Initialize logger
	utils.InitializeLogger()

	// Initialize the HTTP client
	api.InitializeHttpClient()

	// Initialize limiters
	services.InitializeRateLimiter()

	//// Initialize the database connection
	_, err := database.InitializeDb()
	if err != nil {
		utils.LogEvent("fatal", "Failed to connect to the database: "+err.Error())
		os.Exit(1)
	}

	// Initialize how program handles termination signals
	setupSignalHandlers()

	// Instantiate and assign the global rate tracker
	services.GlobalRateTracker = services.NewRateTracker()
}

func main() {
	initializeDependencies()
	cmd.Execute()
}
