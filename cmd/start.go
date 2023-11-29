package cmd

import (
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Run GoSyncMLS to start the import process or update the database",
	// PreRun is a Cobra hook that runs before the command is executed. We use it to start the tickers once the start command is run and only once using a flag inside StartTickers() function.
	// Reason for that is that we want to start the tickers only once, and not every time the start command is run.
	PreRun: func(cmd *cobra.Command, args []string) {
		// Start tickers here
		services.StartTickers()
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.AddCommand(initialSyncCmd)
	startCmd.AddCommand(updateCmd)
}
