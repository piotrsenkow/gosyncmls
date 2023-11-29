package cmd

import (
	"github.com/piotrsenkow/gosyncmls/services"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Run GoSyncMLS to start the import process or update the database",
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
