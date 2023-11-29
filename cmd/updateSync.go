package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Use the update command after the initial-sync stage is complete",
	Long:  "Use update after the initial sync for replication queries to the MLSGrid api. Here listings can be added/updated/and deleted from your local database destinations.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Updating with %d threads...\n", threads)
		// Your sync logic here
	},
}
