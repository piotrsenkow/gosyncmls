package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var cmdVersion = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of GoSyncMLS",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("GoSyncMLS v0.1 -- HEAD")
	},
}

func init() {
	rootCmd.AddCommand(cmdVersion)
}
