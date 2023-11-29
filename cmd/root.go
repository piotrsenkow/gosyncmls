package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"runtime"
)

var threads int

var rootCmd = &cobra.Command{
	Use:   "gosyncmls",
	Short: "GoSyncMLS is a CLI tool for syncing MLS data",
	Run: func(cmd *cobra.Command, args []string) {
		// Your code here
		fmt.Println("Welcome to GoSyncMLS CLI tool")
		dbConnStr := viper.GetString("DB_CONN_STRING")
		fmt.Printf("DB Connection String: %s\n", dbConnStr)

		APIBearerToken := viper.GetString("API_BEARER_TOKEN")
		fmt.Printf("API Bearer Token: %s\n", APIBearerToken)
		fmt.Printf("Threads chosen: %d\n", threads)
	},
}

// Execute executes the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "T", 2, "Number of threads")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.AutomaticEnv() // read in environment variables that match

	// If you have specific configuration files, you can set them up here
	// viper.SetConfigName("config")
	// viper.AddConfigPath("/etc/yourapp/")
	// viper.AddConfigPath("$HOME/.yourapp")
	// viper.ReadInConfig()
	// Limit the number of threads to the number of available CPU threads
	availableCPUs := runtime.NumCPU()
	if threads > availableCPUs {
		fmt.Printf("Reducing threads from %d to %d (number of available CPUs)\n", threads, availableCPUs)
		threads = availableCPUs
	}
}
