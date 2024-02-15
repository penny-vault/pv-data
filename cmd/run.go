/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"os"
	"time"

	"github.com/hako/durafmt"
	"github.com/penny-vault/pvdata/library"
	"github.com/penny-vault/pvdata/providers"
	"github.com/penny-vault/pvdata/providers/provider"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run [subscription-id...]",
	Short: "Run data import subscriptions",
	Long: `The run sub-command executes subscriptions and saves the data they generate. If no
arguments are provided then run will execute as a daemon and execute each subscription at the
scheduled times. If subscription IDs are provided then each subscription will execute
sequentially (ignoring any set schedule).`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		// load the library
		myLibrary, err := library.NewFromDB(ctx, viper.GetString("dbUrl"))
		if err != nil {
			log.Fatal().Err(err).Msg("could not connect to library")
		}

		// check if we are running in daemon mode
		if len(args) == 0 {
			// no args provided -- run as a daemon
			// TODO
			os.Exit(0)
		}

		// not daemon mode, execute each subscription individually
		for _, subscriptionID := range args {
			subscription, err := myLibrary.SubscriptionFromID(ctx, subscriptionID)
			if err != nil {
				log.Fatal().Err(err).Str("SubscriptionID", subscriptionID).Msg("could not load subscription")
			}

			var (
				subProvider provider.Provider
				subDataset  provider.Dataset
				ok          bool
			)

			if subProvider, ok = providers.Map[subscription.Provider]; !ok {
				log.Fatal().Str("ProviderKey", subscription.Provider).Msg("subscription is mis-configured, provider not found")
			}

			if subDataset, ok = subProvider.Datasets()[subscription.Dataset]; !ok {
				log.Fatal().Str("ProviderKey", subscription.Provider).Str("DatasetKey", subscription.Dataset).
					Msg("subscription is mis-configured, dataset not found")
			}

			outChan := make(chan interface{}, 100)
			progressChan := make(chan int, 100)

			fetchLogger := log.With().Str("SubscriptionID", subscriptionID).Logger()

			startTime := time.Now()
			numRet, err := subDataset.Fetch(subscription.Config, subscription.DataTables, outChan, fetchLogger, progressChan)
			if err != nil {
				fetchLogger.Fatal().Err(err).Msg("fetch returned an error")
			}

			endTime := time.Now()

			runTime := endTime.Sub(startTime)

			fetchLogger.Info().Str("RunTime", durafmt.Parse(runTime).String()).Int("NumberReturned", numRet).Msg("successfully fetched results")
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
