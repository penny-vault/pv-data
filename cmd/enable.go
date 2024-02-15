/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"

	"github.com/penny-vault/pvdata/library"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// enableCmd represents the enable command
var enableCmd = &cobra.Command{
	Use:   "enable <subscription-id>",
	Short: "Enable inactive subscriptions",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		myLibrary, err := library.NewFromDB(ctx, viper.GetString("dbUrl"))
		if err != nil {
			log.Fatal().Err(err).Msg("could not load library info")
		}

		for _, id := range args {
			sub, err := myLibrary.SubscriptionFromID(ctx, id)
			if err != nil {
				log.Fatal().Err(err).Str("ID", id).Msg("could not get subscription for ID")
			}

			if err := sub.Activate(ctx); err != nil {
				log.Fatal().Err(err).Msg("could not activate subscription")
			}

			log.Info().Str("ID", id).Msg("subscription enabled")
		}
	},
}

func init() {
	rootCmd.AddCommand(enableCmd)
}
