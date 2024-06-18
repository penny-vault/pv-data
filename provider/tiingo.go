// Copyright 2024
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/library"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

type Tiingo struct {
}

func (tiingo *Tiingo) Name() string {
	return "tiingo"
}

func (tiingo *Tiingo) ConfigDescription() map[string]string {
	return map[string]string{
		"apiKey":    "Enter your tiingo API key:",
		"rateLimit": "What is the maximum number of requests per minute?",
	}
}

func (tiingo *Tiingo) Description() string {
	return `The Polygon.io Stocks API provides REST endpoints that let you query the latest market data from all US stock exchanges. You can also find data on company financials, stock market holidays, corporate actions, and more.`
}

func (tiingo *Tiingo) Datasets() map[string]Dataset {
	return map[string]Dataset{
		"EOD": {
			Name:        "EOD",
			Description: "Get end-of-day stock prices for active assets.",
			DataTypes:   []*data.DataType{data.DataTypes[data.EODKey]},
			DateRange: func() (time.Time, time.Time) {
				return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), time.Now().UTC()
			},
			Fetch: func(ctx context.Context, subscription *library.Subscription, out chan<- *data.Observation, exitNotification chan<- data.RunSummary) {
				logger := zerolog.Ctx(ctx)

				runSummary := data.RunSummary{
					StartTime:        time.Now(),
					SubscriptionID:   subscription.ID,
					SubscriptionName: subscription.Name,
				}

				numObs := 0

				defer func() {
					runSummary.EndTime = time.Now()
					runSummary.NumObservations = numObs
					exitNotification <- runSummary
				}()

				rateLimit, err := strconv.Atoi(subscription.Config["rateLimit"])
				if err != nil {
					logger.Error().Err(err).Str("configRateLimit", subscription.Config["rateLimit"]).Msg("could not convert rateLimit configuration parameter to an integer")
					return
				}

				if rateLimit <= 0 {
					rateLimit = 5000
				}

				client := resty.New().SetQueryParam("token", subscription.Config["apiKey"])
				limiter := rate.NewLimiter(rate.Limit(float64(rateLimit)/float64(61)), 1)

				// get nyc timezone
				nyc, err := time.LoadLocation("America/New_York")
				if err != nil {
					logger.Panic().Err(err).Msg("could not load timezone")
					return
				}

				// fetch ticker EOD prices
				if err := limiter.Wait(ctx); err != nil {
					log.Panic().Err(err).Msg("rate limit wait failed")
				}

				// Get a list of active assets
				conn, err := subscription.Library.Pool.Acquire(ctx)
				if err != nil {
					log.Panic().Msg("could not acquire database connection")
				}

				defer conn.Release()

				assets := data.ActiveAssets(ctx, conn)

				log.Debug().Int("NumAssets", len(assets)).Msg("downloading EOD quotes from Tiingo")

				// lookback 14 days in the past
				startDate := time.Now().Add(-14 * 24 * time.Hour)
				startDateStr := startDate.Format("2006-01-02")

				for _, asset := range assets {
					// reformat ticker for tiingo
					ticker := strings.ReplaceAll(asset.Ticker, "/", "-")
					url := fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices", ticker)

					respContent := make([]*tiingoEod, 0)
					resp, err := client.R().
						SetQueryParam("startDate", startDateStr).
						SetResult(&respContent).
						Get(url)
					if err != nil {
						logger.Error().Err(err).Msg("resty returned an error when querying eod prices")
						return
					}

					if resp.StatusCode() >= 300 {
						logger.Error().Int("StatusCode", resp.StatusCode()).Str("Ticker", ticker).Str("URL", resp.Request.URL).Msg("tiigno returned an invalid HTTP response")
						continue
					}

					for _, quote := range respContent {
						quoteDate, err := time.Parse(time.RFC3339Nano, quote.Date)
						if err != nil {
							logger.Error().Err(err).Str("tiingoDate", quote.Date).Msg("could not parse date from tiingo eod object")
							continue
						}

						// set tiingo date to correct time zone and market close
						quoteDate = time.Date(quoteDate.Year(), quoteDate.Month(), quoteDate.Day(), 16, 0, 0, 0, nyc)

						eodQuote := &data.Eod{
							Date:          quoteDate,
							Ticker:        asset.Ticker,
							CompositeFigi: asset.CompositeFigi,
							Open:          quote.Open,
							High:          quote.High,
							Low:           quote.Low,
							Close:         quote.Close,
							Volume:        quote.Volume,
							Dividend:      quote.Dividend,
							Split:         quote.Split,
						}

						out <- &data.Observation{
							EodQuote:         eodQuote,
							ObservationDate:  time.Now(),
							SubscriptionID:   subscription.ID,
							SubscriptionName: subscription.Name,
						}
					}
				}
			},
		},
	}
}

// Private interface

type tiingoEod struct {
	Date          string  `json:"date"`
	Ticker        string  `json:"ticker"`
	CompositeFigi string  `json:"compositeFigi"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	Volume        float64 `json:"volume"`
	Dividend      float64 `json:"divCash"`
	Split         float64 `json:"splitFactor"`
}
