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
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/library"
	"github.com/rs/zerolog"
)

type Fred struct{}

func (fred *Fred) Name() string {
	return "FRED"
}

func (fred *Fred) ConfigDescription() map[string]string {
	return map[string]string{
		"seriesIds": "Enter all series to retrieve from FRED (e.g. UNRATE, DTB3):",
		"apiKey":    "What is your FRED api key?",
	}
}

func (fred *Fred) Description() string {
	return `The Financial Reserve Economic Data (FRED) provides access over 800,000 economic indicators`
}

func (fred *Fred) Datasets() map[string]Dataset {
	return map[string]Dataset{
		"Economic Indicators": {
			Name:        "Economic Indicators",
			Description: "Download economic indicators.",
			DataTypes:   []*data.DataType{data.DataTypes[data.EconomicIndicatorKey]},
			DateRange: func() (time.Time, time.Time) {
				return time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), time.Now().UTC()
			},
			Fetch: downloadAllFredIndicators,
		},
	}
}

func downloadAllFredIndicators(ctx context.Context, subscription *library.Subscription, out chan<- *data.Observation, exitNotification chan<- data.RunSummary) {
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

	seriesIds := strings.Split(subscription.Config["seriesIds"], ",")
	for _, seriesId := range seriesIds {
		seriesId = strings.TrimSpace(seriesId)
		downloadIndicator(ctx, subscription, out, seriesId)
	}
}

func downloadIndicator(ctx context.Context, subscription *library.Subscription, out chan<- *data.Observation, seriesId string) {
	logger := zerolog.Ctx(ctx)

	// get nyc timezone
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		logger.Panic().Err(err).Msg("could not load timezone")
		return
	}

	var resp fredResponse

	client := resty.New().SetQueryParam("api_key", subscription.Config["apiKey"])
	req, err := client.R().
		SetQueryParam("file_type", "json").
		SetQueryParam("series_id", seriesId).
		SetQueryParam("sort_order", "desc").
		SetResult(&resp).Get("https://api.stlouisfed.org/fred/series/observations")

	if err != nil {
		logger.Error().Err(err).Msg("downloading economic indicators failed")
		return
	}

	if req.StatusCode() >= 300 {
		logger.Error().Int("StatusCode", req.StatusCode()).Msg("downloading economic indicators returned error status code")
		return
	}

	for _, obs := range resp.Observations {
		indicator := &data.EconomicIndicator{
			Series: seriesId,
		}

		if eventDate, err := time.Parse("2006-01-02", obs.Date); err == nil {
			indicator.EventDate = time.Date(eventDate.Year(), eventDate.Month(), eventDate.Day(), 0, 0, 0, 0, nyc)
		} else {
			logger.Error().Err(err).Str("DateStr", obs.Date).Msg("parsing observation date failed")
			continue
		}

		if obs.Value == "." {
			// no observation
			continue
		}

		if val, err := strconv.ParseFloat(obs.Value, 64); err == nil {
			indicator.Value = val
		} else {
			logger.Error().Err(err).Str("ValueStr", obs.Value).Msg("parsing observation value failed")
			continue
		}

		out <- &data.Observation{
			EconomicIndicator: indicator,
			ObservationDate:   time.Now(),
			SubscriptionID:    subscription.ID,
			SubscriptionName:  subscription.Name,
		}
	}
}

type fredResponse struct {
	RealTimeStart    string            `json:"realtime_start"`
	RealTimeEnd      string            `json:"realtime_end"`
	ObservationStart string            `json:"observation_start"`
	ObservationEnd   string            `json:"observation_end"`
	Units            string            `json:"units"`
	OutputType       int               `json:"output_type"`
	FileType         string            `json:"file_type"`
	OrderBy          string            `json:"order_by"`
	SortOrder        string            `json:"sort_order"`
	Count            int               `json:"count"`
	Offset           int               `json:"offset"`
	Limit            int               `json:"limit"`
	Observations     []fredObservation `json:"observations"`
}

type fredObservation struct {
	RealTimeStart string `json:"realtime_start"`
	RealTimeEnd   string `json:"realtime_end"`
	Date          string `json:"date"`
	Value         string `json:"value"`
}
