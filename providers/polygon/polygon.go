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
package polygon

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/goccy/go-json"
	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/providers/provider"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	ErrInvalidStatusCode = errors.New("invalid status code received")
)

type Polygon struct {
}

func (polygon *Polygon) Name() string {
	return "polygon"
}

func (polygon *Polygon) ConfigDescription() map[string]string {
	return map[string]string{
		"apiKey":    "Enter your polygon.io API key:",
		"rateLimit": "What is the maximum number of requests per minute?",
	}
}

func (polygon *Polygon) Description() string {
	return `The Polygon.io Stocks API provides REST endpoints that let you query the latest market data from all US stock exchanges. You can also find data on company financials, stock market holidays, corporate actions, and more.`
}

func (polygon *Polygon) Datasets() map[string]provider.Dataset {
	return map[string]provider.Dataset{
		"Stock Tickers": {
			Name:        "Stock Tickers",
			Description: "Details about tradeable stocks and ETFs.",
			DataTypes:   []*provider.DataType{provider.DataTypes["asset-description"]},
			DateRange: func() (time.Time, time.Time) {
				return time.Date(1998, 1, 1, 0, 0, 0, 0, time.UTC), time.Now().UTC()
			},
			Fetch: func(config map[string]string, tables []string, out chan<- interface{}, logger zerolog.Logger, progress chan<- int) (int, error) {
				// get a list of all active assets
				assets := make([]*data.Asset, 0, 6000)

				var (
					rateDuration time.Duration
				)

				rateLimit, err := strconv.Atoi(config["rateLimit"])
				if err != nil {
					logger.Error().Err(err).Str("configRateLimit", config["rateLimit"]).Msg("could not convert rateLimit configuration parameter to an integer")
					return 0, err
				}

				if rateLimit > 0 {
					rateDuration = time.Duration(60/rateLimit+1) * time.Second
				}

				client := resty.New()

				for _, assetType := range []string{"CS", "ADRC", "ETF"} {
					if tmpAssets, err := getPolygonTickers(config, assetType, client, rateLimit, rateDuration, logger); err != nil {
						logger.Error().Err(err).Str("AssetType", assetType).Msg("error getting ticker information")
						return 0, err
					} else {
						assets = append(assets, tmpAssets...)
					}

					if rateLimit > 0 {
						time.Sleep(rateDuration)
					}
				}

				log.Info().Int("Count", len(assets)).Msg("got assets from polygon")

				return len(assets), nil
			},
		},
	}
}

// Private interface

type polygonResponse struct {
	Results   *json.RawMessage `json:"results"`
	Status    string           `json:"status"`
	RequestID string           `json:"request_id"`
	Count     int              `json:"count"`
	Next      string           `json:"next_url"`
}

type polygonStock struct {
	Ticker          string `json:"ticker"`
	Name            string `json:"name"`
	CompositeFIGI   string `json:"composite_figi"`
	ShareClassFIGI  string `json:"share_class_figi"`
	Locale          string `json:"locale"`
	PrimaryExchange string `json:"primary_exchange"`
	Type            string `json:"type"`
	Active          bool   `json:"active"`
	CurrencyName    string `json:"currency_name"`
	CIK             string `json:"cik"`
	LastUpdated     string `json:"last_updated_utc"`
}

func getPolygonTickers(config map[string]string, assetType string, client *resty.Client, rateLimit int, rateDuration time.Duration, logger zerolog.Logger) ([]*data.Asset, error) {
	var respContent polygonResponse
	assets := make([]*data.Asset, 0, 6000)

	// first we query the reference endpoint which is faster than the details endpoint
	// this gives us a list of all assets we should query details for
	// NOTE: results are limited to stocks
	maxQueries := 1000

	logger.Info().Msg("getPolygonTickers")

	resp, err := client.R().
		SetQueryParam("apiKey", config["apiKey"]).
		SetQueryParam("market", "stocks").
		SetQueryParam("active", "true").
		SetQueryParam("type", assetType).
		SetQueryParam("limit", "1000").
		SetResult(&respContent).
		Get("https://api.polygon.io/v3/reference/tickers")
	if err != nil {
		logger.Error().Err(err).Msg("resty returned an error when querying reference/tickers")
		return assets, err
	}

	for ii := 0; ii < maxQueries; ii++ {
		if resp.StatusCode() >= 300 {
			logger.Error().Int("StatusCode", resp.StatusCode()).Str("ResponseBody", string(resp.Body())).
				Str("URL", "https://api.polygon.io/v3/reference/tickers").
				Msg("received an invalid status code when querying polygon reference/tickers endpoint")
			return assets, fmt.Errorf("%w (%d): %s", ErrInvalidStatusCode, resp.StatusCode(), string(resp.Body()))
		}

		// de-serealize stock content
		polygonTickers := make([]*polygonStock, 0, 100)
		json.Unmarshal(*respContent.Results, &polygonTickers)

		logger.Info().Int("ReceivedNAssets", len(polygonTickers)).Str("AssetType", assetType).Msg("got tickers")

		for _, ticker := range polygonTickers {
			lastUpdated, err := time.Parse(time.RFC3339, ticker.LastUpdated)
			if err != nil {
				logger.Error().Err(err).Str("Ticker", ticker.Ticker).Msg("could not parse last updated string for tickers")
			}

			polygonAsset := &data.Asset{
				Ticker:          ticker.Ticker,
				Name:            ticker.Name,
				CompositeFigi:   ticker.CompositeFIGI,
				ShareClassFigi:  ticker.ShareClassFIGI,
				PrimaryExchange: ticker.PrimaryExchange,
				AssetType:       data.AssetType(ticker.Type),
				LastUpdated:     lastUpdated.Unix(),
				CIK:             ticker.CIK,
			}

			assets = append(assets, polygonAsset)
		}

		// check if all results have been returned
		if respContent.Next == "" {
			break
		}

		// get next result
		next := respContent.Next
		respContent.Next = ""

		// delay to match rate limit
		if rateLimit > 0 {
			time.Sleep(rateDuration)
		}

		logger.Info().Str("Next", next).Str("AssetType", assetType).Int("ii", ii).Msg("making next query")

		resp, err = client.R().
			SetQueryParam("apiKey", config["apiKey"]).
			SetResult(&respContent).
			Get(next)
		if err != nil {
			logger.Error().Err(err).Msg("resty returned an error when querying reference/tickers")
			return assets, err
		}
	}

	return assets, nil
}
