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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/go-resty/resty/v2"
	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/library"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
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
		"filer":     "Where should logos and icons be saved? (e.g. file:///path/)",
	}
}

func (polygon *Polygon) Description() string {
	return `The Polygon.io Stocks API provides REST endpoints that let you query the latest market data from all US stock exchanges. You can also find data on company financials, stock market holidays, corporate actions, and more.`
}

func (polygon *Polygon) Datasets() map[string]Dataset {
	return map[string]Dataset{
		"Stock Tickers": {
			Name:        "Stock Tickers",
			Description: "Details about tradeable stocks and ETFs.",
			DataTypes:   []*data.DataType{data.DataTypes["asset-description"]},
			DateRange: func() (time.Time, time.Time) {
				return time.Date(1998, 1, 1, 0, 0, 0, 0, time.UTC), time.Now().UTC()
			},
			Fetch: func(ctx context.Context, subscription *library.Subscription, out chan<- *data.Observation, exitNotification chan<- data.RunSummary) {
				logger := zerolog.Ctx(ctx)

				runSummary := data.RunSummary{
					StartTime:        time.Now(),
					SubscriptionID:   subscription.ID,
					SubscriptionName: subscription.Name,
				}

				// get a list of all active assets
				assets := make([]*data.Asset, 0, 6000)
				var assetDetail []*data.Asset

				defer func() {
					runSummary.EndTime = time.Now()
					runSummary.NumObservations = len(assetDetail)
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

				api := &polygonAssetFetcher{
					subscription: subscription,
					client:       resty.New().SetQueryParam("apiKey", subscription.Config["apiKey"]),
					limiter:      rate.NewLimiter(rate.Limit(float64(rateLimit)/float64(60)), rateLimit),
					publishChan:  out,
				}

				//for _, assetType := range []string{"CS", "ADRC", "ETF"} {
				for _, assetType := range []string{"ADRC"} {
					if tmpAssets, err := api.assets(ctx, assetType); err != nil {
						logger.Error().Err(err).Str("AssetType", assetType).Msg("error getting ticker information")
						return
					} else {
						assets = append(assets, tmpAssets...)
					}
				}

				assets = assets[:1]

				logger.Info().Int("Count", len(assets)).Msg("got assets from polygon")

				assetDetail, err = api.filterAssetsByLastUpdated(ctx, assets)
				if err != nil {
					// logged by caller
					return
				}

				// fetch asset details
				logger.Info().Int("NumActive", len(assetDetail)).Msg("querying polygon for asset details")

				assetDetails := api.assetDetails(ctx, assets)

				// get delisting date for inactive assets
				delistedAssets, err := api.delistedAssets(ctx, assets)
				if err != nil {
					// logged by caller
					return
				}

				logger.Info().Int("assetDetailsCnt", len(assetDetails)).Int("delistedAssetsCnt", len(delistedAssets)).Msg("publishing assets")

				api.publishAssets(assetDetails)
				api.publishAssets(delistedAssets)
			},
		},
	}
}

// Private interfaces

type polygonAssetFetcher struct {
	subscription *library.Subscription
	client       *resty.Client
	limiter      *rate.Limiter
	publishChan  chan<- *data.Observation
}

type polygonResponse struct {
	Results   *json.RawMessage `json:"results"`
	Status    string           `json:"status"`
	RequestID string           `json:"request_id"`
	Count     int              `json:"count"`
	Next      string           `json:"next_url"`
}

type polygonAddress struct {
	Address1   string `json:"address1"`
	City       string `json:"city"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
}

type polygonBranding struct {
	LogoURL string `json:"logo_url"`
	IconURL string `json:"icon_url"`
}

type polygonStock struct {
	Ticker          string          `json:"ticker"`
	Name            string          `json:"name"`
	Description     string          `json:"description"`
	CompositeFIGI   string          `json:"composite_figi"`
	ShareClassFIGI  string          `json:"share_class_figi"`
	PrimaryExchange string          `json:"primary_exchange"`
	Type            string          `json:"type"`
	Active          bool            `json:"active"`
	CIK             string          `json:"cik"`
	SIC             string          `json:"sic_code"`
	CorporateURL    string          `json:"homepage_url"`
	ListDate        string          `json:"list_date"`
	DelistDate      string          `json:"delisted_utc"`
	Branding        polygonBranding `json:"branding"`
	Address         polygonAddress  `json:"address"`
	LastUpdated     string          `json:"last_updated_utc"`
}

func (api *polygonAssetFetcher) publishAssets(assets []*data.Asset) {
	for _, asset := range assets {
		api.publishChan <- &data.Observation{
			AssetObject:      asset,
			ObservationDate:  time.Now(),
			SubscriptionID:   api.subscription.ID,
			SubscriptionName: api.subscription.Name,
		}
	}
}

func (api *polygonAssetFetcher) assets(ctx context.Context, assetType string) ([]*data.Asset, error) {
	logger := zerolog.Ctx(ctx)

	var respContent polygonResponse
	assets := make([]*data.Asset, 0, 6000)

	// first we query the reference endpoint which is faster than the details endpoint
	// this gives us a list of all assets we should query details for
	// NOTE: results are limited to stocks
	maxQueries := 1000

	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		logger.Error().Err(err).Msg("could not load timezone")
		return []*data.Asset{}, err
	}

	api.limiter.Wait(ctx)
	resp, err := api.client.R().
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
		polygonTickers := make([]*polygonStock, 0, 1000)
		json.Unmarshal(*respContent.Results, &polygonTickers)

		logger.Info().Int("ReceivedNAssets", len(polygonTickers)).Str("AssetType", assetType).Msg("got tickers")

		for _, ticker := range polygonTickers {
			lastUpdated, err := time.Parse(time.RFC3339, ticker.LastUpdated)
			if err != nil {
				logger.Error().Err(err).Str("Ticker", ticker.Ticker).Msg("could not parse last updated string for tickers")
				continue
			}

			lastUpdated = lastUpdated.In(nyc)

			polygonAsset := &data.Asset{
				Ticker:          ticker.Ticker,
				Name:            ticker.Name,
				CompositeFigi:   ticker.CompositeFIGI,
				ShareClassFigi:  ticker.ShareClassFIGI,
				PrimaryExchange: ticker.PrimaryExchange,
				AssetType:       data.AssetType(ticker.Type),
				LastUpdated:     lastUpdated,
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

		logger.Info().Str("Next", next).Str("AssetType", assetType).Int("ii", ii).Msg("making next query")
		api.limiter.Wait(ctx)
		resp, err = api.client.R().
			SetResult(&respContent).
			Get(next)
		if err != nil {
			logger.Error().Err(err).Msg("resty returned an error when querying reference/tickers")
			return assets, err
		}
	}

	return assets, nil
}

func (api *polygonAssetFetcher) filterAssetsByLastUpdated(ctx context.Context, assets []*data.Asset) ([]*data.Asset, error) {
	logger := zerolog.Ctx(ctx)

	assetDetail := make([]*data.Asset, 0, len(assets))

	dbConn, err := api.subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("error getting database connection")
		return nil, err
	}
	defer dbConn.Release()

	// for each asset determine if details need to be queried
	for _, asset := range assets {
		var lastUpdated time.Time
		sql := fmt.Sprintf("SELECT COALESCE(last_updated, '0001-01-01'::timestamp) as last_updated FROM %s WHERE composite_figi=$1 AND ticker=$2 LIMIT 1", api.subscription.DataTablesMap[data.AssetKey])
		err := dbConn.QueryRow(
			ctx,
			sql,
			asset.CompositeFigi, asset.Ticker,
		).Scan(&lastUpdated)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				assetDetail = append(assetDetail, asset)
				continue
			}

			logger.Error().Err(err).Str("SQL", sql).Str("CompositeFIGI", asset.CompositeFigi).Str("Ticker", asset.Ticker).Msg("error when querying database for asset")
			return nil, err
		}

		if lastUpdated.After(asset.LastUpdated) {
			assetDetail = append(assetDetail, asset)
		}
	}

	return assetDetail, nil
}

func (api *polygonAssetFetcher) delistedAssets(ctx context.Context, assets []*data.Asset) ([]*data.Asset, error) {
	logger := zerolog.Ctx(ctx)

	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		logger.Error().Err(err).Msg("could not load timezone")
		return nil, err
	}

	dbConn, err := api.subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("error getting database connection")
		return nil, err
	}
	defer dbConn.Release()

	assetMap := make(map[string]*data.Asset, len(assets))
	for _, asset := range assets {
		assetMap[asset.ID()] = asset
	}

	// get a list of assets that should be marked as inactive
	inactive := make([]*data.Asset, 0, 50)
	rows, err := dbConn.Query(ctx, fmt.Sprintf(`SELECT
		ticker,
		composite_figi,
		share_class_figi,
		primary_exchange,
		asset_type,
		active,
		name,
		description,
		corporate_url,
		sector,
		industry,
		icon_url,
		logo_url,
		sic_code,
		cik,
		cusips,
		isins,
		other_identifiers,
		similar_tickers,
		tags,
		listed,
		delisted,
		last_updated
	FROM %s WHERE active=true`, api.subscription.DataTablesMap[data.AssetKey]))
	if err != nil {
		logger.Error().Err(err).Msg("error when querying database for active tickers")
		return nil, err
	}

	var dbActiveAssets []*data.Asset
	err = pgxscan.ScanAll(&dbActiveAssets, rows)
	if err != nil {
		logger.Error().Err(err).Msg("error when scanning values into dbActiveAssets")
	}

	for _, asset := range dbActiveAssets {
		if _, ok := assetMap[asset.ID()]; !ok {
			inactive = append(inactive, asset)
		}
	}

	if len(inactive) == 0 {
		// no inactive assets to consider
		return []*data.Asset{}, nil
	}

	inactiveMap := make(map[string]*data.Asset, len(inactive))
	for _, asset := range inactive {
		inactiveMap[asset.ID()] = asset
	}

	var respContent polygonResponse
	api.limiter.Wait(ctx)
	resp, err := api.client.R().
		SetQueryParam("active", "false").
		SetQueryParam("sort", "last_updated_utc").
		SetQueryParam("order", "desc").
		SetQueryParam("limit", "1000").
		SetResult(&respContent).
		Get("https://api.polygon.io/v3/reference/tickers")
	if err != nil {
		logger.Error().Err(err).Msg("error when retrieving inactive assets")
	}

	maxQueries := 30
	updatedCount := 0

	for ii := 0; ii < maxQueries; ii++ {
		if resp.StatusCode() >= 300 {
			logger.Error().Int("StatusCode", resp.StatusCode()).Str("ResponseBody", string(resp.Body())).
				Str("URL", "https://api.polygon.io/v3/reference/tickers").
				Msg("received an invalid status code when querying polygon reference/tickers endpoint")
			return assets, fmt.Errorf("%w (%d): %s", ErrInvalidStatusCode, resp.StatusCode(), string(resp.Body()))
		}

		// de-serealize stock content
		polygonAssets := make([]*polygonStock, 0, 1000)
		json.Unmarshal(*respContent.Results, &polygonAssets)

		logger.Info().Int("ReceivedNAssets", len(polygonAssets)).Msg("got inactive tickers")

		for _, polygonAsset := range polygonAssets {
			fmt.Printf("%+v\n", polygonAsset)
			lastUpdated, err := time.Parse(time.RFC3339, polygonAsset.LastUpdated)
			if err != nil {
				logger.Error().Err(err).Str("Ticker", polygonAsset.Ticker).Msg("could not parse last updated string for tickers")
			}

			lastUpdated = lastUpdated.In(nyc)

			asset := data.Asset{
				Ticker:        polygonAsset.Ticker,
				CompositeFigi: polygonAsset.CompositeFIGI,
			}

			if inactiveAsset, ok := inactiveMap[asset.ID()]; ok {
				inactiveAsset.DelistingDate = strings.Split(polygonAsset.DelistDate, "T")[0]
				inactiveAsset.LastUpdated = lastUpdated
				updatedCount++
			}
		}

		// check if all results have been returned
		if respContent.Next == "" || updatedCount >= len(inactive) {
			break
		}

		// get next result
		next := respContent.Next
		respContent.Next = ""

		logger.Info().Str("Next", next).Int("ii", ii).Msg("making next query")
		api.limiter.Wait(ctx)
		resp, err = api.client.R().
			SetResult(&respContent).
			Get(next)
		if err != nil {
			logger.Error().Err(err).Msg("resty returned an error when querying reference/tickers")
			return inactive, err
		}
	}

	return inactive, nil
}

func (api *polygonAssetFetcher) assetDetails(ctx context.Context, assets []*data.Asset) []*data.Asset {
	logger := zerolog.Ctx(ctx)

	assetDetails := make([]*data.Asset, 0, len(assets))

	sometimes := rate.Sometimes{Interval: 60 * time.Second}
	started := time.Now()

	for idx, asset := range assets {
		fullAsset, err := api.assetDetail(ctx, asset)
		if err != nil {
			logger.Error().Err(err).Msg("received an error when querying polygon details")
			continue
		}

		assetDetails = append(assetDetails, fullAsset)

		sometimes.Do(func() {
			secondsPerItem := time.Since(started) / (time.Duration(idx+1) * time.Second)
			timeLeft := secondsPerItem * time.Duration(len(assets)-idx)
			logger.Info().Int("Completed", idx+1).Str("ETA", timeLeft.String()).Msg("asset detail progress")
		})
	}

	return assetDetails
}

func (api *polygonAssetFetcher) assetDetail(ctx context.Context, asset *data.Asset) (*data.Asset, error) {
	var respContent polygonResponse

	logger := zerolog.Ctx(ctx)
	detailsURL := fmt.Sprintf("https://api.polygon.io/v3/reference/tickers/%s", asset.Ticker)

	api.limiter.Wait(ctx)
	resp, err := api.client.R().
		SetResult(&respContent).
		Get(detailsURL)
	if err != nil {
		logger.Error().Err(err).Msg("resty returned an error when querying v3/reference/tickers details")
		return nil, err
	}

	if resp.StatusCode() >= 300 {
		logger.Error().Int("StatusCode", resp.StatusCode()).Str("ResponseBody", string(resp.Body())).
			Str("URL", detailsURL).
			Msg("received an invalid status code when querying polygon reference/tickers details endpoint")
		return nil, fmt.Errorf("%w (%d): %s", ErrInvalidStatusCode, resp.StatusCode(), string(resp.Body()))
	}

	// de-serealize stock content
	var polygonAsset polygonStock
	err = json.Unmarshal(*respContent.Results, &polygonAsset)
	if err != nil {
		logger.Error().Err(err).Msg("error when unmarshalling json from details response")
		return nil, err
	}

	location := ""
	if polygonAsset.Address.City != "" {
		location = fmt.Sprintf("%s, %s", polygonAsset.Address.City, polygonAsset.Address.State)
	}

	sicCode, err := strconv.Atoi(polygonAsset.SIC)
	if err != nil {
		sicCode = 0
	}

	// fetch icon and logo
	var icon []byte
	if polygonAsset.Branding.IconURL != "" {
		api.limiter.Wait(ctx)
		resp, err := api.client.R().Get(polygonAsset.Branding.IconURL)
		if err != nil {
			logger.Error().Err(err).Msg("error when fetching asset icon")
			return nil, err
		}

		icon = resp.Body()
	}

	var logo []byte
	if polygonAsset.Branding.LogoURL != "" {
		api.limiter.Wait(ctx)
		resp, err := api.client.R().Get(polygonAsset.Branding.LogoURL)
		if err != nil {
			logger.Error().Err(err).Msg("error when fetching asset logo")
			return nil, err
		}

		logo = resp.Body()
	}

	// build Asset object
	assetDetail := &data.Asset{
		Ticker:               polygonAsset.Ticker,
		CompositeFigi:        polygonAsset.CompositeFIGI,
		ShareClassFigi:       polygonAsset.ShareClassFIGI,
		Name:                 polygonAsset.Name,
		Description:          polygonAsset.Description,
		Active:               polygonAsset.Active,
		PrimaryExchange:      polygonAsset.PrimaryExchange,
		AssetType:            data.AssetType(polygonAsset.Type),
		HeadquartersLocation: location,
		CIK:                  polygonAsset.CIK,
		SIC:                  sicCode,
		CorporateUrl:         polygonAsset.CorporateURL,
		Icon:                 icon,
		Logo:                 logo,
		LastUpdated:          asset.LastUpdated,
	}

	return assetDetail, nil
}
