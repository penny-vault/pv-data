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
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type AssetType string

const (
	CommonStock  AssetType = "CS"
	ETF          AssetType = "ETF"
	ETN          AssetType = "ETN"
	CEF          AssetType = "CEF"
	MutualFund   AssetType = "MF"
	ADRC         AssetType = "ADRC"
	FRED         AssetType = "FRED"
	UnknownAsset AssetType = "Unknown"
)

type Asset struct {
	Ticker               string    `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name                 string    `json:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Description          string    `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PrimaryExchange      string    `json:"primary_exchange" toml:"primary_exchange" parquet:"name=primary_exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType            AssetType `json:"asset_type" toml:"asset_type" parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi        string    `json:"composite_figi" toml:"composite_figi" parquet:"name=composite_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShareClassFigi       string    `json:"share_class_figi" toml:"share_class_figi" parquet:"name=share_class_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Active               bool      `json:"active" toml:"active" parquet:"name=active, type=BOOLEAN"`
	CUSIP                []string  `json:"cusip" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ISIN                 []string  `json:"isin" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CIK                  string    `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SIC                  int       `json:"sic"`
	ListingDate          string    `json:"listing_date" toml:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DelistingDate        string    `json:"delisting_date" toml:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Industry             string    `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string    `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Icon                 []byte    `parquet:"name=icon, type=BYTE_ARRAY"`
	IconMimeType         string    `parquet:"name=icon_mime_type, tyle=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Logo                 []byte    `parquet:"name=logo, type=BYTE_ARRAY"`
	LogoMimeType         string    `parquet:"name=logo_mime_type, tyle=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorporateUrl         string    `json:"corporate_url" toml:"corporate_url" parquet:"name=corporate_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HeadquartersLocation string    `json:"headquarters_location" toml:"headquarters_location" parquet:"name=headquarters_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OtherIdentifiers     map[string]string
	Tags                 []string
	SimilarTickers       []string  `json:"similar_tickers" toml:"similar_tickers" parquet:"name=similar_tickers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	LastUpdated          time.Time `json:"last_updated" parquet:"name=last_updated, type=INT64"`
}

func (asset *Asset) ID() string {
	return fmt.Sprintf("%s:%s", asset.Ticker, asset.CompositeFigi)
}

func (asset *Asset) Save(ctx context.Context, tbl string, dbConn *pgxpool.Conn) error {
	tx, err := dbConn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Commit(ctx)

	iconUrl := ""
	logoUrl := ""

	sql := fmt.Sprintf(`INSERT INTO %[1]s (
		"ticker",
		"composite_figi",
		"share_class_figi",
		"primary_exchange",
		"asset_type",
		"active",
		"name",
		"description",
		"corporate_url",
		"sector",
		"industry",
		"icon_url",
		"logo_url",
		"sic_code",
		"cik",
		"cusips",
		"isins",
		"other_identifiers",
		"similar_tickers",
		"tags",
		"listed",
		"delisted",
		"last_updated"
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
		$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
	) ON CONFLICT ON CONSTRAINT %[1]s_pkey DO UPDATE SET
		primary_exchange = EXCLUDED.primary_exchange,
		active = EXCLUDED.active,
		name = EXCLUDED.name,
		description = EXCLUDED.description,
		corporate_url = EXCLUDED.corporate_url,
		sector = EXCLUDED.sector,
		industry = EXCLUDED.industry,
		icon_url = EXCLUDED.icon_url,
		logo_url = EXCLUDED.logo_url,
		sic_code = EXCLUDED.sic_code,
		cik = EXCLUDED.cik,
		cusips = EXCLUDED.cusips,
		isins = EXCLUDED.isins,
		other_identifiers = EXCLUDED.other_identifiers,
		similar_tickers = EXCLUDED.similar_tickers,
		tags = EXCLUDED.tags,
		listed = EXCLUDED.listed,
		delisted = EXCLUDED.delisted,
		last_updated = EXCLUDED.last_updated`, tbl)
	_, err = tx.Exec(ctx, sql, asset.Ticker, asset.CompositeFigi, asset.ShareClassFigi,
		asset.PrimaryExchange, asset.AssetType, asset.Active, asset.Name, asset.Description,
		asset.CorporateUrl, asset.Sector, asset.Industry, iconUrl, logoUrl, asset.SIC, asset.CIK,
		asset.CUSIP, asset.ISIN, asset.OtherIdentifiers, asset.SimilarTickers, asset.Tags,
		/*asset.ListingDate*/ nil,
		/*asset.DelistingDate*/ nil,
		asset.LastUpdated)

	if err != nil {
		log.Error().Err(err).Str("SQL", sql).Msg("save asset to DB failed")
		return err
	}

	return nil
}
