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
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
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
	SYNTH        AssetType = "SYNTH"
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
	CUSIP                []string  `json:"cusips" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" db:"cusips"`
	ISIN                 []string  `json:"isins" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" db:"isins"`
	CIK                  string    `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SIC                  int       `json:"sic" db:"sic_code"`
	ListingDate          string    `json:"listing_date" toml:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" db:"listed"`
	DelistingDate        string    `json:"delisting_date" toml:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" db:"delisted"`
	Industry             string    `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string    `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Icon                 []byte    `parquet:"name=icon, type=BYTE_ARRAY"`
	IconMimeType         string    `parquet:"name=icon_mime_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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

func (asset *Asset) SaveFiles(ctx context.Context, filer Filer) error {
	type File struct {
		Name     string
		MimeType string
		Data     []byte
	}

	files := []File{{
		Name:     asset.CompositeFigi + "-icon",
		MimeType: asset.IconMimeType,
		Data:     asset.Icon,
	}, {
		Name:     asset.CompositeFigi + "-logo",
		MimeType: asset.LogoMimeType,
		Data:     asset.Logo,
	}}

	for _, ff := range files {
		switch ff.MimeType {
		case "image/jpeg":
			filer.CreateFile(ff.Name+".jpg", ff.Data)
		case "image/png":
			filer.CreateFile(ff.Name+".png", ff.Data)
		case "image/svg+xml":
			fallthrough
		case "image/svg":
			filer.CreateFile(ff.Name+".svg", ff.Data)
		case "":
			// do nothing
		default:
			log.Error().Str("MimeType", ff.MimeType).Msg("unknown image mimetype")
			return errors.New("unknown mimetype")
		}
	}

	return nil
}

func (asset *Asset) SaveDB(ctx context.Context, tbl string, dbConn *pgxpool.Conn) error {
	tx, err := dbConn.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Commit(ctx)

	listingDate := &asset.ListingDate
	delistingDate := &asset.DelistingDate

	if asset.ListingDate == "" {
		listingDate = nil
	}

	if asset.DelistingDate == "" {
		delistingDate = nil
	}

	log.Debug().Object("Asset", asset).Msg("Saving asset to database")

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
		$13, $14, $15, $16, $17, $18, $19, $20, $21
	) ON CONFLICT ON CONSTRAINT %[1]s_pkey DO UPDATE SET
		primary_exchange = EXCLUDED.primary_exchange,
		active = EXCLUDED.active,
		name = EXCLUDED.name,
		description = EXCLUDED.description,
		corporate_url = EXCLUDED.corporate_url,
		sector = EXCLUDED.sector,
		industry = EXCLUDED.industry,
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
		asset.CorporateUrl, asset.Sector, asset.Industry, asset.SIC, asset.CIK,
		asset.CUSIP, asset.ISIN, asset.OtherIdentifiers, asset.SimilarTickers, asset.Tags,
		listingDate, delistingDate, asset.LastUpdated)

	if err != nil {
		log.Error().Err(err).Str("SQL", sql).Msg("save asset to DB failed")
		return err
	}

	return nil
}

func (asset *Asset) MarshalZerologObject(e *zerolog.Event) {
	e.Str("Ticker", asset.Ticker)
	e.Str("Name", asset.Name)
	e.Str("Description", asset.Description)
	e.Str("PrimaryExchange", asset.PrimaryExchange)
	e.Str("AssetType", string(asset.AssetType))
	e.Str("CompositeFigi", asset.CompositeFigi)
	e.Str("ShareClassFigi", asset.ShareClassFigi)
	e.Bool("Active", asset.Active)
	e.Strs("CUSIP", asset.CUSIP)
	e.Strs("ISIN", asset.ISIN)
	e.Str("CIK", asset.CIK)
	e.Int("SIC", asset.SIC)
	e.Str("ListingDate", asset.ListingDate)
	e.Str("DelistingDate", asset.DelistingDate)
	e.Str("Industry", asset.Industry)
	e.Str("Sector", asset.Sector)
	e.Str("CorporateURL", asset.CorporateUrl)
	e.Str("HeadquartersLocation", asset.HeadquartersLocation)

	for key, val := range asset.OtherIdentifiers {
		e.Str(key, val)
	}

	e.Strs("Tags", asset.Tags)
	e.Strs("SimilarTickers", asset.SimilarTickers)
	e.Time("LastUpdated", asset.LastUpdated)
}
