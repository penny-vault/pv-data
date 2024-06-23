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
	"fmt"
	"time"

	"github.com/google/uuid"
)

type RunSummary struct {
	StartTime        time.Time
	EndTime          time.Time
	NumObservations  int
	SubscriptionID   uuid.UUID
	SubscriptionName string
}

type Observation struct {
	AssetObject   *Asset
	MarketHoliday *MarketHoliday
	EodQuote      *Eod
	Fundamental   *Fundamental

	ObservationDate  time.Time
	SubscriptionID   uuid.UUID
	SubscriptionName string
}

type DataType struct {
	Name          string
	Schema        string
	Migrations    []string
	Version       int
	IsPartitioned bool
}

const (
	AssetKey          = "asset-description"
	EODKey            = "eod"
	MetricKey         = "metric"
	FundamentalsKey   = "fundamental"
	MarketHolidaysKey = "market-holidays"
)

var DataTypes = map[string]*DataType{
	AssetKey: {
		Name: AssetKey,
		Schema: `CREATE TABLE %[1]s (
ticker TEXT,
composite_figi TEXT,
share_class_figi TEXT,
primary_exchange TEXT,
asset_type assettype,
active BOOLEAN,
name TEXT,
description TEXT,
corporate_url TEXT,
sector TEXT,
industry TEXT,
sic_code INT,
cik TEXT,
cusips text[],
isins text[],
other_identifiers JSONB,
similar_tickers TEXT[],
tags TEXT[],
listed timestamp,
delisted timestamp,
last_updated timestamp,
PRIMARY KEY (ticker, composite_figi)
);

CREATE INDEX %[1]s_active ON %[1]s(active);

ALTER TABLE %[1]s
ADD COLUMN search tsvector
	GENERATED ALWAYS AS (
		setweight(to_tsvector('pg_catalog.english', coalesce(ticker,'')), 'A') ||
		setweight(to_tsvector('pg_catalog.english', coalesce(name,'')), 'B') ||
		setweight(to_tsvector('pg_catalog.english', coalesce(composite_figi,'')), 'C')
) STORED;

CREATE INDEX %[1]s_search_idx ON %[1]s USING GIN (search);`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: false,
	},
	EODKey: {
		Name: EODKey,
		Schema: `CREATE TABLE %[1]s (
ticker         CHARACTER VARYING(10) NOT NULL,
composite_figi CHARACTER(12)         NOT NULL,
event_date     DATE                  NOT NULL,
open           NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
high           NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
low            NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
close          NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
adj_close      NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
volume         BIGINT                NOT NULL DEFAULT 0.0,
dividend       NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
split_factor   NUMERIC(9, 6)         NOT NULL DEFAULT 1.0,
PRIMARY KEY (composite_figi, event_date)
) PARTITION BY RANGE (event_date);

CREATE INDEX %[1]s_event_date_idx ON %[1]s(event_date);
CREATE INDEX %[1]s_ticker_idx ON %[1]s(ticker);

CREATE TRIGGER %[1]s_adj_close_default
BEFORE INSERT ON %[1]s
FOR EACH ROW
WHEN (NEW.adj_close IS NULL AND NEW.close IS NOT NULL)
EXECUTE PROCEDURE adj_close_default();`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: true,
	},
	MetricKey: {
		Name: MetricKey,
		Schema: `CREATE TABLE %[1]s (
ticker         CHARACTER VARYING(10) NOT NULL,
composite_figi CHARACTER(12)         NOT NULL,
event_date     DATE                  NOT NULL,
market_cap     NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
ev             NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
pe             NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
pb             NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
ps             NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
ev_ebit        NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
ev_ebitda      NUMERIC(12, 4)        NOT NULL DEFAULT 0.0,
asset_type     assettype             NOT NULL DEFAULT 0.0,
sp500          BOOLEAN               DEFAULT false,
CHECK (LENGTH(TRIM(BOTH composite_figi)) = 12),
PRIMARY KEY (composite_figi, event_date)
) PARTITION BY RANGE (event_date);

CREATE INDEX %[1]s_event_date_idx ON %[1]s(event_date);
CREATE INDEX %[1]s_ticker_idx ON %[1]s(ticker);`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: true,
	},
	FundamentalsKey: {
		Name: FundamentalsKey,
		Schema: `CREATE TABLE %[1]s (
	event_date DATE,
	ticker TEXT,
	composite_figi TEXT,
	dimension TEXT,
	date_key DATE,
	report_period DATE,
	last_updated DATE,

	accumulated_other_comprehensive_income BIGINT,
	total_assets BIGINT,
	average_assets BIGINT,
	current_assets BIGINT,
	assets_non_current BIGINT,
	asset_turnover NUMERIC,
	book_value_per_share NUMERIC,
	capital_expenditure BIGINT,
	cash_and_equivalents BIGINT,
	cost_of_revenue BIGINT,
	consolidated_income BIGINT,
	current_ratio NUMERIC,
	debt_to_equity_ratio NUMERIC,
	total_debt BIGINT,
	debt_current BIGINT,
	debt_non_current BIGINT,
	deferred_revenue BIGINT,
	depreciation_amortization_and_accretion BIGINT,
	deposits BIGINT,
	dividend_yield NUMERIC,
	dividends_per_basic_common_share NUMERIC,
	ebit BIGINT,
	ebitda BIGINT,
	ebitda_margin NUMERIC,
	ebt BIGINT,
	eps NUMERIC,
	eps_diluted NUMERIC,
	equity BIGINT,
	equity_avg BIGINT,
	enterprise_value BIGINT,
	ev_to_ebit BIGINT,
	ev_to_ebitda NUMERIC,
	free_cash_flow BIGINT,
	free_cash_flow_per_share NUMERIC,
	fx_usd NUMERIC,
	gross_profit BIGINT,
	gross_margin NUMERIC,
	intangibles BIGINT,
	interest_expense BIGINT,
	invested_capital BIGINT,
	invested_capital_average BIGINT,
	inventory BIGINT,
	investments BIGINT,
	investments_current BIGINT,
	investments_non_current BIGINT,
	total_liabilities BIGINT,
	current_liabilities BIGINT,
	liabilities_non_current BIGINT,
	market_capitalization BIGINT,
	net_cash_flow BIGINT,
	net_cash_flow_business BIGINT,
	net_cash_flow_common BIGINT,
	net_cash_flow_debt BIGINT,
	net_cash_flow_dividend BIGINT,
	net_cash_flow_from_financing BIGINT,
	net_cash_flow_from_investing BIGINT,
	net_cash_flow_invest BIGINT,
	net_cash_flow_from_operations BIGINT,
	net_cash_flow_fx BIGINT,
	net_income BIGINT,
	net_income_common_stock BIGINT,
	net_loss_income_discontinued_operations BIGINT,
	net_income_to_non_controlling_interests BIGINT,
	profit_margin NUMERIC,
	operating_expenses BIGINT,
	operating_income BIGINT,
	payables BIGINT,
	payout_ratio NUMERIC,
	pb NUMERIC,
	pe NUMERIC,
	pe1 NUMERIC,
	property_plant_and_equipment_net BIGINT,
	preferred_dividends_income_statement_impact BIGINT,
	price NUMERIC,
	ps NUMERIC,
	ps1 NUMERIC,
	receivables BIGINT,
	accumulated_retained_earnings_deficit BIGINT,
	revenues BIGINT,
	r_and_d_expenses BIGINT,
	roa NUMERIC,
	roe NUMERIC,
	roic NUMERIC,
	return_on_sales NUMERIC,
	share_based_compensation BIGINT,
	selling_general_and_administrative_expense BIGINT,
	share_factor NUMERIC,
	shares_basic BIGINT,
	weighted_average_shares BIGINT,
	weighted_average_shares_diluted BIGINT,
	sales_per_share NUMERIC,
	tangible_asset_value BIGINT,
	tax_assets BIGINT,
	income_tax_expense BIGINT,
	tax_liabilities BIGINT,
	tangible_assets_book_value_per_share NUMERIC,
	working_capital BIGINT,

	PRIMARY KEY (composite_figi, dimension, event_date)
);

CREATE INDEX %[1]s_ticker_idx ON %[1]s(ticker, dimension);
CREATE INDEX %[1]s_event_date_idx ON %[1]s(event_date, dimension);`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: false,
	},
	MarketHolidaysKey: {
		Name: MarketHolidaysKey,
		Schema: `CREATE TABLE %[1]s (
holiday TEXT NOT NULL,
event_date DATE NOT NULL,
market VARCHAR(25) NOT NULL,
early_close BOOLEAN NOT NULL DEFAULT false,
close_time TIME NOT NULL DEFAULT '16:00:00',
PRIMARY KEY (event_date, market)
);`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: false,
	},
}

// Schema returns the schema of the data type. A getter is used to ensure that the value is immutable after construction
func (dt *DataType) ExpandedSchema(tableName string) string {
	return fmt.Sprintf(dt.Schema, tableName)
}
