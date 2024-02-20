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

import "fmt"

var DataTypes = map[string]*DataType{
	"asset-description": {
		Name: "asset-description",
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
logo_url TEXT,
sic_code INT,
cik TEXT,
cusips text[],
isins text[],
sharadar_id INT,
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
	"eod": {
		Name: "eod",
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
CHECK (LENGTH(TRIM(BOTH composite_figi)) = 12),
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
	"metric": {
		Name: "metric",
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
	"fundamentals": {
		Name: "fundamentals",
		Schema: `CREATE TABLE %[1]s (
event_date DATE,
ticker TEXT,
composite_figi TEXT,
dim DIMENSION NOT NULL,

calendar_date DATE,
report_period DATE,

cost_of_revenue NUMERIC,
total_sell_gen_admin_exp NUMERIC,
research_devel_exp NUMERIC,
opex NUMERIC,
interest_exp NUMERIC,
tax_exp NUMERIC,
net_income_discontinued_operations NUMERIC,
consolidated_income NUMERIC,
net_income_nci NUMERIC,
net_income NUMERIC,
pref_dividends NUMERIC,
eps_diluted NUMERIC,
wavg_shares_outstanding NUMERIC,
wavg_shares_outstanding_diluted NUMERIC,
capx NUMERIC,
net_business_acquisitions_divestures NUMERIC,
net_invest_acquisitions_divestures NUMERIC,
free_cash_flow_per_share NUMERIC,
net_cash_flow_from_financing NUMERIC,
total_issuance_repayment_debt NUMERIC,
total_issuance_repayment_equity NUMERIC,
common_dividends NUMERIC,
net_cash_flow_from_invest NUMERIC,
net_cash_flow_from_oper NUMERIC,
effect_of_fgn_exch_rate_on_cash NUMERIC,
net_cash_flow NUMERIC,
stock_based_comp NUMERIC,
total_depreciation_amortization NUMERIC,
total_assets NUMERIC,
total_invest NUMERIC,
curr_invest NUMERIC,
non_curr_invest NUMERIC,
deferred_revenue NUMERIC,
total_deposits NUMERIC,
net_property_plant_equip NUMERIC,
inventory_sterm NUMERIC,
tax_assets NUMERIC,
total_receivables NUMERIC,
total_payables NUMERIC,
intangibles NUMERIC,
total_liabilities NUMERIC,
retained_earnings NUMERIC,
accumulated_other_comprehensive_income NUMERIC,
curr_assets NUMERIC,
non_curr_assets NUMERIC,
curr_liabilities NUMERIC,
non_curr_liabilities NUMERIC,
tax_liabilities NUMERIC,
curr_debt NUMERIC,
non_curr_debt NUMERIC,
ebt NUMERIC,
fgn_exchange_rate NUMERIC,
equity NUMERIC,
eps NUMERIC,
total_revenue NUMERIC,
net_income_common_stock NUMERIC,
cash_equiv NUMERIC,
book_value_per_share NUMERIC,
total_debt NUMERIC,
ebit NUMERIC,
ebitda NUMERIC,
shares_outstanding NUMERIC,
dividend_per_share NUMERIC,
share_factor NUMERIC,
market_cap NUMERIC,
ev NUMERIC,
invest_capital NUMERIC,
equity_avg NUMERIC,
assets_avg NUMERIC,
invested_capital_avg NUMERIC,
tangibles NUMERIC,
roe NUMERIC,
roa NUMERIC,
free_cash_flow NUMERIC,
ret_on_invested_capital NUMERIC,
gross_profit NUMERIC,
opinc NUMERIC,
gross_margin NUMERIC,
net_margin NUMERIC,
ebitda_margin NUMERIC,
return_on_sales NUMERIC,
asset_turnover NUMERIC,
payout_ratio NUMERIC,
ev_to_ebitda NUMERIC,
ev_to_ebit NUMERIC,
pe NUMERIC,
pe_alt NUMERIC,
sales_per_share NUMERIC,
price_to_sales_alt NUMERIC,
price_to_sales NUMERIC,
pb NUMERIC,
debt_to_equity NUMERIC,
dividend_yield NUMERIC,
curr_ratio NUMERIC,
working_capital NUMERIC,
tangible_book_value_per_share NUMERIC,

source datasource,
created TIMESTAMP NOT NULL DEFAULT now(),
lastchanged TIMESTAMP NOT NULL DEFAULT now(),

PRIMARY KEY (composite_figi, dim, event_date)
);

CREATE INDEX %[1]s_ticker_idx ON %[1]s(ticker, dim);
CREATE INDEX %[1]s_event_date_idx ON %[1]s(event_date, dim);
CREATE INDEX %[1]s_calendar_date_idx ON %[1]s(calendar_date, dim);`,
		Migrations:    []string{},
		Version:       0,
		IsPartitioned: false,
	},
}

// Schema returns the schema of the data type. A getter is used to ensure that the value is immutable after construction
func (dt *DataType) ExpandedSchema(tableName string) string {
	return fmt.Sprintf(dt.Schema, tableName)
}
