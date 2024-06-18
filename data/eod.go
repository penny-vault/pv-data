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

type Eod struct {
	Date          time.Time `json:"date"`
	Ticker        string    `json:"ticker"`
	CompositeFigi string    `json:"compositeFigi"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Close         float64   `json:"close"`
	Volume        float64   `json:"volume"`
	Dividend      float64   `json:"divCash"`
	Split         float64   `json:"splitFactor"`
}

func (eod *Eod) SaveDB(ctx context.Context, tbl string, dbConn *pgxpool.Conn) error {
	tx, err := dbConn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Error().Err(err).Msg("error committing eod transaction to database")
		}
	}()

	sql := fmt.Sprintf(`INSERT INTO %[1]s (
		"ticker",
		"composite_figi",
		"event_date",
		"open",
		"high",
		"low",
		"close",
		"volume",
		"dividend",
		"split_factor"
	) VALUES (
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7,
		$8,
		$9,
		$10
	) ON CONFLICT ON CONSTRAINT %[1]s_pkey
	DO UPDATE SET
		open = EXCLUDED.open,
		high = EXCLUDED.high,
		low = EXCLUDED.low,
		close = EXCLUDED.close,
		volume = EXCLUDED.volume,
		dividend = EXCLUDED.dividend,
		split_factor = EXCLUDED.split_factor;`, tbl)

	_, err = tx.Exec(ctx, sql, eod.Ticker, eod.CompositeFigi, eod.Date,
		eod.Open, eod.High, eod.Low, eod.Close, eod.Volume, eod.Dividend,
		eod.Split)
	if err != nil {
		log.Error().Err(err).Str("SQL", sql).Msg("error saving EOD quote to database")
	}

	return nil
}
