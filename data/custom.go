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

type Custom struct {
	Ticker        string
	CompositeFigi string
	EventDate     time.Time
	Key           string
	Value         interface{}
}

func (custom *Custom) SaveDB(ctx context.Context, tbl string, dbConn *pgxpool.Conn) error {
	if custom.CompositeFigi == "" {
		return nil
	}

	tx, err := dbConn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Error().Err(err).Msg("error committing asset transaction to database")
		}
	}()

	sql := fmt.Sprintf(`INSERT INTO %[1]s (
		"ticker",
		"composite_figi",
		"event_date",
		"key",
		"value"
	) VALUES (
		$1, $2, $3, $4, $5
	) ON CONFLICT ON CONSTRAINT %[1]s_pkey DO UPDATE SET
		value = EXCLUDED.value`, tbl)

	_, err = tx.Exec(ctx, sql, custom.Ticker, custom.CompositeFigi, custom.EventDate, custom.Key, custom.Value)

	if err != nil {
		log.Error().Err(err).Str("SQL", sql).Msg("save custom data to DB failed")
		if err2 := tx.Rollback(ctx); err2 != nil {
			log.Error().Err(err).Msg("error rollingback tx")
		}
	}

	return nil
}
