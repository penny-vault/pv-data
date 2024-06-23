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
package library

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gosimple/slug"
	"github.com/jackc/pgx/v5"
	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/healthcheck"
	"github.com/rs/zerolog/log"
)

type Subscription struct {
	ID   uuid.UUID
	Name string

	Provider string
	Dataset  string
	Config   map[string]string

	DataTables    []string
	DataTypes     []string
	DataTablesMap map[string]string
	IsPartitioned bool

	TotalRecords         int64
	NumRecordsLastImport int64

	TotalSecurities         int64
	NumSecuritiesLastImport int64

	FirstObsDate time.Time
	LastObsDate  time.Time

	Schedule      string
	HealthCheckID string
	LastRun       time.Time
	Active        bool
	SchemaVersion int

	CreatedOn time.Time
	CreatedBy string

	Library *Library
}

type dateRange struct {
	Start int
	End   int
}

// Delete the subscription from database along with all associated tables
func (subscription *Subscription) Delete(ctx context.Context) error {
	conn, err := subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("error rollingback tx")
			}
		}
	}()

	tables := subscription.PartitionTables()
	tables = append(tables, subscription.DataTables...)

	// delete tables
	for _, tblName := range tables {
		log.Info().Str("TableName", tblName).Msg("delete table")
		_, err := tx.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tblName))
		if err != nil {
			return err
		}
	}

	// delete subscription entry
	if _, err := tx.Exec(ctx, "DELETE FROM subscriptions WHERE id=$1", subscription.ID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// now that all database related modification has succeeded delete any corresponding health check
	if subscription.HealthCheckID != "" {
		if err := healthcheck.Delete(subscription.HealthCheckID); err != nil {
			return err
		}
	}

	return nil
}

// Activate the subscription
func (subscription *Subscription) Activate(ctx context.Context) error {
	conn, err := subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("error rollingback tx")
			}
		}
	}()

	// activate subscription entry
	if _, err := tx.Exec(ctx, "UPDATE subscriptions SET active='t' WHERE id=$1", subscription.ID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// now that all database related modification has succeeded resume any corresponding health check
	if subscription.HealthCheckID != "" {
		if err := healthcheck.Resume(subscription.HealthCheckID); err != nil {
			return err
		}
	}

	return nil
}

// Deactivate the subscription; all data is still saved in the database but the subscription
// is marked as inactive and it won't show up in reports
func (subscription *Subscription) Deactivate(ctx context.Context) error {
	conn, err := subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("error rollingback tx")
			}
		}
	}()

	// de-activate subscription entry
	if _, err := tx.Exec(ctx, "UPDATE subscriptions SET active='f' WHERE id=$1", subscription.ID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// now that all database related modification has succeeded pause any corresponding health check
	if subscription.HealthCheckID != "" {
		if err := healthcheck.Pause(subscription.HealthCheckID); err != nil {
			return err
		}
	}

	return nil
}

// Save the subscription to the database
func (subscription *Subscription) Save(ctx context.Context) error {
	conn, err := subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("error rollingback tx")
			}
		}
	}()

	// create table structure for each data type this dataset produces
	if err := subscription.createTables(ctx, tx); err != nil {
		return err
	}

	// make sure current user is set on subscription
	if user, err := user.Current(); err != nil {
		return err
	} else {
		subscription.CreatedBy = user.Username
	}

	// create an entry in the subscription table
	if _, err := tx.Exec(ctx, `INSERT INTO subscriptions
("id", "name", "provider", "dataset", "config", "data_tables", "data_types",
 "schedule", "health_check_id", "schema_version", "created_by")
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`, subscription.ID.String(),
		subscription.Name, subscription.Provider, subscription.Dataset, subscription.Config,
		subscription.DataTables, subscription.DataTypes, subscription.Schedule,
		subscription.HealthCheckID, subscription.SchemaVersion, subscription.CreatedBy); err != nil {
		return err
	}

	// manage partitions
	if err := subscription.managePartitionsWithTransaction(ctx, tx); err != nil {
		return err
	}

	// commit to database
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

// Compute table names based on subscription data types
func (subscription *Subscription) ComputeTableNames() {
	ret := make([]string, len(subscription.DataTypes))
	subscription.DataTablesMap = make(map[string]string, len(subscription.DataTypes))
	for idx, dataType := range subscription.DataTypes {
		tbl := slug.Make(fmt.Sprintf("%s %s %s %s", subscription.Provider, subscription.Dataset, dataType, subscription.ID.String()[:5]))
		tbl = strings.ReplaceAll(tbl, "-", "_")
		ret[idx] = tbl

		subscription.DataTablesMap[dataType] = tbl
	}

	subscription.DataTables = ret
}

// ManagePartitions creates any new partitions needed for the subscription
func (subscription *Subscription) ManagePartitions(ctx context.Context) error {
	conn, err := subscription.Library.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("error rollingback tx")
			}
		}
	}()

	// manage partitions
	if err := subscription.managePartitionsWithTransaction(ctx, tx); err != nil {
		log.Error().Err(err).Msg("error encountered when creating partitions")
		return err
	}

	// commit to database
	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("error committing manage partitions transaction")
		return err
	}

	return nil
}

// managePartitionsWithTransaction uses the specified transaction `tx` to create missing partitions
func (subscription *Subscription) managePartitionsWithTransaction(ctx context.Context, tx pgx.Tx) error {
	for idx, dataTypeName := range subscription.DataTypes {
		dataType := data.DataTypes[dataTypeName]
		dataTable := subscription.DataTables[idx]

		// if table is not partitioned skip to next dataType
		if !dataType.IsPartitioned {
			continue
		}

		// construct a list of date ranges
		dates := []dateRange{
			{
				Start: 1900,
				End:   2000,
			},
			{
				Start: 2000,
				End:   2005,
			},
			{
				Start: 2005,
				End:   2010,
			},
			{
				Start: 2010,
				End:   2015,
			},
		}

		today := time.Now()
		year := today.Year() + 1

		for ii := 2015; ii < year; ii += 5 {
			dates = append(dates, dateRange{Start: ii, End: ii + 5})
		}

		// create tables for expected date ranges
		for _, dt := range dates {
			tableName := fmt.Sprintf("%s_%d_%d", dataTable, dt.Start, dt.End)
			sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%d-01-01') TO ('%d-01-01');",
				tableName, dataTable, dt.Start, dt.End)
			log.Debug().Str("SQL", sql).Msg("creating partition table")
			if _, err := tx.Exec(ctx, sql); err != nil {
				return err
			}
		}
	}
	return nil
}

// PartitionTables returns the table names for all paritions in the set
func (subscription *Subscription) PartitionTables() []string {
	tables := make([]string, 0, 10)

	for idx, dataTypeName := range subscription.DataTypes {
		dataType := data.DataTypes[dataTypeName]
		dataTable := subscription.DataTables[idx]

		// if table is not partitioned skip to next dataType
		if !dataType.IsPartitioned {
			continue
		}

		// construct a list of date ranges
		dates := []dateRange{
			{
				Start: 1900,
				End:   2000,
			},
			{
				Start: 2000,
				End:   2005,
			},
			{
				Start: 2005,
				End:   2010,
			},
			{
				Start: 2010,
				End:   2015,
			},
		}

		today := time.Now()
		year := today.Year() + 1

		for ii := 2015; ii < year; ii++ {
			dates = append(dates, dateRange{Start: ii, End: ii + 1})
		}

		// create tables for expected date ranges
		for _, dt := range dates {
			tableName := fmt.Sprintf("%s_%d_%d", dataTable, dt.Start, dt.End)
			tables = append(tables, tableName)
		}
	}

	return tables
}

func (subscription *Subscription) createTables(ctx context.Context, tx pgx.Tx) error {
	for idx, dataTypeName := range subscription.DataTypes {
		dataType := data.DataTypes[dataTypeName]
		schema := dataType.ExpandedSchema(subscription.DataTables[idx])
		_, err := tx.Exec(ctx, schema)
		if err != nil {
			return err
		}
	}
	return nil
}
