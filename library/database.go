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
	"fmt"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Library struct {
	DBUrl string
	Name  string
	Owner string

	Pool *pgxpool.Pool
}

// Connect to the database configured for the library
func (myLibrary *Library) Connect(ctx context.Context) error {
	if myLibrary.Pool != nil {
		return nil
	}

	pool, err := pgxpool.New(context.Background(), myLibrary.DBUrl)
	if err != nil {
		return err
	}
	myLibrary.Pool = pool

	return nil
}

// Close the database pool
func (myLibrary *Library) Close() {
	myLibrary.Pool.Close()
}

// NewFromDB creates a new library object with values from the database
func NewFromDB(ctx context.Context, dbURL string) (*Library, error) {
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	myLibrary := Library{
		DBUrl: dbURL,
		Pool:  pool,
	}

	if err := conn.QueryRow(ctx, "SELECT name, owner FROM library").Scan(&myLibrary.Name, &myLibrary.Owner); err != nil {
		return nil, err
	}

	return &myLibrary, nil
}

// SaveDB creates a new record in the library table for this library
func (myLibrary *Library) SaveDB(ctx context.Context) error {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, `INSERT INTO library ("name", "owner") VALUES ($1, $2)`, myLibrary.Name, myLibrary.Owner)
	return err
}

// NumSubscriptions returns the total count of subscriptions configured in the database
func (myLibrary *Library) NumSubscriptions(ctx context.Context) (int, error) {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	count := 0
	err = conn.QueryRow(ctx, "SELECT count(*) FROM subscriptions WHERE active='t'").Scan(&count)
	return count, err
}

// LastUpdated returns the date that the database was last updated
func (myLibrary *Library) LastUpdated(ctx context.Context) (time.Time, error) {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Release()

	var lastUpdated time.Time
	err = conn.QueryRow(ctx, "SELECT coalesce(max(last_run), '0001-01-01'::timestamp) FROM subscriptions WHERE active='t'").Scan(&lastUpdated)
	if err != nil {
		return time.Time{}, err
	}

	return lastUpdated, nil
}

// TotalRecords returns the total number of records in the library
func (myLibrary *Library) TotalRecords(ctx context.Context) (int, error) {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	count := 0
	err = conn.QueryRow(ctx, "SELECT coalesce(sum(total_records), 0) FROM subscriptions WHERE active='t'").Scan(&count)
	return count, err
}

// TotalSecurities returns the total number of securities in the library
func (myLibrary *Library) TotalSecurities(ctx context.Context) (int, error) {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	count := 0
	err = conn.QueryRow(ctx, "SELECT coalesce(sum(total_records), 0) FROM subscriptions WHERE active='t'").Scan(&count)
	return count, err
}

// Subscriptions returns an array of subscription objects
func (myLibrary *Library) Subscriptions(ctx context.Context) ([]*Subscription, error) {
	var subscriptions []*Subscription
	err := pgxscan.Select(ctx, myLibrary.Pool, &subscriptions,
		`SELECT id, name, provider, dataset, config, data_tables, data_types, total_records,
num_records_last_import, total_securities, num_securities_last_import,
coalesce(first_obs_date, '0001-01-01'::timestamp) as first_obs_date,
coalesce(last_obs_date, '0001-01-01'::timestamp) as last_obs_date, schedule, health_check_id,
coalesce(last_run, '0001-01-01'::timestamp) as last_run, active, schema_version, created_on,
created_by FROM subscriptions`)
	for _, sub := range subscriptions {
		sub.Library = myLibrary
	}
	return subscriptions, err
}

// SubscriptionFromID fetches a subscription from the library with the given ID
func (myLibrary *Library) SubscriptionFromID(ctx context.Context, id string) (*Subscription, error) {
	conn, err := myLibrary.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	subscription := &Subscription{
		Library: myLibrary,
	}

	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT id, name, provider, dataset, config,
	data_tables, data_types, total_records, num_records_last_import, total_securities,
	num_securities_last_import, coalesce(first_obs_date, '0001-01-01'::timestamp) as first_obs_date,
	coalesce(last_obs_date, '0001-01-01'::timestamp) as last_obs_date,
	schedule, health_check_id, coalesce(last_run, '0001-01-01'::timestamp) as last_run, active,
	schema_version, created_on, created_by FROM subscriptions WHERE id::text like '%s%%' LIMIT 1`, id))
	if err != nil {
		return nil, err
	}

	err = pgxscan.ScanOne(subscription, rows)
	if err != nil {
		return nil, err
	}

	return subscription, nil
}
