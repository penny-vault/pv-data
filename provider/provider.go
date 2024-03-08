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
	"time"

	"github.com/penny-vault/pvdata/data"
	"github.com/penny-vault/pvdata/library"
)

type Provider interface {
	Name() string
	ConfigDescription() map[string]string
	Description() string
	Datasets() map[string]Dataset
}

type Dataset struct {
	Name        string
	Description string
	DataTypes   []*data.DataType
	DateRange   func() (time.Time, time.Time)

	// Fetch is called when pvdata wants to retrieve measurements from the dataset. It
	// passes a config with the provider configuration, a channel to write results to,
	// a logger to write log messages to, and a channel to write progress.
	Fetch func(context.Context, *library.Subscription, chan<- *data.Observation, chan<- data.RunSummary)
}
