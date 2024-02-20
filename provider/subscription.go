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
	"errors"

	"github.com/google/uuid"
	"github.com/penny-vault/pvdata/library"
)

var (
	ErrProviderNotFound = errors.New("provider not found")
	ErrDatasetNotFound  = errors.New("dataset not found")
)

// NewSubscription returns a new subscription object with the dataset
// properly filled out
func NewSubscription(providerName, datasetName string, config map[string]string, myLibrary *library.Library) (*library.Subscription, error) {
	providerObj, ok := Map[providerName]
	if !ok {
		return nil, ErrProviderNotFound
	}

	datasetObj, ok := providerObj.Datasets()[datasetName]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	dataTypes := datasetObj.DataTypes

	subscription := &library.Subscription{
		ID:        uuid.New(),
		Name:      providerObj.Name(),
		Provider:  providerName,
		DataTypes: make([]string, len(dataTypes)),
		Config:    config,
		Schedule:  "0 0 * * 1-5",
		Library:   myLibrary,
	}

	// make sure that the dataset types and tables are populated
	subscription.DataTypes = make([]string, len(dataTypes))
	for idx, dataType := range dataTypes {
		subscription.DataTypes[idx] = dataType.Name
	}

	subscription.ComputeTableNames()

	return subscription, nil
}
