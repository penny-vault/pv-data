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
	"os"
	"path"
	"strings"
)

type Filer interface {
	CreateFile(name string, data []byte) (string, error)
}

type FSFiler struct {
	BasePath string
}

func (fs *FSFiler) CreateFile(name string, data []byte) (string, error) {
	filePath := path.Join(fs.BasePath, name)
	err := os.WriteFile(filePath, data, 0644)
	return filePath, err
}

func NewFilerFromString(spec string) Filer {
	switch {
	case strings.HasPrefix(spec, "file://"):
		return &FSFiler{
			BasePath: strings.TrimPrefix(spec, "file://"),
		}
	}
	return nil
}
