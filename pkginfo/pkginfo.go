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
package pkginfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"

	"github.com/rs/zerolog/log"
)

var (
	BuildDate  string
	CommitHash string
	Version    string
)

// BuildVersionString returns a version info string suitable for printing on the command line
func BuildVersionString() string {
	osArch := runtime.GOOS + "/" + runtime.GOARCH
	goVersion := runtime.Version()

	versionString := fmt.Sprintf(`pvdata %s %s

Build Date: %s
Commit: %s
Built with: %s`, Version, osArch, BuildDate, CommitHash, goVersion)

	return versionString
}

// GetDependencyList returns an array of all dependencies linked in with this program
// each string is of the form `package="version"`
func GetDependencyList() []string {
	var deps []string

	formatDep := func(path, version string) string {
		return fmt.Sprintf("%s=%q", path, version)
	}

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		log.Error().Msg("could not get package build info")
		return deps
	}

	for _, dep := range buildInfo.Deps {
		deps = append(deps, formatDep(dep.Path, dep.Version))
	}

	sort.Strings(deps)

	return deps
}
