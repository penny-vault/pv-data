// Copyright 2023
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

package cmd

import (
	"fmt"
	"strings"

	"github.com/penny-vault/pvdata/pkginfo"
	"github.com/spf13/cobra"
)

var (
	deps  bool
	short bool
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version info",
	Run: func(cmd *cobra.Command, args []string) {
		if short {
			fmt.Println(pkginfo.Version)
		} else {
			fmt.Println(pkginfo.BuildVersionString())
		}

		if deps {
			fmt.Printf("\n\n")
			fmt.Println(strings.Join(pkginfo.GetDependencyList(), "\n"))
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	versionCmd.Flags().BoolVarP(&deps, "deps", "d", false, "print dependencies")
	versionCmd.Flags().BoolVarP(&short, "short", "s", false, "only print version number")
}
