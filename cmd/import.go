/*
Copyright © 2025 Thearas thearas850@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

// ImportConfig holds the configuration values
var ImportConfig = Import{}

// Import holds the configuration for the gendata command
type Import struct {
	Data string

	table2datafiles map[string][]string
}

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import generated data to Doris database",
	Long: `Import generated data to Doris via stream load, need 'sh' and 'curl' command.

Example:
  dodo import --dbs db1,db2
  dodo import --dbs db1 --tables t1,t2 --http-port 8030 --data output/gendata/
  dodo import --dbs db1 --tables t1 --data data.csv`,
	Aliases: []string{"i"},
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := completeImportConfig(); err != nil {
			return err
		}
		if len(ImportConfig.table2datafiles) == 0 {
			logrus.Infoln("No table data file found")
			return nil
		}
		GlobalConfig.Parallel = min(GlobalConfig.Parallel, len(ImportConfig.table2datafiles))

		logrus.Infof("Import data for %d tables, parallel: %d\n", len(ImportConfig.table2datafiles), GlobalConfig.Parallel)

		g := src.ParallelGroup(GlobalConfig.Parallel)
		for table, datafiles := range ImportConfig.table2datafiles {
			dbtable := strings.SplitN(table, ".", 2)
			for i, data := range datafiles {
				g.Go(func() error {
					return src.StreamLoad(
						ctx,
						GlobalConfig.DBHost, cast.ToString(GlobalConfig.HTTPPort),
						GlobalConfig.DBUser, GlobalConfig.DBPassword,
						dbtable[0], dbtable[1], data,
						fmt.Sprintf("%d/%d", i+1, len(datafiles)),
						GlobalConfig.DryRun,
					)
				})
			}
		}

		return g.Wait()
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().SortFlags = false
	importCmd.Flags().SortFlags = false

	pFlags := importCmd.PersistentFlags()
	pFlags.StringVarP(&ImportConfig.Data, "data", "d", "", "Directory or file where CSV data files located")

}

func completeImportConfig() (err error) {
	if ImportConfig.Data == "" {
		ImportConfig.Data = filepath.Join(GlobalConfig.OutputDir, "gendata")
	}

	if err := completeDBTables("expected at least one database or tables, please use --dbs/--tables flag"); err != nil {
		return err
	}

	table2datafiles := map[string][]string{}
	// if --data is data file(s), just load it
	dataFiles, _ := src.FileGlob(strings.Split(ImportConfig.Data, ","))
	var isFile bool
	if len(dataFiles) > 0 {
		f, err := os.Stat(dataFiles[0])
		isFile = err == nil && !f.IsDir()
	}
	if isFile {
		if len(GlobalConfig.Tables) != 1 {
			return errors.New("expect only import one table when specifying data file(s)")
		}
		table2datafiles[GlobalConfig.Tables[0]] = dataFiles
	} else if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
			dbPrefix := db + "."
			subdirs, err := os.ReadDir(ImportConfig.Data)
			if err != nil {
				logrus.Errorf("Get db '%s' data file under '%s' failed\n", db, filepath.Join(ImportConfig.Data, fmt.Sprintf("%s.*", db)))
				return err
			}
			datadirs := lo.FilterMap(subdirs, func(d os.DirEntry, _ int) (string, bool) {
				return filepath.Join(ImportConfig.Data, d.Name()), d.IsDir() && strings.HasPrefix(d.Name(), dbPrefix)
			})

			logrus.Infoln("Found", len(datadirs), "table(s) to be imported for database", db)

			for _, datadir := range datadirs {
				dbtable := filepath.Base(strings.TrimSuffix(datadir, "/"))
				datafiles, err := src.FileGlob([]string{filepath.Join(datadir, "*")})
				if err != nil {
					return err
				}

				logrus.Debugln("found", len(datadirs), "data files to be imported for table", dbtable)

				if len(datafiles) == 0 {
					continue
				}

				table2datafiles[dbtable] = datafiles
			}
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			datadir := filepath.Join(ImportConfig.Data, table, "*")
			datafiles, err := src.FileGlob([]string{datadir})
			if err != nil {
				logrus.Errorf("Get table '%s' data files under '%s' failed\n", table, datadir)
				return err
			}
			if len(datafiles) == 0 {
				continue
			}
			table2datafiles[table] = datafiles
		}
	}

	ImportConfig.table2datafiles = table2datafiles

	return nil
}
