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
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
	gen "github.com/Thearas/dodo/src/generator"
)

// ImportConfig holds the configuration values
var ImportConfig = Import{}

// Import holds the configuration for the import command
type Import struct {
	Data            string
	ColumnSeparator string
	LineDelimiter   string
	MaxFilterRatio  float64

	// for external table, import via pyspark cli
	Spark Spark

	table2datafiles map[string][]string
}

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import CSV data to Doris database",
	Long: `Import CSV data to Doris via stream load, need 'sh' and 'curl' command.

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

		// only for external catalog
		var (
			conn              *sqlx.DB
			createCatalogStmt string
			sshUrl            *url.URL
			err               error
		)

		if !src.IsInternalCatalog(GlobalConfig.Catalog) {
			conn, err = connectDBWithoutDBName()
			if err != nil {
				return err
			}
			createCatalogStmt, err = src.ShowCreateCatalog(ctx, conn, GlobalConfig.Catalog)
			if err != nil {
				return err
			}
			sshUrl, err = url.Parse(fmt.Sprintf("ssh://root@%s:22", GlobalConfig.DBHost))
			if err != nil {
				return err
			}
			user := sshUrl.User
			sshUrl.User = url.UserPassword(user.Username(), SparkConfig.SSHPassword)
		}

		logrus.Infof("Import data for %d tables, parallel: %d", len(ImportConfig.table2datafiles), GlobalConfig.Parallel)

		g := src.ParallelGroup(GlobalConfig.Parallel)
		for table, datafiles := range ImportConfig.table2datafiles {
			// cancel when ctrl+c
			if ctx.Err() != nil {
				return ctx.Err()
			}

			dbtable := strings.SplitN(table, ".", 2)

			// 1. For internal catalog, import via stream load
			if src.IsInternalCatalog(GlobalConfig.Catalog) {
				for i, data := range datafiles {
					// cancel when ctrl+c
					if ctx.Err() != nil {
						return ctx.Err()
					}
					g.Go(func() error {
						return src.StreamLoad(
							ctx,
							GlobalConfig.DBHost, cast.ToString(GlobalConfig.HTTPPort),
							GlobalConfig.DBUser, GlobalConfig.DBPassword,
							dbtable[0], dbtable[1], data,
							fmt.Sprintf("%d/%d", i+1, len(datafiles)),
							ImportConfig.ColumnSeparator,
							ImportConfig.LineDelimiter,
							ImportConfig.MaxFilterRatio,
							GlobalConfig.DryRun,
						)
					})
				}
				continue
			}

			// 2. For external table, import via pyspark cli
			createTableStmt, err := src.ShowCreateTables(ctx, conn, false, dbtable[0], dbtable[1])
			if err != nil {
				return err
			}
			sparkCli, err := src.NewSparkCli(
				ctx, conn, GlobalConfig.DBHost,
				createCatalogStmt, createTableStmt[0].CreateStmt,
				ImportConfig.Spark.StorageSecretKey,
				sshUrl.String(), ImportConfig.Spark.SSHPrivateKey,
				GlobalConfig.DodoDataDir, GlobalConfig.Parallel,
			)
			if err != nil {
				return err
			}

			logrus.Infof("Spark load %s", table)

			err = src.PysparkImportCSV(
				ctx, sparkCli,
				dbtable[0], dbtable[1],
				// only support passing directory for external table import
				filepath.Dir(datafiles[0]),
				ImportConfig.ColumnSeparator,
				ImportConfig.Spark.AdditionalParams...,
			)
			if err != nil {
				return fmt.Errorf("pyspark import table %s failed: %v", table, err)
			}
		}

		return g.Wait()
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().SortFlags = false
	importCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(importCmd, "dodo-data-dir", "output", "dry-run", "parallel", "host", "port", "http-port", "user", "password", "catalog", "dbs", "tables")

	pFlags := importCmd.PersistentFlags()
	pFlags.StringVarP(&ImportConfig.Data, "data", "d", "", "Directory or files where CSV data located, default is 'output/gendata/'")
	pFlags.StringVar(&ImportConfig.ColumnSeparator, "column-separator", string(gen.ColumnSeparator), "Column separator for CSV data")
	pFlags.StringVar(&ImportConfig.LineDelimiter, "line-delimiter", "\n", "Line delimiter for CSV data, e.g. ☉")
	pFlags.Float64Var(&ImportConfig.MaxFilterRatio, "max-filter-ratio", 0, "Maximum error tolerance rate for stream load (0.0~1.0), see https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual")

	// spark flags
	pFlags.StringSliceVarP(&ImportConfig.Spark.AdditionalParams, "spark-params", "p", []string{}, "Additional parameters for pyspark cli")
	pFlags.StringVarP(&ImportConfig.Spark.StorageSecretKey, "storage-secret-key", "s", "", "Storage secret key (like for s3/oss/...) for pyspark cli")
	pFlags.StringVar(&ImportConfig.Spark.SSHPassword, "ssh-password", "", "SSH password for DB host")
	pFlags.StringVar(&ImportConfig.Spark.SSHPrivateKey, "ssh-private-key", "~/.ssh/id_rsa", "File path of SSH private key for DB host")

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
	// only support passing directory for external table import
	if isFile && src.IsInternalCatalog(GlobalConfig.Catalog) {
		if len(GlobalConfig.Tables) != 1 {
			return errors.New("expect only import one table when specifying data file(s)")
		}
		table2datafiles[GlobalConfig.Tables[0]] = dataFiles
	} else if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
			dbPrefix := db + "."
			subdirs, err := os.ReadDir(ImportConfig.Data)
			if err != nil {
				logrus.Errorf("Get db '%s' data file under '%s' failed", db, filepath.Join(ImportConfig.Data, fmt.Sprintf("%s.*", db)))
				return err
			}
			datadirs := lo.FilterMap(subdirs, func(d os.DirEntry, _ int) (string, bool) {
				return filepath.Join(ImportConfig.Data, d.Name()), d.IsDir() && strings.HasPrefix(d.Name(), dbPrefix)
			})

			logrus.Infoln("Found", len(datadirs), "table(s) to be imported for database", db)

			for _, datadir := range datadirs {
				dbtable := filepath.Base(strings.TrimSuffix(datadir, "/"))
				datafiles, err := src.FileGlob([]string{filepath.Join(datadir, "*.csv")})
				if err != nil {
					return err
				}

				logrus.Debugln("found", len(datafiles), "data files to be imported for table", dbtable)

				if len(datafiles) == 0 {
					continue
				}

				table2datafiles[dbtable] = datafiles
			}
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			datadir := filepath.Join(ImportConfig.Data, table, "*.csv")
			datafiles, err := src.FileGlob([]string{datadir})
			if err != nil {
				logrus.Errorf("Get table '%s' data files under '%s' failed", table, datadir)
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
