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
	"path/filepath"
	"slices"
	"strings"

	"github.com/emirpasic/gods/queues/circularbuffer"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	createTableDDLs = []string{}
	createOtherDDLs = []string{} // like views and other unknown ddls
	createConnDB    string
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create tables and views",
	Long: `Create tables and views.

Example:
  dodo create --dbs db1,db2
  dodo create --dbs db1 --tables table1,table2
  dodo create --ddl dir/*.sql`,
	Aliases: []string{"c"},
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := completeCreateConfig(); err != nil {
			return err
		}
		GlobalConfig.Parallel = min(GlobalConfig.Parallel, len(createTableDDLs))

		logrus.Infof("Create %d table(s) and %d view(s), parallel: %d\n", len(createTableDDLs), len(createOtherDDLs), GlobalConfig.Parallel)

		db, err := connectDBWithoutDBName()
		if err != nil {
			return err
		}
		beCount, err := src.ShowBackendCount(ctx, db)
		if err != nil {
			return err
		}

		// 1. Create tables first.
		g := src.ParallelGroup(GlobalConfig.Parallel)
		for _, t := range createTableDDLs {
			g.Go(func() error {
				dbname, _ := dbtableFromFileName(t)
				if createConnDB != "" {
					dbname = createConnDB
				}
				logrus.Debugf("create ddl file %s in db %s", t, dbname)
				if _, err := src.RunCreateSQL(ctx, db, dbname, t, beCount, GlobalConfig.DryRun); err != nil {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}

		// 2. Create views in queue.
		if len(createOtherDDLs) == 0 {
			return nil
		}
		queue := circularbuffer.New(len(createOtherDDLs))
		lo.ForEach(createOtherDDLs, func(v string, _ int) { queue.Enqueue(lo.Tuple2[string, int]{A: v, B: 1}) })
		for i := 0; !queue.Empty(); i++ {
			v_, _ := queue.Dequeue()
			v, count := v_.(lo.Tuple2[string, int]).Unpack()

			logrus.Debugln("create ddl file", v, ", round:", count)
			dbname, _ := dbtableFromFileName(v)
			if createConnDB != "" {
				dbname = createConnDB
			}
			needDeps, err := src.RunCreateSQL(ctx, db, dbname, v, beCount, GlobalConfig.DryRun)
			if err != nil {
				return err
			}

			// view may depends on other tables/views
			if needDeps != "" {
				count++
				if count > len(createOtherDDLs) {
					return fmt.Errorf("ddl need depends, message: %s", needDeps)
				}
				queue.Enqueue(lo.Tuple2[string, int]{A: v, B: count})
			}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.PersistentFlags().SortFlags = false
	createCmd.Flags().SortFlags = false

	pFlags := createCmd.PersistentFlags()
	pFlags.StringSliceVarP(&createTableDDLs, "ddl", "d", nil, "Directories or files containing DDL (.sql)")
	pFlags.StringVar(&createConnDB, "db", "", "The database to connect when creating schema")
}

// completeCreateConfig validates and completes the create configuration
func completeCreateConfig() (err error) {
	if len(createTableDDLs) > 0 {
		createDDLs_, err := src.FileGlob(createTableDDLs)
		if err != nil {
			return err
		}
		var tableDDLs []string
		for _, ddl := range createDDLs_ {
			db, _ := dbtableFromFileName(ddl)
			isDumpTable := db != ""
			if isDumpTable {
				tableDDLs = append(tableDDLs, ddl)
			} else {
				createOtherDDLs = append(createOtherDDLs, ddl)
			}
		}
		createTableDDLs = tableDDLs

		return err
	}

	ddldir := filepath.Join(GlobalConfig.OutputDir, "ddl")

	GlobalConfig.DBs, GlobalConfig.Tables = lo.Uniq(GlobalConfig.DBs), lo.Uniq(GlobalConfig.Tables)
	dbs, tables := GlobalConfig.DBs, GlobalConfig.Tables
	if len(dbs) == 0 && len(tables) == 0 {
		return errors.New("expected at least one database or tables, please use --dbs/--tables flag or --ddl flag")
	} else if len(dbs) == 1 {
		// prepend default database if only one database specified
		prefix := dbs[0] + "."
		for i, t := range GlobalConfig.Tables {
			if !strings.Contains(t, ".") {
				GlobalConfig.Tables[i] = prefix + t
			}
		}
	} else {
		for _, t := range tables {
			if !strings.Contains(t, ".") {
				return errors.New("expected database in table name when zero/multiple databases specified, e.g. --tables db1.table1,db2.table2")
			}
		}
	}

	if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
			fmatch := filepath.Join(ddldir, fmt.Sprintf("%s.*.table.sql", db))
			tableddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed\n", db, fmatch)
				return err
			}
			createTableDDLs = append(createTableDDLs, tableddls...)

			fmatch = filepath.Join(ddldir, fmt.Sprintf("%s.*view.sql", db))
			viewddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed\n", db, fmatch)
				return err
			}
			createOtherDDLs = append(createOtherDDLs, viewddls...)
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			tableddl := filepath.Join(ddldir, fmt.Sprintf("%s.table.sql", table))
			createTableDDLs = append(createTableDDLs, tableddl)
		}
	}

	slices.Sort(createTableDDLs)
	slices.Sort(createOtherDDLs)

	return nil
}
