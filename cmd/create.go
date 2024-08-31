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
	"slices"
	"strings"
	"sync"

	"github.com/emirpasic/gods/queues/circularbuffer"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	createTableDDLs       = []string{}
	createOtherDDLs       = []string{} // like views and other unknown ddls
	createContinueOnError bool
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

		logrus.Infof("Create %d table(s) and %d view(s), parallel: %d", len(createTableDDLs), len(createOtherDDLs), GlobalConfig.Parallel)

		db, err := connectDBWithoutDBName()
		if err != nil {
			return err
		}
		beCount, err := src.ShowBackendCount(ctx, db)
		if err != nil {
			return err
		}

		// 1. Create tables first.
		tableErrs, err := runCreateTableDDLs(createTableDDLs, GlobalConfig.Parallel, createContinueOnError, func(t string) error {
			dbname, _, _ := dbtableFromFileName(t)
			if dbname == "" {
				return fmt.Errorf("failed to get database name from ddl file %s, expect file name format: <db name>.<table name>.<table|view>.sql", t)
			}
			logrus.Debugf("create ddl file %s in db '%s'", t, dbname)
			_, err := src.RunCreateSQL(ctx, db, dbname, t, beCount, GlobalConfig.DryRun)
			return err
		})
		if err != nil {
			return err
		}

		// 2. Create views in queue.
		otherErrs, err := runCreateOtherDDLs(createOtherDDLs, createContinueOnError, func(v string) (string, error) {
			dbname, _, _ := dbtableFromFileName(v)
			if dbname == "" {
				return "", fmt.Errorf("failed to get database name from ddl file %s, expect file name format: <db name>.<table name>.<table|view>.sql", v)
			}
			return src.RunCreateSQL(ctx, db, dbname, v, beCount, GlobalConfig.DryRun)
		})
		if err != nil {
			return err
		}

		return summarizeCreateErrors(append(tableErrs, otherErrs...))
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.PersistentFlags().SortFlags = false
	createCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(createCmd, "output", "dry-run", "parallel", "host", "port", "user", "password", "catalog", "dbs", "tables")

	pFlags := createCmd.PersistentFlags()
	pFlags.StringSliceVarP(&createTableDDLs, "ddl", "d", nil, "Directories or files containing DDL (.sql), default is 'output/ddl/'")
	pFlags.BoolVar(&createContinueOnError, "continue-on-error", false, "Skip failed DDLs and continue creating remaining schemas; report failures at the end")
}

//nolint:revive // continueOnError is the explicit CLI mode switch for create.
func runCreateTableDDLs(tableDDLs []string, parallel int, continueOnError bool, run func(string) error) ([]error, error) {
	if !continueOnError {
		g := src.ParallelGroup(parallel)
		for _, ddl := range tableDDLs {
			g.Go(func() error { return run(ddl) })
		}
		return nil, g.Wait()
	}

	var (
		g    = src.ParallelGroup(parallel)
		mu   sync.Mutex
		errs []error
	)
	for _, ddl := range tableDDLs {
		g.Go(func() error {
			if err := run(ddl); err != nil {
				logrus.Warnf("Skip ddl file %s due to error: %v", ddl, err)
				mu.Lock()
				errs = append(errs, fmt.Errorf("ddl file %s: %w", ddl, err))
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return errs, nil
}

//nolint:revive // continueOnError is the explicit CLI mode switch for create.
func runCreateOtherDDLs(otherDDLs []string, continueOnError bool, run func(string) (string, error)) ([]error, error) {
	if len(otherDDLs) == 0 {
		return nil, nil
	}

	queue := circularbuffer.New(len(otherDDLs))
	lo.ForEach(otherDDLs, func(v string, _ int) { queue.Enqueue(lo.Tuple2[string, int]{A: v, B: 1}) })

	var errs []error
	for !queue.Empty() {
		v_, _ := queue.Dequeue()
		v, count := v_.(lo.Tuple2[string, int]).Unpack()

		logrus.Debugln("create ddl file", v, ", round:", count)
		needDeps, err := run(v)
		if err != nil {
			if !continueOnError {
				return nil, err
			}
			logrus.Warnf("Skip ddl file %s due to error: %v", v, err)
			errs = append(errs, fmt.Errorf("ddl file %s: %w", v, err))
			continue
		}

		// View may depend on other tables/views.
		if needDeps != "" {
			count++
			if count > len(otherDDLs) || queue.Empty() {
				depErr := fmt.Errorf("ddl need depends, %s needs %s", v, needDeps)
				if !continueOnError {
					return nil, depErr
				}
				logrus.Warnf("Skip ddl file %s due to unresolved dependency: %s", v, needDeps)
				errs = append(errs, depErr)
				continue
			}
			queue.Enqueue(lo.Tuple2[string, int]{A: v, B: count})
		}
	}

	return errs, nil
}

func summarizeCreateErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("create completed with %d error(s): %w", len(errs), errors.Join(errs...))
}

// completeCreateConfig validates and completes the create configuration
func completeCreateConfig() (err error) {
	ddldir := filepath.Join(GlobalConfig.OutputDir, "ddl")
	isDDLDir := false
	if len(createTableDDLs) == 1 {
		s, err := os.Stat(createTableDDLs[0]) // check if it is a directory
		if err == nil && s.IsDir() {
			ddldir = createTableDDLs[0]
			isDDLDir = true
		}
	}
	if len(createTableDDLs) > 0 && !isDDLDir {
		createDDLs_, err := src.FileGlob(createTableDDLs)
		if err != nil {
			return err
		}
		var tableDDLs []string
		for _, ddl := range createDDLs_ {
			db, _, isTable := dbtableFromFileName(ddl)
			isDumpTable := db != ""
			if isDumpTable && isTable {
				tableDDLs = append(tableDDLs, ddl)
			} else {
				createOtherDDLs = append(createOtherDDLs, ddl)
			}
		}
		createTableDDLs = tableDDLs
		return nil
	}

	if err := completeDBTables(); err != nil {
		return err
	}

	// auto find ddl
	createTableDDLs = []string{}
	if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
			fmatch := filepath.Join(ddldir, fmt.Sprintf("%s.*.table.sql", db))
			tableddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed", db, fmatch)
				return err
			}
			createTableDDLs = append(createTableDDLs, tableddls...)

			fmatch = filepath.Join(ddldir, fmt.Sprintf("%s.*view.sql", db))
			viewddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed", db, fmatch)
				return err
			}
			createOtherDDLs = append(createOtherDDLs, viewddls...)
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			tableddl := filepath.Join(ddldir, fmt.Sprintf("%s.table.sql", table))
			if _, err := os.Stat(tableddl); err != nil {
				// maybe a view
				fmatch := filepath.Join(ddldir, fmt.Sprintf("%s.*view.sql", table))
				if viewddls, err := src.FileGlob([]string{fmatch}); err == nil && len(viewddls) > 0 {
					createOtherDDLs = append(createOtherDDLs, viewddls...)
				}
				continue
			}
			createTableDDLs = append(createTableDDLs, tableddl)
		}
	}

	slices.Sort(createTableDDLs)
	slices.Sort(createOtherDDLs)

	return nil
}

func dbtableFromFileName(file string) (string, string, bool) {
	// table ddl file has 4 parts: {db}.{table}.{table|view|materialized_view|...}.sql
	dumpsuffixs := lo.Map(src.AllSchemaTypes, func(t src.SchemaType, _ int) string { return t.Lower() })

	parts := strings.Split(filepath.Base(file), ".")
	isDumpTable := len(parts) == 4 && (lo.ContainsBy(dumpsuffixs, func(s string) bool { return parts[2] == s && parts[3] == "sql" }))
	if !isDumpTable {
		return "", "", false
	}

	return parts[0], parts[1], parts[2] == "table"
}
