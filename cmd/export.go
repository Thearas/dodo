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
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/valyala/fasttemplate"

	"github.com/Thearas/dodo/src"
)

// ExportConfig holds the configuration values
var ExportConfig = Export{}

// Export holds the configuration for the export command
type Export struct {
	Target     string
	ToURL      string
	Properties map[string]string
	With       map[string]string

	dbconn *sqlx.DB
}

// TODO: Support BROKER export?
// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data from Doris",
	Long: `Export data from Doris via [Export](https://doris.apache.org/docs/sql-manual/sql-statements/data-modification/load-and-export/EXPORT) command.

Example:
  dodo export --target s3 --url 's3://bucket/export/{db}/{table}_' -p timeout=60 -w s3.endpoint=xxx -w s3.access_key=xxx -w s3.secret_key=xxx
  dodo export --target hdfs --url 'hdfs://path/to/export/{db}/{table}_' -w fs.defaultFS=hdfs://HDFS8000871 -w hadoop.username=xxx`,
	Aliases: []string{"e"},
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) (err error) {
		ctx, _ := signal.NotifyContext(cmd.Context(), syscall.SIGABRT, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

		if err := completeExportConfig(ctx); err != nil {
			return err
		}
		GlobalConfig.Parallel = min(GlobalConfig.Parallel, len(GlobalConfig.Tables))

		logrus.Infof("Export data for %d table(s) to '%s', parallel: %d", len(GlobalConfig.Tables), ExportConfig.ToURL, GlobalConfig.Parallel)
		if len(GlobalConfig.Tables) == 0 {
			return nil
		}
		if !src.Confirm("Confirm") {
			return nil
		}

		g := src.ParallelGroup(GlobalConfig.Parallel)
		for _, t := range GlobalConfig.Tables {
			dbtable := strings.SplitN(t, ".", 2)
			if len(dbtable) != 2 {
				return fmt.Errorf("invalid table format '%s', expected 'db.table'", t)
			}
			dbname, table := dbtable[0], dbtable[1]
			toURL := fasttemplate.ExecuteString(ExportConfig.ToURL, "{", "}", map[string]any{"db": dbname, "table": table})

			g.Go(func() error {
				logrus.Infof("Exporting table '%s.%s' to '%s'", dbname, table, toURL)
				if err := src.Export(ctx, ExportConfig.dbconn, dbname, table, ExportConfig.Target, toURL, ExportConfig.With, ExportConfig.Properties); err != nil {
					return fmt.Errorf("export table '%s.%s' failed: %w", dbname, table, err)
				}
				logrus.Infof("Export completed for table '%s.%s'", dbname, table)
				return nil
			})
		}

		return g.Wait()
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.PersistentFlags().SortFlags = false
	exportCmd.Flags().SortFlags = false

	pFlags := exportCmd.PersistentFlags()
	pFlags.StringVarP(&ExportConfig.Target, "target", "t", "s3", "Target storage for the export, e.g. 's3', 'hdfs'")
	pFlags.StringVarP(&ExportConfig.ToURL, "url", "u", "", "Target URL that Doris export to, can use placeholders {db} and {table}, e.g. 's3://bucket/export/{db}/{table}_', 'hdfs://path/to/{db}/{table}_'")
	pFlags.StringToStringVarP(&ExportConfig.Properties, "props", "p", map[string]string{}, "Additional properties, e.g. 'format=parquet'")
	pFlags.StringToStringVarP(&ExportConfig.With, "with", "w", map[string]string{}, "Additional options for export target, e.g. 's3.endpoint=xxx'")

	exportCmd.RegisterFlagCompletionFunc("target", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"s3", "hdfs", "local"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
	})

	exportCmd.RegisterFlagCompletionFunc("url", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		compopts := cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
		switch ExportConfig.Target {
		case "s3":
			return []string{"s3://"}, compopts
		case "hdfs":
			return []string{"hdfs://"}, compopts
		case "local":
			return []string{"file://"}, compopts
		}
		return []string{}, cobra.ShellCompDirectiveError
	})

	compopts := cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault | cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveKeepOrder
	exportCmd.RegisterFlagCompletionFunc("props", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		// https://doris.apache.org/docs/sql-manual/sql-statements/data-modification/load-and-export/EXPORT#optional-parameters
		return []string{
				"label=",
				"column_separator=",
				"line_delimiter=",
				"timeout=",
				"columns=",
				"format=",
				"parallelism=",
				"delete_existing_files=",
				"max_file_size=",
				"with_bom=",
				"compress_type=",
			},
			compopts
	})

	exportCmd.RegisterFlagCompletionFunc("with", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		switch ExportConfig.Target {
		case "s3":
			return []string{"s3.endpoint=", "s3.access_key=", "s3.secret_key=", "s3.region="}, compopts
		case "hdfs":
			return []string{"fs.defaultFS=", "hadoop.username=", "fs.", "dfs.", "hadoop."}, compopts
		}
		return []string{}, cobra.ShellCompDirectiveError
	})
}

func completeExportConfig(ctx context.Context) (err error) {
	if err = completeDBTables(); err != nil {
		return err
	}

	ExportConfig.Target = strings.ToLower(ExportConfig.Target)
	if ExportConfig.Target == "" {
		return errors.New("export target is required, use --target or -t to specify it")
	}
	urlPrefix := ExportConfig.Target
	if urlPrefix == "local" {
		urlPrefix = "file"
	}
	if !strings.HasPrefix(ExportConfig.ToURL, urlPrefix+"://") {
		return fmt.Errorf("export URL must start with '%s://', got: '%s'", urlPrefix, ExportConfig.ToURL)
	}

	ExportConfig.dbconn, err = connectDBWithoutDBName()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// find tables if not provided
	if len(GlobalConfig.Tables) > 0 {
		return nil
	}
	for _, db := range GlobalConfig.DBs {
		schemas, err := src.ShowTables(ctx, ExportConfig.dbconn, db)
		if err != nil {
			return fmt.Errorf("failed to get tables for database '%s': %w", db, err)
		}
		tables := lo.FilterMap(schemas, func(s *src.Schema, _ int) (string, bool) {
			return s.Name, s.Type == src.SchemaTypeTable
		})
		logrus.Infof("Found %d table(s) in database '%s'", len(tables), db)
		for _, table := range tables {
			GlobalConfig.Tables = append(GlobalConfig.Tables, db+"."+table)
		}
	}

	return nil
}
