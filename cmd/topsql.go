/*
Copyright © 2024 Thearas thearas850@gmail.com

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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var TopSqlConfig = TopSql{}

type TopSql struct {
	SqlFiles     []string
	TopN         int
	SortBy       string
	MinCount     int
	ReplayOutput bool
	From_, To_   string
	From, To     int64
}

// topsqlCmd represents the topsql command
var topsqlCmd = &cobra.Command{
	Use:     "topsql",
	Short:   "Find the most common SQL structures from dump files",
	Aliases: []string{"ts"},
	Long: `
Find the most common SQL structures from dump files.

This command analyzes SQL files dumped by 'dodo dump' and identifies the top N 
most frequently occurring SQL patterns. It normalizes SQLs by replacing literals 
with placeholders (e.g., 'SELECT 1' and 'SELECT 2' are treated as the same pattern).

Complexity score is calculated based on:
  - JOINs (weight: 2)
  - Subqueries (weight: 3)
  - Aggregate functions (weight: 1)
  - Window functions (weight: 2)
  - UNION/INTERSECT/EXCEPT (weight: 2)
  - CASE expressions (weight: 1)
  - CTEs (weight: 2)
  - Nesting depth
`,
	Example: `  dodo topsql -f /path/to/dump.sql
  dodo topsql -f "./output/sql/*.sql" --top 20
  dodo topsql -f q0.sql -f q1.sql
  dodo topsql -f ./output/sql/*.sql --sort complexity
  dodo topsql -f ./output/sql/*.sql --dbs db1,db2`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if err := completeTopSqlConfig(); err != nil {
			return err
		}

		return src.TopSql(
			cmd.Context(),
			TopSqlConfig.SqlFiles,
			TopSqlConfig.TopN,
			src.TopSqlSortBy(TopSqlConfig.SortBy),
			TopSqlConfig.MinCount,
			TopSqlConfig.ReplayOutput,
			TopSqlConfig.From,
			TopSqlConfig.To,
			GlobalConfig.DBs,
			GlobalConfig.Parallel,
		)
	},
}

func init() {
	rootCmd.AddCommand(topsqlCmd)
	topsqlCmd.PersistentFlags().SortFlags = false
	topsqlCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(topsqlCmd, "output", "parallel", "dbs")

	pFlags := topsqlCmd.PersistentFlags()
	pFlags.StringSliceVarP(&TopSqlConfig.SqlFiles, "file", "f", []string{}, "SQL files to analyze (supports glob pattern)")
	pFlags.IntVar(&TopSqlConfig.TopN, "top", 10, "Number of top SQL patterns to show")
	pFlags.StringVar(&TopSqlConfig.SortBy, "sort", "count", "Sort by: 'count', 'complexity', or 'combined' (count*complexity)")
	pFlags.IntVar(&TopSqlConfig.MinCount, "min-count", 0, "Ignore patterns with count less than this value (not included in statistics)")
	pFlags.BoolVar(&TopSqlConfig.ReplayOutput, "replay-output", false, "Output a replay SQL file with same-pattern SQLs replaced by sample SQL (keep original dodo meta comments)")
	pFlags.StringVar(&TopSqlConfig.From_, "from", "", "Only analyze queries from this time, like '2006-01-02 15:04:05'")
	pFlags.StringVar(&TopSqlConfig.To_, "to", "", "Only analyze queries to this time, like '2006-01-02 16:04:05'")

	topsqlCmd.RegisterFlagCompletionFunc("sort", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"count", "complexity", "combined"}, cobra.ShellCompDirectiveNoFileComp
	})
}

func completeTopSqlConfig() (err error) {
	if len(TopSqlConfig.SqlFiles) == 0 {
		// default to output/sql/*.sql
		defaultPath := filepath.Join(GlobalConfig.OutputDir, "sql", "*.sql")
		TopSqlConfig.SqlFiles = []string{defaultPath}
		logrus.Infof("No SQL files specified, using default: %s", defaultPath)
	}

	if TopSqlConfig.TopN <= 0 {
		return errors.New("--top must be a positive integer")
	}

	if TopSqlConfig.SortBy != "count" && TopSqlConfig.SortBy != "complexity" && TopSqlConfig.SortBy != "combined" {
		return errors.New("--sort must be 'count', 'complexity', or 'combined'")
	}

	if TopSqlConfig.MinCount < 0 {
		return errors.New("--min-count must be a non-negative integer")
	}

	var t time.Time
	if TopSqlConfig.From_ != "" {
		t, err = time.Parse(time.DateTime, TopSqlConfig.From_)
		if err != nil {
			return err
		}
		TopSqlConfig.From = t.UnixMilli()
	}
	if TopSqlConfig.To_ != "" {
		t, err = time.Parse(time.DateTime, TopSqlConfig.To_)
		if err != nil {
			return err
		}
		TopSqlConfig.To = t.UnixMilli()
	}
	if TopSqlConfig.From > 0 && TopSqlConfig.To > 0 && TopSqlConfig.From > TopSqlConfig.To {
		return fmt.Errorf("invalid time range: --from '%s' is after --to '%s'", TopSqlConfig.From_, TopSqlConfig.To_)
	}

	return nil
}
