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
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	noColor          bool
	minDurationDiff  time.Duration
	originalDumpSQLs []string

	// aggregate mode flags
	aggregate           bool
	aggregateSortBy     string
	aggregateTopN       int
	aggregateOutput     string
	aggregateReplaySQLs []string
)

const defaultAggregateDiffOutput = "output/diff/result.txt"

// diffCmd represents the diff command
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Diff a replay result with another or the original dump sql",
	Example: `dodo diff replay1/ replay2/
dodo diff --original-sqls dump.sql replay1/
dodo diff --aggregate replay1/
dodo diff --aggregate replay1/ replay2/ --sort p95-change --top 20`,
	SilenceUsage: true,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	RunE: func(_ *cobra.Command, args []string) error {
		if noColor {
			if err := os.Setenv("NO_COLOR", "true"); err != nil {
				return err
			}
		}

		if aggregate {
			return diffAggregate(args)
		}

		if !(len(args) == 2 || (len(originalDumpSQLs) > 0 && len(args) == 1)) {
			return errors.New("diff requires two replay result dirs or --original-sqls flag with one replay result dir")
		}

		if len(originalDumpSQLs) > 0 {
			return diffDumpSQL(args[0])
		}
		return diffTwoReplays(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.PersistentFlags().SortFlags = false
	diffCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(diffCmd, "output")

	flags := diffCmd.Flags()
	flags.BoolVar(&noColor, "no-color", false, "Disable color output")
	flags.DurationVar(&minDurationDiff, "min-duration-diff", 100*time.Millisecond, "Print diff if duration difference is greater than this value")
	flags.StringSliceVar(&originalDumpSQLs, "original-sqls", nil, "Diff with original dump sql instead of another replay result")

	// aggregate mode
	flags.BoolVarP(&aggregate, "aggregate", "a", false, "Aggregate replay results by PatternHash and show p50/p95/avg/max statistics")
	flags.StringVar(&aggregateSortBy, "sort", "p95-change", "Sort aggregate results by: 'p95-change', 'p50-change', 'avg-change', 'count' (single dir uses the corresponding metric for *-change)")
	flags.IntVar(&aggregateTopN, "top", 20, "Number of top patterns to show in aggregate mode")
	flags.StringVarP(&aggregateOutput, "output-file", "o", "", "Write aggregate report to file")
	flags.StringSliceVar(&aggregateReplaySQLs, "replay-sqls", nil, "Original replay SQL files used to fill sample SQL in aggregate diff output (supports glob pattern, default '<output-dir>/sql/*.sql')")

	diffCmd.RegisterFlagCompletionFunc("sort", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"p95-change", "p50-change", "avg-change", "count"}, cobra.ShellCompDirectiveNoFileComp
	})
}

func diffAggregate(args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("aggregate mode requires one or two replay result dirs")
	}

	sortBy := src.AggregateSortBy(aggregateSortBy)
	useColor := !noColor

	// Validate sort value
	switch sortBy {
	case src.AggregateSortByP95Change, src.AggregateSortByP50Change, src.AggregateSortByAvgChange, src.AggregateSortByCount:
	default:
		return fmt.Errorf("invalid --sort value %q, must be 'p95-change', 'p50-change', 'avg-change', or 'count'", aggregateSortBy)
	}

	var (
		output     string
		fileOutput string
	)

	if len(args) == 1 {
		// Single directory mode: just show aggregate stats
		logrus.Infof("Reading replay results from %s ...", args[0])
		stats, err := src.ReadReplayResults(args[0])
		if err != nil {
			return err
		}
		if len(stats) == 0 {
			logrus.Warn("No results with PatternHash found")
			return nil
		}

		output = src.FormatSingleAggregate(stats, sortBy, aggregateTopN)
		fileOutput = output
	} else {
		// Two directory mode: compare
		logrus.Infof("Reading replay results from %s (before) and %s (after) ...", args[0], args[1])
		before, err := src.ReadReplayResults(args[0])
		if err != nil {
			return fmt.Errorf("failed to read before dir: %w", err)
		}
		after, err := src.ReadReplayResults(args[1])
		if err != nil {
			return fmt.Errorf("failed to read after dir: %w", err)
		}
		if len(before) == 0 && len(after) == 0 {
			logrus.Warn("No results with PatternHash found in either directory")
			return nil
		}

		replaySQLFiles := aggregateReplaySQLs
		if len(replaySQLFiles) == 0 {
			defaultPath := filepath.Join(GlobalConfig.OutputDir, "sql", "*.sql")
			replaySQLFiles = []string{defaultPath}
			logrus.Infof("No replay SQL files specified, using default: %s", defaultPath)
		}

		queryIDToStmt, err := src.LoadReplaySQLSampleIndex(replaySQLFiles)
		if err != nil {
			return fmt.Errorf("failed to load replay sql samples: %w", err)
		}
		if len(queryIDToStmt) == 0 {
			logrus.Warn("No replay SQL samples found; sample SQL in aggregate diff output may be N/A")
		} else {
			src.FillSampleStmtsFromIndex(before, queryIDToStmt)
			src.FillSampleStmtsFromIndex(after, queryIDToStmt)
			src.FillQueryResultStmtsFromIndex(before, queryIDToStmt)
			src.FillQueryResultStmtsFromIndex(after, queryIDToStmt)
		}

		diffs := src.DiffReplayAggregate(before, after)
		src.SortDiffPatternStats(diffs, sortBy)
		output = src.FormatDiffAggregate(diffs, sortBy, aggregateTopN, useColor)
		fileOutput = src.FormatDiffAggregateDetailed(diffs, sortBy, aggregateTopN)
	}

	_, _ = fmt.Print(output)

	reportPath := aggregateOutput
	if reportPath == "" && len(args) == 2 {
		reportPath = defaultAggregateDiffOutput
	}
	if reportPath != "" {
		if err := os.MkdirAll(filepath.Dir(reportPath), 0755); err != nil {
			return fmt.Errorf("failed to create output dir for %s: %w", reportPath, err)
		}
		if err := src.WriteFile(reportPath, fileOutput); err != nil {
			return fmt.Errorf("failed to write output file %s: %w", reportPath, err)
		}
		logrus.Infof("Wrote aggregate report to %s", reportPath)
	}

	return nil
}

func guessClientCount(replay string) (int, error) {
	fs, err := os.ReadDir(replay)
	if err != nil {
		return 0, err
	}

	return lo.SumBy(fs, func(f os.DirEntry) int {
		name := f.Name()
		if f.IsDir() || !strings.HasPrefix(name, src.ReplayCustomClientPrefix) || !strings.HasSuffix(name, src.ReplayResultFileExt) {
			return 0
		}
		return 1
	}), nil
}

func diffDumpSQL(replay string) error {
	rstats, err := os.Stat(replay)
	if err != nil {
		return err
	}
	if !rstats.IsDir() {
		return errors.New("replay result should be a directory")
	}
	replayRoot, err := os.OpenRoot(replay)
	if err != nil {
		return err
	}
	defer replayRoot.Close()

	clientCount, err := guessClientCount(replay)
	if err != nil {
		return err
	}

	client2sqls, err := readOriginalDumpSQLs(clientCount)
	if err != nil {
		return err
	}

	return filepath.WalkDir(replay, func(path2 string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path2, src.ReplayResultFileExt) {
			return nil
		}

		client := strings.TrimSuffix(filepath.Base(path2), src.ReplayResultFileExt)
		clientsqls, ok := client2sqls[client]
		if !ok {
			logrus.Errorf("client %s not found in original dump sql, skipping", client)
			return nil
		}

		relPath, err := filepath.Rel(replay, path2)
		if err != nil {
			return err
		}
		f2, err := replayRoot.Open(relPath)
		if err != nil {
			return err
		}
		defer f2.Close()
		scan2 := bufio.NewScanner(f2)

		logrus.Debugf("diffing %s:", path2)

		id2sqls := lo.SliceToMap(clientsqls, func(s *src.ReplaySql) (string, *src.ReplaySql) { return s.QueryId, s })
		if err := diff(&diffReader{id2sqls: id2sqls}, &diffReader{scan: scan2}); err != nil {
			logrus.Errorf("diff %s failed, err: %v", path2, err)
		}
		return nil
	})
}

func readOriginalDumpSQLs(clientCount int) (map[string][]*src.ReplaySql, error) {
	sqls, err := src.FileGlob(originalDumpSQLs)
	if err != nil {
		return nil, err
	}

	client2sqls := make(map[string][]*src.ReplaySql, 10240)
	for _, originalDumpSQL := range sqls {
		f, err := os.Open(originalDumpSQL)
		if err != nil {
			return nil, err
		}
		//nolint:revive
		defer f.Close()

		client2sqls_, _, _, err := src.DecodeReplaySqls(
			f,
			make(map[string]struct{}),
			make(map[string]struct{}),
			0, 0,
			clientCount,
		)
		if err != nil {
			return nil, err
		}

		for client, sqls := range client2sqls_ {
			client2sqls[client] = append(client2sqls[client], sqls...)
		}
	}

	return client2sqls, nil

}

func diffTwoReplays(replay1, replay2 string) error {
	lstats, err := os.Stat(replay1)
	if err != nil {
		return err
	}
	rstats, err := os.Stat(replay2)
	if err != nil {
		return err
	}
	if !lstats.IsDir() || !rstats.IsDir() {
		return errors.New("paths should be both directory")
	}
	replay1Root, err := os.OpenRoot(replay1)
	if err != nil {
		return err
	}
	defer replay1Root.Close()
	replay2Root, err := os.OpenRoot(replay2)
	if err != nil {
		return err
	}
	defer replay2Root.Close()

	return filepath.WalkDir(replay1, func(path1 string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path1, src.ReplayResultFileExt) {
			return nil
		}

		relativePath := strings.TrimPrefix(path1, replay1)
		path2 := filepath.Join(replay2, relativePath)
		relPath1, err := filepath.Rel(replay1, path1)
		if err != nil {
			return err
		}
		relPath2, err := filepath.Rel(replay2, path2)
		if err != nil {
			return err
		}

		f1, err := replay1Root.Open(relPath1)
		if err != nil {
			return err
		}
		defer f1.Close()
		scan1 := bufio.NewScanner(f1)
		f2, err := replay2Root.Open(relPath2)
		if err != nil {
			return err
		}
		defer f2.Close()
		scan2 := bufio.NewScanner(f2)

		logrus.Debugf("diffing %s and %s", path1, path2)

		if err := diff(&diffReader{scan: scan1}, &diffReader{scan: scan2}); err != nil {
			logrus.Errorf("diff %s and %s failed, err: %v", path1, path2, err)
		}
		return nil
	})
}

func diff(scan1, scan2 *diffReader) error {
	id2diff := make(map[string]string)
	for r2 := scan2.get(""); r2 != nil; r2 = scan1.get("") {
		d := diff2{
			r1: scan1.get(r2.QueryId),
			r2: r2,
		}
		if d.r1 == nil {
			id2diff[d.r2.QueryId] = "query id not found in original dump sql or replay1"
			continue
		}
		if d.r1.QueryId != d.r2.QueryId {
			id2diff[d.r2.QueryId] = fmt.Sprintf("query id not match, %s != %s", d.r1.QueryId, d.r2.QueryId)
			continue
		}
		if diffmsg := d.result(); diffmsg != "" {
			id2diff[d.r2.QueryId] = diffmsg
		}
	}

	// print diff result
	for id, diffmsg := range id2diff {
		fmt.Printf("QueryId: %s, %s", color.CyanString(id), diffmsg)
		if len(scan1.id2sqls) > 0 {
			if s, ok := scan1.id2sqls[id]; ok {
				fmt.Printf("Stmt: %s", s.Stmt)
			}
		}
		fmt.Println()
		fmt.Println()
	}
	fmt.Println()
	fmt.Println()

	return nil
}

type diffReader struct {
	scan    *bufio.Scanner // or
	id2sqls map[string]*src.ReplaySql
}

func (r *diffReader) get(queryId string) *src.ReplayResult {
	if r.scan != nil {
		if !r.scan.Scan() {
			return nil
		}
		b := r.scan.Bytes()
		if len(b) == 0 {
			return nil
		}
		result := &src.ReplayResult{}
		if err := json.Unmarshal(b, result); err != nil {
			logrus.Errorf("unmarshal %s failed, err: %v", r.scan.Text(), err)
			return nil
		}
		return result
	}

	if len(r.id2sqls) == 0 {
		return nil
	}

	s, ok := r.id2sqls[queryId]
	if !ok {
		return nil
	}
	return s.ToReplayResult()
}

type diff2 struct {
	r1, r2 *src.ReplayResult
}

func (d *diff2) result() string {
	var result []string

	// NOTE: original dump sql does not have err and return rows
	if d.r1.Err != d.r2.Err {
		r1e := d.r1.Err
		if r1e == "" {
			r1e = "<empty>"
		}
		r2e := d.r2.Err
		if r2e == "" {
			r2e = "<empty>"
		}

		result = append(result, fmt.Sprintf(`err not match:
%s
------
%s`, color.GreenString(r1e), color.RedString(r2e)))
	}

	if len(originalDumpSQLs) == 0 {
		if d.r1.ReturnRows != d.r2.ReturnRows {
			result = append(result, fmt.Sprintf("rows count not match: %s != %s",
				color.GreenString(strconv.Itoa(d.r1.ReturnRows)),
				color.RedString(strconv.Itoa(d.r2.ReturnRows))))
		}
		if d.r1.ReturnRowsHash != d.r2.ReturnRowsHash {
			result = append(result, color.RedString("rows hash not match (count: %d)", d.r1.ReturnRows))
		}
	}

	if d.r2.DurationMs-d.r1.DurationMs > minDurationDiff.Milliseconds() {
		result = append(result, fmt.Sprintf("duration too long: %s vs %s",
			color.GreenString("%dms", d.r1.DurationMs),
			color.RedString("%dms", d.r2.DurationMs)))
	}
	return strings.Join(result, "\n")
}
