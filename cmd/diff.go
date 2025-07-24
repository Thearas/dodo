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
)

// diffCmd represents the diff command
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Diff a replay result with another or the original dump sql",
	Example: `dodo diff replay1/ replay2/
dodo diff --original-sqls dump.sql replay1/`,
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

	flags := diffCmd.Flags()
	flags.BoolVar(&noColor, "no-color", false, "Disable color output")
	flags.DurationVar(&minDurationDiff, "min-duration-diff", 100*time.Millisecond, "Print diff if duration difference is greater than this value")
	flags.StringSliceVar(&originalDumpSQLs, "original-sqls", nil, "Diff with original dump sql instead of another replay result")
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

		f2, err := os.Open(path2)
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
			bufio.NewScanner(f),
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

	return filepath.WalkDir(replay1, func(path1 string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path1, src.ReplayResultFileExt) {
			return nil
		}

		relativePath := strings.TrimPrefix(path1, replay1)
		path2 := filepath.Join(replay2, relativePath)

		f1, err := os.Open(path1)
		if err != nil {
			return err
		}
		defer f1.Close()
		scan1 := bufio.NewScanner(f1)
		f2, err := os.Open(path2)
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
