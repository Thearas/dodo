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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var ReplayConfig = Replay{}

type Replay struct {
	Cluster         string
	ReplayFile      string
	ReplayResultDir string
	Users_          []string
	From_, To_      string
	ClientCount     int
	Speed           float32
	MaxHashRows     int
	MaxConnIdleTime time.Duration

	DBs      map[string]struct{}
	Users    map[string]struct{}
	From, To int64

	Clean bool
}

// replayCmd represents the replay command
var replayCmd = &cobra.Command{
	Use:     "replay",
	Short:   "Replay queries from dump file",
	Aliases: []string{"r"},
	Example: "dodo replay -f /path/to/dump.sql",
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if err := completeReplayConfig(); err != nil {
			return err
		}
		if ReplayConfig.Clean {
			if err := cleanFile(ReplayConfig.ReplayResultDir, true); err != nil {
				return err
			}
		}

		return replay(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(replayCmd)
	replayCmd.PersistentFlags().SortFlags = false
	replayCmd.Flags().SortFlags = false

	pFlags := replayCmd.PersistentFlags()
	pFlags.StringVarP(&ReplayConfig.ReplayFile, "file", "f", "", "Replay queries from dump file")
	pFlags.StringVarP(&ReplayConfig.Cluster, "cluster", "c", "", "Replay queries on the cluster")
	pFlags.StringVar(&ReplayConfig.ReplayResultDir, "result-dir", "", "Replay result directory, default is '<output-dir>/replay'")
	pFlags.StringSliceVar(&ReplayConfig.Users_, "users", []string{}, "Replay queries from these users")
	pFlags.StringVar(&ReplayConfig.From_, "from", "", "Replay queries from this time, like '2006-01-02 15:04:05'")
	pFlags.StringVar(&ReplayConfig.To_, "to", "", "Replay queries to this time, like '2006-01-02 16:04:05'")
	pFlags.IntVar(&ReplayConfig.ClientCount, "client-count", 0, "Set replay client count")
	pFlags.Float32Var(&ReplayConfig.Speed, "speed", 1.0, "Replay speed, like 0.5, 2, 4, ...")
	pFlags.IntVar(&ReplayConfig.MaxHashRows, "max-hash-rows", 0, "Number of query return rows to hash, useful when diff replay result")
	pFlags.DurationVar(
		&ReplayConfig.MaxConnIdleTime,
		"max-conn-idle-time",
		5*time.Second,
		"Max idle duration of a replay client connection, <= 0 means unlimited",
	)

	flags := replayCmd.Flags()
	flags.BoolVar(&ReplayConfig.Clean, "clean", false, "Clean previous replay result")
}

func completeReplayConfig() (err error) {
	if ReplayConfig.ReplayFile == "" {
		return errors.New("replay file is required, please use --file flag")
	}
	if ReplayConfig.ReplayResultDir == "" {
		ReplayConfig.ReplayResultDir = filepath.Join(GlobalConfig.OutputDir, "replay")
	}

	var t time.Time
	if ReplayConfig.From_ != "" {
		t, err = time.Parse(time.DateTime, ReplayConfig.From_)
		if err != nil {
			return err
		}
		ReplayConfig.From = t.UnixMilli()
	}
	if ReplayConfig.To_ != "" {
		t, err = time.Parse(time.DateTime, ReplayConfig.To_)
		if err != nil {
			return err
		}
		ReplayConfig.To = t.UnixMilli()
	}

	if ReplayConfig.Speed <= 0 {
		return errors.New("replay speed must be > 0")
	}

	ReplayConfig.DBs = lo.SliceToMap(GlobalConfig.DBs, func(s string) (string, struct{}) { return s, struct{}{} })
	ReplayConfig.Users = lo.SliceToMap(ReplayConfig.Users_, func(s string) (string, struct{}) { return s, struct{}{} })

	return nil
}

func replay(ctx context.Context) error {
	f, err := os.Open(ReplayConfig.ReplayFile)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewScanner(f)
	buf.Buffer(make([]byte, 0, 10*1024*1024), 10*1024*1024)

	logrus.Debugf("replay file %s with filter, db: %v, user: %v, from: %s, to: %s",
		ReplayConfig.ReplayFile,
		ReplayConfig.DBs,
		ReplayConfig.Users,
		ReplayConfig.From_, ReplayConfig.To_,
	)

	// TODO: better to use connection -> sqls, but no connection id in audit log yet
	client2Sqls, minTs, count, err := src.DecodeReplaySqls(
		buf,
		ReplayConfig.DBs,
		ReplayConfig.Users,
		ReplayConfig.From,
		ReplayConfig.To,
		ReplayConfig.ClientCount,
	)
	if err != nil {
		return err
	}
	clientSqls := lo.MapToSlice(client2Sqls, func(k string, v []*src.ReplaySql) src.ClientSqls {
		return src.ClientSqls{Client: k, Sqls: v}
	})
	if len(clientSqls) == 0 {
		return fmt.Errorf("no SQLs found in replay file: %s", ReplayConfig.ReplayFile)
	}
	logrus.Infoln("Found", count, "replay sql(s)")

	if GlobalConfig.DryRun {
		return nil
	}

	if err := os.MkdirAll(ReplayConfig.ReplayResultDir, 0755); err != nil {
		return err
	}

	return src.ReplaySqls(
		ctx,
		GlobalConfig.DBHost, GlobalConfig.DBPort, GlobalConfig.DBUser, GlobalConfig.DBPassword, GlobalConfig.Catalog, ReplayConfig.Cluster,
		ReplayConfig.ReplayResultDir, clientSqls, ReplayConfig.Speed, ReplayConfig.MaxHashRows, ReplayConfig.MaxConnIdleTime,
		minTs, GlobalConfig.Parallel,
	)
}
