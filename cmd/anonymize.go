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
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/Thearas/dodo/src"
)

var AnonymizeConfig = Anonymize{}

type Anonymize struct {
	// common
	Enabled      bool
	Method       string
	IdMinLength  int
	ReserveIds   []string
	HashDictPath string

	// only for anonymize cmd
	File string
}

// anonymizeCmd represents the anonymize command
var anonymizeCmd = &cobra.Command{
	Use:     "anonymize",
	Short:   "Anonymize sqls",
	Aliases: []string{"a"},
	Example: `echo "select * from table1" | dodo anonymize -f -`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	RunE: func(_ *cobra.Command, _ []string) (err error) {
		input, err := src.ReadFileOrStdin(AnonymizeConfig.File)
		if err != nil {
			return err
		}

		SetupAnonymizer()

		sql := src.AnonymizeSql(AnonymizeConfig.Method, "", input)
		_, _ = fmt.Println(sql)

		// store anonymize hash dict
		src.StoreMiniHashDict(AnonymizeConfig.Method, AnonymizeConfig.HashDictPath)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(anonymizeCmd)
	anonymizeCmd.PersistentFlags().SortFlags = false
	anonymizeCmd.Flags().SortFlags = false

	pFlags := anonymizeCmd.PersistentFlags()
	addAnonymizeBaseFlags(pFlags, true)
	pFlags.MarkHidden("anonymize")

	flags := anonymizeCmd.Flags()
	flags.StringVarP(&AnonymizeConfig.File, "file", "f", "", "File path to anonymize sqls, '-' for reading from stdin")
	anonymizeCmd.MarkFlagRequired("file")
}

func addAnonymizeBaseFlags(pFlags *pflag.FlagSet, defaultEnabled bool) {
	pFlags.BoolVar(&AnonymizeConfig.Enabled, "anonymize", defaultEnabled, "Anonymize sqls")
	pFlags.StringSliceVar(&AnonymizeConfig.ReserveIds, "anonymize-reserve-ids", nil, "Skip anonymization for these ids, usually database names")
	pFlags.StringVar(&AnonymizeConfig.Method, "anonymize-method", "minihash", "Anonymize method, hash or minihash")
	pFlags.IntVar(&AnonymizeConfig.IdMinLength, "anonymize-id-min-length", 3, "Skip anonymization for id which length is less than this value, only for hash method")
	pFlags.StringVar(&AnonymizeConfig.HashDictPath, "anonymize-minihash-dict", "./dodo_hashdict.yaml", "Hash dict file path for minihash method")
}

func SetupAnonymizer() {
	src.SetupAnonymizer(
		AnonymizeConfig.Method,
		AnonymizeConfig.HashDictPath,
		AnonymizeConfig.IdMinLength,
		AnonymizeConfig.ReserveIds...,
	)
}

func AnonymizeStats(s []*src.TableStats) []*src.TableStats {
	// deepcopy
	stats := []*src.TableStats{}
	if err := json.Unmarshal(src.MustJsonMarshal(s), &stats); err != nil {
		panic("unreachable")
	}

	for _, t := range stats {
		if t == nil {
			continue
		}
		t.Name = src.Anonymize(AnonymizeConfig.Method, t.Name)
		for _, c := range t.Columns {
			c.Name = src.Anonymize(AnonymizeConfig.Method, c.Name)
		}
	}

	return stats
}

func AnonymizeSQL(identifier, sql string) string {
	return src.AnonymizeSql(AnonymizeConfig.Method, identifier, sql)
}
