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
	"bufio"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Thearas/dodo/src"
	"github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/parser"
)

// GendataConfig holds the configuration values
var GendataConfig = Gendata{}

// Gendata holds the configuration for the gendata command
type Gendata struct {
	DDL           string
	OutputDataDir string
	GenConf       string
	NumRows       int
	RowsPerFile   int
	LLM           string
	LLMApiKey     string
	Query         string
	Prompt        string

	genFromDDLs []string
}

// gendataCmd represents the gendata command
var gendataCmd = &cobra.Command{
	Use:   "gendata",
	Short: "Generates CSV data based on DDL and stats files.",
	Long: `Gendata command reads table structures from DDL (.table.sql) files and table statistics files (.stats.yaml) to generate fake CSV data.

Example:
  dodo gendata --dbs db1,db2
  dodo gendata --dbs db1 --tables t1,t2 --rows 500 --ddl output/ddl/
  dodo gendata --ddl create.table.sql
  dodo gendata --dbs db1 --tables t1,t2 \
	--llm 'deepseek-chat' --llm-api-key 'sk-xxx' \
  	-q 'select * from t1 join t2 on t1.a = t2.b where t1.c IN ("a", "b", "c") and t2.d = 1'`,
	Aliases: []string{"g"},
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) (err error) {
		ctx := cmd.Context()

		if err := completeGendataConfig(); err != nil {
			return err
		}
		GlobalConfig.Parallel = min(GlobalConfig.Parallel, len(GendataConfig.genFromDDLs))

		logrus.Infof("Generate data for %d table(s), parallel: %d\n", len(GendataConfig.genFromDDLs), GlobalConfig.Parallel)
		if len(GendataConfig.genFromDDLs) == 0 {
			return nil
		}

		// 1. Construct table generators
		var (
			tableGens []*src.TableGen
			tables    = make([]string, len(GendataConfig.genFromDDLs))
			statss    = make([]*src.TableStats, len(GendataConfig.genFromDDLs))
		)
		for i, ddlFile := range GendataConfig.genFromDDLs {
			logrus.Debugf("generating data to %s ...\n", strings.TrimSuffix(ddlFile, ".table.sql"))

			ddl, err := src.ReadFileOrStdin(ddlFile)
			if err != nil {
				return err
			}
			stats, err := findTableStats(ddlFile)
			if err != nil {
				return err
			}
			tables[i] = ddl
			statss[i] = stats
		}
		// anonymize
		query := GendataConfig.Query
		rawTables := tables
		if AnonymizeConfig.Enabled {
			SetupAnonymizer()
			tables = lo.Map(tables, func(t string, i int) string { return AnonymizeSQL(GendataConfig.genFromDDLs[i], t) })
			statss = AnonymizeStats(statss)
			query = AnonymizeSQL("query", query)
		}
		// send to LLM
		useLLM := GendataConfig.GenConf == "" && GendataConfig.LLM != ""
		if useLLM {
			genconfPath := filepath.Join(GlobalConfig.DodoDataDir, "gendata.yaml")
			logrus.Infof("Generating config '%s' via LLM model: %s, with anonymization: %v\n", genconfPath, GendataConfig.LLM, AnonymizeConfig.Enabled)

			genconf, err := src.LLMGendataConfig(
				ctx,
				GendataConfig.LLMApiKey, "", GendataConfig.LLM, GendataConfig.Prompt,
				tables, lo.FilterMap(statss, func(s *src.TableStats, _ int) (string, bool) { return string(src.MustYamlMarshal(s)), s != nil }),
				[]string{query},
			)
			if err != nil {
				logrus.Errorf("Failed to create gendata config via LLM %s\n", GendataConfig.LLM)
				return err
			}

			// store gendata.yaml
			if err := os.MkdirAll(GlobalConfig.DodoDataDir, 0755); err != nil {
				return err
			}
			if err := src.WriteFile(genconfPath, genconf); err != nil {
				logrus.Errorf("Failed to write gendata config to %s\n", genconfPath)
				return err
			}
			if !src.Confirm(fmt.Sprintf("Using LLM output config: '%s', please check it before going on", genconfPath)) {
				logrus.Infoln("Aborted")
				return nil
			}
			GendataConfig.GenConf = genconfPath
		}

		// 2. Setup generator
		if err := generator.Setup(GendataConfig.GenConf); err != nil {
			return err
		}
		for i, ddlFile := range GendataConfig.genFromDDLs {
			// set streamload column mapping to the unanonymized version
			streamloadCols := []string{}
			if AnonymizeConfig.Enabled {
				streamloadCols, err = parser.GetTableCols(ddlFile, rawTables[i])
				if err != nil {
					return fmt.Errorf("failed to get columns for table %s: %v", rawTables[i], err)
				}
			}

			tg, err := src.NewTableGen(ddlFile, tables[i], statss[i], GendataConfig.NumRows, streamloadCols)
			if err != nil {
				return err
			}

			tableGens = append(tableGens, tg)
		}

		if GlobalConfig.DryRun {
			return nil
		} else if len(tableGens) == 0 {
			logrus.Infoln("No table to generate.")
			return nil
		}
		// store anonymize hash dict
		if AnonymizeConfig.Enabled {
			src.StoreMiniHashDict(AnonymizeConfig.Method, AnonymizeConfig.HashDictPath)
		}

		// 3. Generate data according to table ref dependence
		var (
			allTables = lo.Map(tableGens, func(tg *src.TableGen, _ int) string { return tg.Name })
			refTables = lo.Uniq(lo.Flatten(lo.Map(tableGens, func(tg *src.TableGen, _ int) []string { return slices.Collect(maps.Keys(tg.RefToTable)) })))

			refNotFoundTable = lo.Without(refTables, allTables...)
		)
		if len(refNotFoundTable) > 0 {
			return fmt.Errorf("these tables are being ref, please generate them together: %v", refNotFoundTable)
		}

		totalTableGens := len(allTables)
		for range totalTableGens {
			if len(tableGens) == 0 {
				return nil
			}

			zeroRefTableGens := lo.Filter(tableGens, func(tg *src.TableGen, _ int) bool { return len(tg.RefToTable) == 0 })
			tableGens = lo.Filter(tableGens, func(tg *src.TableGen, _ int) bool { return len(tg.RefToTable) > 0 })

			// check ref deadlock
			if len(zeroRefTableGens) == 0 {
				remainTable2Refs := lo.SliceToMap(tableGens, func(tg *src.TableGen) (string, []string) {
					return tg.Name, slices.Collect(maps.Keys(tg.RefToTable))
				})
				return fmt.Errorf("table refs deadlock: %v", remainTable2Refs)
			}

			// Generate the tables with zero ref.
			g := src.ParallelGroup(GlobalConfig.Parallel)
			for _, tg := range zeroRefTableGens {
				logrus.Infof("Generating data for table: %s, rows: %d\n", tg.Name, tg.Rows)
				g.Go(func() error {
					rowsPerFile := min(GendataConfig.RowsPerFile, tg.Rows)
					for i, end := range lo.RangeWithSteps(0, tg.Rows+rowsPerFile, rowsPerFile) {
						rows := rowsPerFile
						if end >= tg.Rows {
							rows = tg.Rows % rowsPerFile
						}
						if rows == 0 {
							break
						}
						o, err := createOutputGenDataWriter(tg.DDLFile, i+1)
						if err != nil {
							return err
						}

						w := bufio.NewWriterSize(o, 256*1024)
						if err := tg.GenCSV(w, rows); err != nil {
							_ = o.Close()
							return err
						}
						if err := w.Flush(); err != nil {
							_ = o.Close()
							return err
						}
						_ = o.Close()
					}
					logrus.Infof("Finish generating data for table: %s\n", tg.Name)
					return nil
				})

				// the ref table data is generating, remove from all waiting tableGens
				lo.ForEach(tableGens, func(g *src.TableGen, _ int) { g.RemoveRefTable(tg.Name) })
			}

			if err := g.Wait(); err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(gendataCmd)
	gendataCmd.PersistentFlags().SortFlags = false
	gendataCmd.Flags().SortFlags = false

	pFlags := gendataCmd.PersistentFlags()
	pFlags.StringVarP(&GendataConfig.DDL, "ddl", "d", "", "Directory or file containing DDL (.table.sql) and stats (.stats.yaml) files")
	pFlags.StringVarP(&GendataConfig.OutputDataDir, "output-data-dir", "o", "", "Directory where CSV files will be generated")
	pFlags.IntVarP(&GendataConfig.NumRows, "rows", "r", 0, fmt.Sprintf("Number of rows to generate per table (default %d)", src.DefaultGenRowCount))
	pFlags.IntVar(&GendataConfig.RowsPerFile, "rows-per-file", 20_000, "Number of rows to store in a CSV file")
	pFlags.StringVarP(&GendataConfig.GenConf, "genconf", "c", "", "Generator config file")
	pFlags.StringVarP(&GendataConfig.LLM, "llm", "l", "", "LLM model to use, e.g. 'deepseek-code', 'deepseek-chat', 'deepseek-reasoner'")
	pFlags.StringVarP(&GendataConfig.LLMApiKey, "llm-api-key", "k", "", "LLM API key")
	pFlags.StringVarP(&GendataConfig.Query, "query", "q", "", "SQL query file to generate data, only can be used when LLM is on")
	pFlags.StringVarP(&GendataConfig.Prompt, "prompt", "p", "", "Additional user prompt for LLM")
	addAnonymizeBaseFlags(pFlags, false)

	gendataCmd.RegisterFlagCompletionFunc("llm", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"deepseek-reasoner", "deepseek-chat"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
	})
}

// completeGendataConfig validates and completes the gendata configuration
func completeGendataConfig() (err error) {
	if GendataConfig.DDL == "" {
		GendataConfig.DDL = filepath.Join(GlobalConfig.OutputDir, "ddl")
	}
	if GendataConfig.OutputDataDir == "" {
		GendataConfig.OutputDataDir = filepath.Join(GlobalConfig.OutputDir, "gendata")
	}

	if GendataConfig.LLM != "" {
		if GendataConfig.LLMApiKey == "" {
			return errors.New("--llm-api-key must be provided when --llm is specified")
		}
	} else if GendataConfig.Query != "" {
		return errors.New("--query can only be used when --llm is specified")
	}

	// if --ddl are sql file(s), not need --dbs or --tables
	ddlFiles, _ := src.FileGlob(strings.Split(GendataConfig.DDL, ","))
	var isFile bool
	if len(ddlFiles) > 0 {
		f, err := os.Stat(ddlFiles[0])
		isFile = err == nil && !f.IsDir()
	}
	if isFile {
		GendataConfig.genFromDDLs = ddlFiles
		return nil
	}

	if err := completeDBTables(); err != nil {
		return err
	}

	ddls := []string{}
	if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
			fmatch := filepath.Join(GendataConfig.DDL, fmt.Sprintf("%s.*.table.sql", db))
			tableddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed\n", db, fmatch)
				return err
			}
			ddls = append(ddls, tableddls...)
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			tableddl := filepath.Join(GendataConfig.DDL, fmt.Sprintf("%s.table.sql", table))
			ddls = append(ddls, tableddl)
		}
	}
	GendataConfig.genFromDDLs = ddls

	return nil
}

func findTableStats(ddlFileName string) (*src.TableStats, error) {
	ddlFileDir := filepath.Dir(ddlFileName)
	ddlFileName = filepath.Base(ddlFileName)

	db, table, isTable := dbtableFromFileName(ddlFileName)
	isDumpTable := db != "" && isTable
	if !isDumpTable {
		return nil, nil
	}

	dbStatsFile := filepath.Join(ddlFileDir, db+".stats.yaml")
	b, err := os.ReadFile(dbStatsFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logrus.Debugf("stats file '%s' not found for db '%s'\n", dbStatsFile, db)
			return nil, nil
		}
		return nil, err
	}

	dbstats := &src.DBSchema{}
	if err := yaml.Unmarshal(b, dbstats); err != nil {
		return nil, err
	}

	for _, tableStats := range dbstats.Stats {
		if tableStats.Name != table || len(tableStats.Columns) == 0 || tableStats.RowCount <= 0 {
			continue
		}
		if tableStats.Columns[0].Method != "FULL" {
			logrus.Warnf("Table stats '%s.%s' is '%s' in '%s', better to dump with '--analyze' or run 'ANALYZE DATABASE `%s` WITH SYNC' before dumping\n",
				db, table,
				tableStats.Columns[0].Method,
				dbStatsFile,
				db,
			)
		}
		return tableStats, nil
	}

	logrus.Warnf("Table stats '%s.%s' not found in '%s', better to dump with '--analyze' or run 'ANALYZE DATABASE `%s` WITH SYNC' before dumping\n",
		db, table,
		dbStatsFile,
		db,
	)
	return nil, nil
}

func createOutputGenDataWriter(ddlFileName string, idx int) (*os.File, error) {
	ddlFileName = filepath.Base(ddlFileName)
	dir := filepath.Join(GendataConfig.OutputDataDir, strings.TrimSuffix(strings.TrimSuffix(ddlFileName, ".table.sql"), ".sql"))
	if idx == 1 {
		// delete previous gen files
		logrus.Debugf("Deleting previous generated data files in %s\n", dir)
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	file := filepath.Join(dir, fmt.Sprintf("%d.csv", idx))
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		logrus.Fatalln("Can not open output data file:", file, ", err:", err)
	}
	return f, nil
}
