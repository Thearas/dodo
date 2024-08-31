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
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/cwriter"
	"go.yaml.in/yaml/v4"

	"github.com/Thearas/dodo/src"
	"github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/parser"
)

const MaxGenconfs = 128 // Maximum number of genconf in a genconf YAML file

// GendataConfig holds the configuration values
var GendataConfig = Gendata{}

// Gendata holds the configuration for the gendata command
type Gendata struct {
	Print             bool
	Progress          bool
	DDL               string
	OutputDataDir     string
	OutputFormat      string
	LineDelimiter     string
	GenConf           string
	NumRows           int64
	RowsPerFile       int64
	RowsPerInsert     int64
	EnableColumnStats bool

	LLM       string
	LLMApiKey string
	Query     string
	Prompt    string

	Simulation bool

	genFromDDLs []string
	getColsDB   *sqlx.DB
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
	--llm 'deepseek-v4-pro' --llm-api-key 'sk-xxx' \
  	-q 'select * from t1 join t2 on t1.a = t2.b where t1.c IN ("a", "b", "c") and t2.d = 1'`,
	Aliases: []string{"g"},
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) (err error) {
		ctx := cmd.Context()

		if err := completeGendataConfig(ctx); err != nil {
			return err
		}
		if GendataConfig.Print {
			// prevent out of order
			GlobalConfig.Parallel = 1
		}

		logrus.Infof("Generate data for %d table(s), parallel: %d", len(GendataConfig.genFromDDLs), GlobalConfig.Parallel)
		if len(GendataConfig.genFromDDLs) == 0 {
			return nil
		}

		// if catalog is not 'internal', need connect to db to get columns
		if !src.IsInternalCatalog(GlobalConfig.Catalog) {
			GendataConfig.getColsDB, err = connectDBWithoutDBName()
			if err != nil {
				return err
			}
			// defer GendataConfig.getColsDB.Close()
		}

		// 1. Find ddl and column stats.
		tables, statss, err := loadDDLAndStats(ctx, GendataConfig.genFromDDLs)
		if err != nil {
			return err
		}

		// 2. LLM gen configuration.
		if GendataConfig.GenConf == "" && GendataConfig.LLM != "" {
			genconfPath, err := generateLLMConfig(ctx, tables, statss)
			if err != nil {
				return err
			}
			if genconfPath == "" {
				// User aborted
				return nil
			}
			GendataConfig.GenConf = genconfPath
		}

		// 3. Run data generation.
		return MRunGenerateData(ctx, tables, statss)
	},
}

func init() {
	rootCmd.AddCommand(gendataCmd)
	gendataCmd.PersistentFlags().SortFlags = false
	gendataCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(gendataCmd, "dodo-data-dir", "output", "dry-run", "parallel", "host", "port", "user", "password", "catalog", "dbs", "tables")

	pFlags := gendataCmd.PersistentFlags()
	pFlags.StringVarP(&GendataConfig.DDL, "ddl", "d", "", "Directory or file containing DDL (.table.sql) and stats (.stats.yaml) files, default is 'output/ddl/'")
	pFlags.Int64VarP(&GendataConfig.NumRows, "rows", "r", 0, fmt.Sprintf("Number of rows to generate per table (default %d)", src.DefaultGenRowCount))
	pFlags.Int64Var(&GendataConfig.RowsPerFile, "rows-per-file", 200_000, "Number of rows to store in a single CSV file")
	pFlags.Int64Var(&GendataConfig.RowsPerInsert, "rows-per-insert", 1000, "Number of rows of one insert, only works when '--output-format=insert'")
	pFlags.StringVarP(&GendataConfig.GenConf, "genconf", "c", "", "Generator config file")
	pFlags.StringVarP(&GendataConfig.LLM, "llm", "l", "", "LLM model to use, e.g. 'deepseek-v4-flash', 'deepseek-v4-pro'")
	pFlags.StringVarP(&GendataConfig.LLMApiKey, "llm-api-key", "k", "", "LLM API key")
	pFlags.StringVarP(&GendataConfig.Query, "query", "q", "", "SQL query file to generate data, only can be used when LLM is on")
	pFlags.StringVarP(&GendataConfig.Prompt, "prompt", "p", "", "Additional user prompt for LLM")
	pFlags.StringVarP(&GendataConfig.OutputDataDir, "output-data-dir", "o", "", "Directory where CSV files will be generated")
	pFlags.StringVar(&GendataConfig.OutputFormat, "output-format", "csv", "Output data format, one of csv and insert")
	pFlags.StringVar(&GendataConfig.LineDelimiter, "line-delimiter", "\n", "Specify the line delimiter in CSV file, e.g. ☉")
	pFlags.BoolVar(&generator.NeedCastInInsertVal, "output-insert-cast", false, "Whether to output insert statements with value casted to target column type")
	pFlags.BoolVar(&GendataConfig.EnableColumnStats, "enable-column-stats", true, "Enable column statistics for data generation, when disabled only table row count is used")
	pFlags.BoolVar(&GendataConfig.Print, "print", false, "Output data to STDOUT instead of files")
	pFlags.BoolVar(&GendataConfig.Progress, "progress", cwriter.IsTerminal(int(os.Stderr.Fd())), "Enable progress output")

	pFlags.MarkHidden("output-insert-cast")

	gendataCmd.RegisterFlagCompletionFunc("output-format", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"csv", "insert"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
	})
	gendataCmd.RegisterFlagCompletionFunc("llm", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"deepseek-v4-pro", "deepseek-v4-flash"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
	})
}

// completeGendataConfig validates and completes the gendata configuration
func completeGendataConfig(ctx context.Context) (err error) {
	if GendataConfig.RowsPerInsert <= 0 {
		GendataConfig.RowsPerInsert = 1000
	}
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

	// When --query is provided with --llm, extract tables from the query using LLM
	if GendataConfig.Query != "" && len(GlobalConfig.Tables) == 0 {
		defaultDB := ""
		if len(GlobalConfig.DBs) == 1 {
			defaultDB = GlobalConfig.DBs[0]
		}
		extractedTables, err := extractTablesFromQuery(ctx, defaultDB)
		if err != nil {
			return fmt.Errorf("failed to extract tables from query: %w", err)
		}
		if len(extractedTables) > 0 {
			// Update GlobalConfig with extracted tables
			GlobalConfig.Tables = extractedTables
			logrus.Infof("Extracted %d table(s) from query: %v", len(extractedTables), GlobalConfig.Tables)
		}
	}

	if err := completeDBTables(); err != nil {
		return err
	}

	genByLLMWithQuery := GendataConfig.LLM != "" && GendataConfig.Query != ""

	ddls := []string{}
	if len(GlobalConfig.Tables) == 0 {
		for _, db := range GlobalConfig.DBs {
		autodumped:
			fmatch := filepath.Join(GendataConfig.DDL, fmt.Sprintf("%s.*.table.sql", db))
			tableddls, err := src.FileGlob([]string{fmatch})
			if err != nil {
				logrus.Errorf("Get db '%s' ddls in '%s' failed", db, fmatch)
				return err
			}
			if len(tableddls) == 0 {
				// auto dump schemas if not found
				logrus.Infof("No local DDLs found for db '%s' at '%s', attempting to dump", db, fmatch)
				var count int
				if count, err = dumpSchemas(ctx, GendataConfig.DDL, map[string][]string{db: {}}, true); err != nil {
					return err
				}
				if count <= 0 {
					logrus.Errorf("No tables found for db '%s'", db)
					continue
				}
				goto autodumped
			}
			logrus.Debugf("Found %d local DDLs for db '%s'", len(tableddls), db)
			ddls = append(ddls, tableddls...)
		}
	} else {
		for _, table := range GlobalConfig.Tables {
			// auto dump schemas if not found
			tableddl := filepath.Join(GendataConfig.DDL, fmt.Sprintf("%s.table.sql", table))
			if _, err := os.Stat(tableddl); errors.Is(err, os.ErrNotExist) {
				if genByLLMWithQuery {
					tableddls, err := src.FileGlob([]string{filepath.Join(GendataConfig.DDL, fmt.Sprintf("%s.*view.sql", table))})
					if err != nil {
						logrus.Errorf("Get view '%s' ddl failed", table)
						return err
					}
					if len(tableddls) > 0 {
						logrus.Infof("Found local DDL view(s) for '%s' at '%v'", table, tableddls)
						ddls = append(ddls, tableddls...)
						continue
					}
				}
				logrus.Infof("No local DDL found for table '%s' at '%s', attempting to dump", table, tableddl)
				dbtable := strings.SplitN(table, ".", 2)
				var count int
				if count, err = dumpSchemas(ctx, GendataConfig.DDL, map[string][]string{dbtable[0]: {dbtable[1]}}, true); err != nil {
					return err
				}
				if count <= 0 {
					logrus.Errorf("Table '%s' not found", table)
					continue
				}
			}
			ddls = append(ddls, tableddl)
		}
	}
	GendataConfig.genFromDDLs = ddls

	return nil
}

// extractTablesFromQuery uses LLM to extract table names from the query
func extractTablesFromQuery(ctx context.Context, defaultDB string) ([]string, error) {
	model := strings.ToLower(GendataConfig.LLM)
	extractTableModel := os.Getenv("DODO_EXTRACT_TABLE_MODEL")
	if extractTableModel != "" {
		model = extractTableModel
	}

	if !strings.HasPrefix(model, "deepseek") {
		return nil, errors.New("--llm must start with 'deepseek', e.g. 'deepseek-v4-flash', 'deepseek-v4-pro'")
	}
	baseURL := "https://api.deepseek.com/beta"

	// Read query from file if it's a file path
	query := GendataConfig.Query
	if content, err := src.ReadFileOrStdin(query); err == nil {
		query = content
	}

	logrus.Infof("Extracting tables from query via LLM model: %s", model)
	tables, err := src.LLMExtractTables(ctx, GendataConfig.LLMApiKey, baseURL, model, defaultDB, []string{query})
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(os.Stdout) // newline after LLM output

	return tables, nil
}

// parseDBTableFromFileName extracts db, table name and type from filename.
// Format: <db>.<table>.{table|view|materialized_view}.sql
// Example: "mydb.users.table.sql" -> ("mydb", "users", "table")
func parseDBTableFromFileName(filePath string) (db, table, typ string) {
	// Get base name without path
	base := filePath
	if idx := strings.LastIndex(filePath, "/"); idx != -1 {
		base = filePath[idx+1:]
	}

	// Remove .sql suffix
	base = strings.TrimSuffix(base, ".sql")

	// Split by "."
	parts := strings.Split(base, ".")
	if len(parts) < 3 {
		return "", "", ""
	}

	// Last part is the type (table/view/materialized_view)
	typ = parts[len(parts)-1]
	if typ != "table" && typ != "view" && typ != "materialized_view" {
		return "", "", ""
	}

	// First part is db, middle parts (joined) are table name
	db = parts[0]
	table = strings.Join(parts[1:len(parts)-1], ".")

	return db, table, typ
}

// prepareLLMTableInputs prepares table info for LLM from files, content, and stats.
// All parsing and file reading happens here - src/llm.go receives pre-parsed data.
func prepareLLMTableInputs(ddlFiles, ddlContents []string, statss []*src.TableStats) []src.LLMTableInput {
	inputs := make([]src.LLMTableInput, len(ddlFiles))
	for i, filePath := range ddlFiles {
		db, name, typ := parseDBTableFromFileName(filePath)
		if typ == "" {
			typ = "table"
		}

		// If filename doesn't match expected format, parse from DDL content
		ddlContent := ""
		if i < len(ddlContents) {
			ddlContent = ddlContents[i]
		}
		if name == "" && ddlContent != "" {
			if parsedDB, parsedName, err := parser.GetTableName(filePath, ddlContent); err == nil {
				db = parsedDB
				name = parsedName
			}
		}

		// Convert stats to YAML string
		var statsContent string
		if i < len(statss) && statss[i] != nil {
			statsContent = string(src.MustYamlMarshal(statss[i]))
		}

		inputs[i] = src.LLMTableInput{
			DB:           db,
			Name:         name,
			Type:         typ,
			DDLContent:   ddlContent,
			StatsContent: statsContent,
		}
	}
	return inputs
}

// generateLLMConfig generates a gendata config file using LLM.
// Returns the path to the generated config file, or empty string if user aborted.
func generateLLMConfig(ctx context.Context, tables []string, statss []*src.TableStats) (string, error) {
	genconfPath := filepath.Join(GlobalConfig.DodoDataDir, "gendata.yaml")

	// Prepare all table info upfront (parsing happens here, not in src/llm.go)
	llmTables := prepareLLMTableInputs(GendataConfig.genFromDDLs, tables, statss)

	// Build queries or table list for LLM
	var sqls []string
	if GendataConfig.Query != "" {
		sqls = []string{GendataConfig.Query}
	}

	model := strings.ToLower(GendataConfig.LLM)
	if !strings.HasPrefix(model, "deepseek") {
		return "", errors.New("--llm must start with 'deepseek', e.g. 'deepseek-v4-flash', 'deepseek-v4-pro'")
	}
	baseURL := "https://api.deepseek.com/beta"

	var (
		genconf string
		err     error
	)

	genconf, err = src.LLMGendataConfig(
		ctx,
		GendataConfig.LLMApiKey, baseURL, model, GendataConfig.Prompt,
		llmTables,
		sqls,
		runGendataSimulation,
	)

	if err != nil {
		logrus.Errorf("Failed to create gendata config via LLM %s", GendataConfig.LLM)
		return "", err
	}

	// Store gendata.yaml
	if err := os.MkdirAll(GlobalConfig.DodoDataDir, 0755); err != nil {
		return "", err
	}
	if err := src.WriteFile(genconfPath, genconf); err != nil {
		logrus.Errorf("Failed to write gendata config to %s", genconfPath)
		return "", err
	}

	if !src.Confirm(fmt.Sprintf("Using LLM output config: '%s', please check it before going on", genconfPath)) {
		logrus.Infoln("Aborted")
		return "", nil
	}

	return genconfPath, nil
}

// runGendataSimulation performs a quick validation of a YAML config string.
// It checks for YAML syntax and runs a 2-row data generation to an in-memory writer.
func runGendataSimulation(ctx context.Context, yamlConfig string) error {
	fmt.Fprintln(os.Stderr)
	logrus.Info("Validating LLM-generated YAML config...")

	// 1. Check YAML syntax
	var temp any
	if err := yaml.Unmarshal([]byte(yamlConfig), &temp); err != nil {
		return fmt.Errorf("YAML syntax error: %w", err)
	}

	// 2. Write to a temporary file for SetupGendata
	if err := os.MkdirAll(GlobalConfig.DodoDataDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", GlobalConfig.DodoDataDir, err)
	}
	tmpFile, err := os.CreateTemp(GlobalConfig.DodoDataDir, "gendata-validation-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp file for validation: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(yamlConfig); err != nil {
		return fmt.Errorf("failed to write to temp validation file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp validation file: %w", err)
	}

	// 3. Run a lightweight data generation
	// To do this, we need to get the tables and stats that were used in the original command context.
	// This is a bit tricky as we're in a separate function. We can access the global GendataConfig.
	if len(GendataConfig.genFromDDLs) == 0 {
		return errors.New("cannot run simulation: no DDLs found in the current context")
	}

	tables, statss, err := loadDDLAndStats(ctx, GendataConfig.genFromDDLs)
	if err != nil {
		return fmt.Errorf("simulation failed to load DDLs and stats: %w", err)
	}

	// Store original config values to restore them later
	originalNumRows := GendataConfig.NumRows
	originalGenConf := GendataConfig.GenConf
	originalSimulation := GendataConfig.Simulation
	originalParallel := GlobalConfig.Parallel
	originalLogLevel := logrus.GetLevel()
	originalLogOutput := logrus.StandardLogger().Out

	// Set simulation config
	GendataConfig.NumRows = 10 // Generate only a few rows
	GendataConfig.GenConf = tmpFile.Name()
	GendataConfig.Simulation = true
	GlobalConfig.Parallel = 1
	logrus.SetLevel(logrus.InfoLevel)
	logOutput := &strings.Builder{}
	logrus.SetOutput(logOutput)

	logrus.Debugln("Running 10-row data generation simulation...")
	// We run for multiple rounds (genconfIdx=0), with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}

			// Restore original config after the simulation
			GendataConfig.NumRows = originalNumRows
			GendataConfig.GenConf = originalGenConf
			GendataConfig.Simulation = originalSimulation
			GlobalConfig.Parallel = originalParallel
			logrus.SetLevel(originalLogLevel)
			logrus.SetOutput(originalLogOutput)
		}()
		err = MRunGenerateData(ctx, tables, statss)
	}()

	// We expect a GenconfEndError if there's only one doc in the YAML, which is normal
	if err != nil && !errors.Is(err, &src.GenconfEndError{}) {
		err := errors.Join(errors.New(logOutput.String()), err)
		logrus.Warningf("Data generation simulation encountered an error: %v", err)
		return fmt.Errorf("data generation simulation failed: %w", err)
	}

	return nil
}

func loadDDLAndStats(ctx context.Context, ddlFiles []string) ([]string, []*src.TableStats, error) {
	tables := make([]string, len(ddlFiles))
	statss := make([]*src.TableStats, len(ddlFiles))

	g := src.ParallelGroup(GlobalConfig.Parallel)
	for i, ddlFile := range ddlFiles {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}

			// 1. Read ddl stmt
			ddl, err := src.ReadFileOrStdin(ddlFile)
			if err != nil {
				return fmt.Errorf("failed to read DDL %s: %w", ddlFile, err)
			}
			tables[i] = ddl

			// 2. Read stats, if any
			if GendataConfig.GenConf != "" && !GendataConfig.EnableColumnStats {
				// If genconf is provided, we only load the DDLs and stats for the tables declared in the genconf
				return nil
			}
			stats, err := findTableStats(ddlFile)
			if err != nil {
				return fmt.Errorf("failed to find stats for %s: %w", ddlFile, err)
			}
			if !GendataConfig.EnableColumnStats {
				stats = stripColumnStats(stats)
			}
			statss[i] = stats

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return tables, statss, nil
}

func MRunGenerateData(ctx context.Context, tables []string, statss []*src.TableStats) (err error) {
	// may have multi genconf in one genconf YAML file, separate by '---'
	for i := range MaxGenconfs {
		if err := RunGenerateData(ctx, tables, statss, i); err != nil {
			if errors.Is(err, &src.GenconfEndError{}) {
				return nil
			}
			return err
		}
		logrus.Infof("=== Generation success (round %d) ===", i+1)
		fmt.Fprintln(os.Stderr)
		if GendataConfig.GenConf == "" {
			break
		}
	}
	return nil
}

func RunGenerateData(
	ctx context.Context,
	tables []string,
	statss []*src.TableStats,
	genconfIdx int,
) (err error) {
	// 1. Setup generator
	genconf := GendataConfig.GenConf
	if err := src.SetupGendata(genconf, genconfIdx, GendataConfig.OutputFormat); err != nil {
		if !errors.Is(err, &src.GenconfEndError{}) {
			logrus.Errorf("Failed to read config file '%s': %v", genconf, err)
		}
		return err
	}

	// 2. If genconf declares specific tables, only generate those tables
	genconfTableNames := generator.GetGenconfTableNames()
	genFromDDLs := GendataConfig.genFromDDLs
	if len(genconfTableNames) > 0 {
		genconfTableSet := lo.SliceToMap(genconfTableNames, func(s string) (string, struct{}) { return strings.ToLower(s), struct{}{} })
		var filteredDDLs []string
		var filteredTables []string
		var filteredStats []*src.TableStats
		for i, ddlFile := range genFromDDLs {
			// read from file name first for better performance, if not match then parse the DDL content for table name
			_, tableName, ok := dbtableFromFileName(ddlFile)
			if !ok {
				tableName, _, err = parser.GetTableNameAndCols(ddlFile, tables[i])
				if err != nil {
					continue
				}
			}
			if _, ok := genconfTableSet[strings.ToLower(tableName)]; ok {
				filteredDDLs = append(filteredDDLs, ddlFile)
				filteredTables = append(filteredTables, tables[i])
				filteredStats = append(filteredStats, statss[i])
			}
		}
		genFromDDLs = filteredDDLs
		tables = filteredTables
		statss = filteredStats
		logrus.Infof("Genconf #%d declares %d table(s)", genconfIdx, len(filteredTables))
	}

	// 3. Construct generator for each table
	tableGens := make([]*src.TableGen, 0, len(genFromDDLs))
	for i, ddlFile := range genFromDDLs {
		tableDDL := tables[i]
		stats := statss[i]

		tableName, _, err := parser.GetTableNameAndCols(ddlFile, tableDDL)
		if err != nil {
			if errors.Is(err, parser.ErrNotCreateTable) {
				logrus.Debugf("Skipping non-table DDL: %s", ddlFile)
				continue
			}
			return fmt.Errorf("failed to get columns from create-table sql, err: %v, stmt: %s", err, tableDDL)
		}

		sqlId := ddlFile
		if stats != nil {
			sqlId += "#" + stats.Name
		}

		p := parser.NewParser(sqlId, tableDDL)
		c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
		if !ok {
			logrus.Debugf("Skipping non-table DDL: %s", ddlFile)
			continue
		} else if p.ErrListener.LastErr != nil {
			return fmt.Errorf("SQL parser error for '%s': %w", ddlFile, p.ErrListener.LastErr)
		}
		columns := src.ColumnsFromParsed(c.ColumnDefs().GetCols(), parser.GetUniqueKeyColumns(c))
		if GendataConfig.getColsDB != nil {
			db, _, _ := dbtableFromFileName(ddlFile)
			columns, err = src.ShowColumns(ctx, GendataConfig.getColsDB, db, tableName)
			if err != nil {
				return fmt.Errorf("failed to get columns for table %s: %v", tableName, err)
			}
			// Mark unique key columns from DDL
			uniqueKeyCols := parser.GetUniqueKeyColumns(c)
			if len(uniqueKeyCols) > 0 {
				uniqueKeySet := lo.SliceToMap(uniqueKeyCols, func(s string) (string, struct{}) { return s, struct{}{} })
				for _, col := range columns {
					if _, ok := uniqueKeySet[col.Name]; ok {
						col.Unique = true
					}
				}
			}
		}
		// selects at most one unique key column to auto-assign inc.
		src.PickUniqueIncColumn(columns)

		tg, err := src.NewTableGen(ddlFile, tableName, columns, stats, GendataConfig.NumRows, src.IsInternalCatalog(GlobalConfig.Catalog))
		if err != nil {
			return err
		}

		// Validate that partition columns have YAML config rules
		partitionCols := parser.GetPartitionColumns(c)
		if len(partitionCols) > 0 {
			if err := src.ValidatePartitionColumns(tableName, partitionCols, tg.ColGenRules); err != nil {
				return err
			}
		}

		tg.SetRowsPerInsert(GendataConfig.RowsPerInsert)
		tg.SetLineDelimiter(GendataConfig.LineDelimiter)

		tableGens = append(tableGens, tg)
	}

	if len(tableGens) == 0 {
		logrus.Infoln("No table to generate. Forgot to run `dodo dump` first?")
		return nil
	} else if GlobalConfig.DryRun {
		return nil
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
		for tidx, tg := range zeroRefTableGens {
			var progress *mpb.Bar
			if GendataConfig.Progress {
				// multi progress bars
				progress = src.GenRowProgressBar(tg.Rows, fmt.Sprintf("[%d/%d] %s", tidx+1, totalTableGens, tg.Name))
			} else {
				logrus.Infof("Generating data for table: %s, rows: %d", tg.Name, tg.Rows)
			}

			rowsPerFile := min(GendataConfig.RowsPerFile, tg.Rows)
			for i, end := range lo.RangeWithSteps(0, tg.Rows+rowsPerFile, rowsPerFile) {
				rows := rowsPerFile
				isLast := end >= tg.Rows
				if isLast {
					rows = tg.Rows % rowsPerFile
				}
				if rows == 0 {
					break
				}
				// cancel when ctrl+c
				if ctx.Err() != nil {
					return ctx.Err()
				}
				o, err := createOutputGenDataWriter(tg.DDLFile, genconfIdx, i)
				if err != nil {
					return err
				}

				g.Go(func() error {
					if !GendataConfig.Simulation {
						defer o.Close()
					}

					if i == 0 {
						logrus.Debugf("Generating data for table: %s, rows: %d", tg.Name, tg.Rows)
					}

					var w *bufio.Writer
					if GendataConfig.Simulation {
						w = bufio.NewWriter(io.Discard)
					} else {
						w = bufio.NewWriterSize(o, 256*1024)
					}
					defer w.Flush()
					return tg.Gen(ctx, w, rows, progress)
				})
			}

			// the ref table data is generating, remove from all waiting tableGens
			lo.ForEach(tableGens, func(g *src.TableGen, _ int) { g.RemoveRefTable(tg.Name) })
		}

		if err := g.Wait(); err != nil {
			return err
		}
		src.ShutdownMultiProgressBar()
	}

	return nil
}

// stripColumnStats returns a copy of stats with only RowCount, removing column-level statistics.
// Returns nil if the input stats is nil.
func stripColumnStats(stats *src.TableStats) *src.TableStats {
	if stats == nil {
		return nil
	}
	return &src.TableStats{
		Name:     stats.Name,
		RowCount: stats.RowCount,
	}
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
			logrus.Debugf("stats file '%s' not found for db '%s'", dbStatsFile, db)
			return nil, nil
		}
		return nil, err
	}

	dbstats := &src.DBSchema{}
	if err := yaml.Unmarshal(b, dbstats); err != nil {
		logrus.Warnf("Decode stats file '%s' failed: %v", dbStatsFile, err)
		return nil, nil
	}

	for _, tableStats := range dbstats.Stats {
		if tableStats.Name != table || len(tableStats.Columns) == 0 || tableStats.RowCount <= 0 {
			continue
		}
		if tableStats.Columns[0].Method == "SAMPLE" {
			logrus.Warnf("Will not use table stats of '%s.%s', because it is '%s', better to dump with '--analyze' or just remove all the `method: SAMPLE` lines in '%s'",
				db, table,
				tableStats.Columns[0].Method,
				dbStatsFile,
			)
			return nil, nil
		}
		logrus.Infof("Using table stats of '%s.%s' in '%s'", db, table, dbStatsFile)
		return tableStats, nil
	}

	logrus.Debugf("Table stats of '%s.%s' not found in '%s', better to dump with '--analyze' or run 'ANALYZE TABLE `%s.%s` WITH SYNC' before dumping",
		db, table,
		dbStatsFile,
		db, table,
	)
	return nil, nil
}

func createOutputGenDataWriter(ddlFileName string, confIdx, datafileIdx int) (*os.File, error) {
	if GendataConfig.Print {
		return os.Stdout, nil
	}

	dir := tableGenDataDir(ddlFileName)
	if confIdx == 0 && datafileIdx == 0 {
		// drop previous data dir
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output data dir '%s': %w", dir, err)
	}

	ext := "csv"
	if GendataConfig.OutputFormat == "insert" {
		ext = "sql"
	}

	file := filepath.Join(dir, fmt.Sprintf("%d_%d.%s", confIdx+1, datafileIdx+1, ext))
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("can not open output data file: %s, err: %w", file, err)
	}
	return f, nil
}

func tableGenDataDir(ddlFilePath string) string {
	return filepath.Join(GendataConfig.OutputDataDir, tableBaseName(ddlFilePath))
}

func tableBaseName(ddlFilePath string) string {
	ddlFileName := filepath.Base(ddlFilePath)
	return strings.TrimSuffix(strings.TrimSuffix(ddlFileName, ".table.sql"), ".sql")
}
