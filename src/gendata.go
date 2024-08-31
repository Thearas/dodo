package src

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/vbauerster/mpb/v8"

	gen "github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/parser"
)

const (
	DefaultGenRowCount         = 1000
	GenDataFileFirstLinePrefix = "columns:" // optional first line prefix if stream load needs 'columns: xxx' header
)

type (
	GenRule         = gen.GenRule
	GenconfEndError = gen.GenconfEndError
)

func SetupGendata(genconf string, confIdx int, outputFormat string) error {
	return gen.Setup(genconf, confIdx, outputFormat == "insert")
}

//nolint:revive
func NewTableGen(
	ddlfile, tableName string,
	columns []*Column,
	stats *TableStats,
	rows int64,
	isInternalTable bool,
) (*TableGen, error) {
	// change 'catalog.db.table' to 'table'
	table := parser.SqlIdentifier(tableName)
	tableParts := strings.Split(table, ".")
	table = tableParts[len(tableParts)-1]

	// remove hudi meta columns
	columns = removeHudiMetaCols(columns)

	// get column stats
	colStats := make(map[string]*ColumnStats)
	if stats != nil {
		colStats = stats.ColumnStats()
		logrus.Debugf("using stats for table '%s'", table)
	} else {
		logrus.Debugf("stats not found for table '%s'", table)
	}

	// get custom table gen rule
	rowCount, customColumnRule, ghostCols := gen.GetCustomTableGenRule(table)
	colCount := len(columns)
	// decide table row count
	if rowCount <= 0 {
		rowCount = DefaultGenRowCount
	}
	if rows <= 0 {
		rows = rowCount
	}

	ddlColNames := lo.Map(columns, func(c *Column, _ int) string { return c.Name })
	// validate ghost column names don't collide with DDL column names (case-insensitive)
	ddlColNamesLower := lo.Map(ddlColNames, func(s string, _ int) string { return strings.ToLower(s) })
	for _, gc := range ghostCols {
		if lo.Contains(ddlColNamesLower, strings.ToLower(gc)) {
			logrus.Fatalf("Ghost column '%s' in table '%s' conflicts with a DDL column of the same name", gc, table)
		}
	}

	tg := &TableGen{
		Name:          table,
		Columns:       append(ddlColNames, ghostCols...), // ghost columns appended after DDL columns
		DDLFile:       ddlfile,
		Rows:          rows,
		realColCount:  colCount,
		rowsPerInsert: 1000,
		lineDelimiter: "\n",
		colGens:       make([]gen.Gen, 0, colCount+len(ghostCols)),
		ColGenRules:   make(map[string]GenRule, colCount+len(ghostCols)),
	}

	streamLoadCols := make([]string, 0, colCount) // construct for streamload header `curl -H 'columns: xxx'`
	hasStreamLoadColMapping := false
	for _, col := range columns {
		var (
			colName     = col.Name
			colType_    = col.Type
			colPath     = fmt.Sprintf("%s.%s", table, colName)
			colBaseType = gen.GetBaseType(colType_, colPath)
		)

		// get column gen rule
		genRule := newColGenRule(col, colName, colBaseType, colStats, customColumnRule)

		// build column generator
		visitor := gen.NewColumnVisitor(table, tg.Columns, colPath, genRule)
		tg.colGens = append(tg.colGens, visitor.GetGen(colType_))
		tg.ColGenRules[colName] = visitor.GenRule
		tg.RecordRefTables(*visitor.TableRefs...)
		tg.RecordRefSelfCols(colName, *visitor.SelfColRefs...)

		// choose value writer
		writeColVal := gen.WriteCSVColVal
		if gen.GenInsertOrCSV {
			writeColVal = gen.GetInsertColValWriter(colType_)
		}
		tg.writeColVal = append(tg.writeColVal, writeColVal)

		// column mapping in streamload columns http-header
		mapping, needMapping := buildStreamLoadMapping(visitor, colName, colBaseType)
		streamLoadCols = append(streamLoadCols, mapping)
		hasStreamLoadColMapping = hasStreamLoadColMapping || needMapping
	}

	// only streamload csv needs columns http-header
	if !gen.GenInsertOrCSV && hasStreamLoadColMapping && isInternalTable {
		tg.StreamloadColMapping = GenDataFileFirstLinePrefix + strings.Join(streamLoadCols, ",")
	}

	// build ghost column generators (after DDL columns so they can be ref'd by index)
	for _, ghostName := range ghostCols {
		colPath := fmt.Sprintf("%s.%s", table, ghostName)
		ghostRule, ok := customColumnRule[strings.ToLower(ghostName)]
		if !ok || len(ghostRule) == 0 {
			logrus.Fatalf("Ghost column '%s' in table '%s' has no gen rule", ghostName, table)
		}
		delete(ghostRule, "ghost") // remove the ghost flag from gen rule

		visitor := gen.NewColumnVisitor(table, tg.Columns, colPath, ghostRule)
		tg.colGens = append(tg.colGens, visitor.GetGen(nil)) // nil type for ghost columns
		tg.RecordRefTables(*visitor.TableRefs...)
		tg.RecordRefSelfCols(ghostName, *visitor.SelfColRefs...)
		tg.writeColVal = append(tg.writeColVal, gen.WriteCSVColVal) // placeholder, never used for output
	}

	return tg, nil
}

func newColGenRule(
	col *Column,
	colName, colBaseType string,
	colStats map[string]*ColumnStats,
	customColumnRule map[string]GenRule,
) GenRule {
	genRule := GenRule{}

	// 1. Merge rules in stats
	if colstats, ok := colStats[colName]; ok {
		var nullFreq float32
		if colstats.Count > 0 {
			nullFreq = float32(colstats.NullCount) / float32(colstats.Count)
		}
		if nullFreq >= 0 && nullFreq < 1 {
			genRule["null_frequency"] = nullFreq
		}

		if IsStringType(colBaseType) {
			avgLen := colstats.AvgSizeByte
			genRule["length"] = avgLen

			// HACK: +-5/10 on string avg size as length
			if len(colstats.Min) != len(colstats.Max) && !slices.Contains([]string{"CHAR", "CHARACTER"}, colBaseType) {
				var extent int64
				if avgLen > 10 {
					extent = 10
				} else if avgLen > 5 {
					extent = 5
				}
				genRule["length"] = GenRule{
					"min": avgLen - extent,
					"max": avgLen + extent,
				}
			}
		} else if IsVaildStatsMinMax(colstats.Min, colstats.Max) {
			genRule["min"] = colstats.Min
			genRule["max"] = colstats.Max
		}
	}

	// 2. Merge rules in global custom rules (case-insensitive lookup for Doris)
	customRule, ok := customColumnRule[strings.ToLower(colName)]
	if !ok || len(customRule) == 0 {
		// 3. Auto-assign inc generator for unique columns without custom gen rule
		if col.Unique {
			applyUniqueIncRule(genRule, colBaseType)
		}
		return genRule
	}
	gen.MergeGenRules(genRule, customRule, true)

	if !col.Nullable {
		genRule["null_frequency"] = 0
	}

	// 3. Auto-assign inc generator for unique columns without custom gen rule
	if col.Unique {
		if _, hasGen := genRule["gen"]; !hasGen {
			applyUniqueIncRule(genRule, colBaseType)
		}
	}

	return genRule
}

// PickUniqueIncColumn selects at most one unique key column to auto-assign inc.
// It picks the first numeric column, and clears
// the Unique flag on all other columns.
func PickUniqueIncColumn(cols []*Column) {
	chosen := -1
	for i, col := range cols {
		if !col.Unique {
			continue
		}
		if col.Type == nil {
			continue
		}
		baseType := parser.GetBaseType(col.Type)
		if IsIntegerType(baseType) || IsFloatType(baseType) {
			chosen = i
			break
		}
	}
	// Clear Unique on all columns except the chosen one
	for i, col := range cols {
		if col.Unique && i != chosen {
			col.Unique = false
		}
	}
}

// applyUniqueIncRule sets a default `gen: inc:` rule for a unique column
// of numeric or datetime type. The start value is taken from the existing
// min rule (from stats or user config), falling back to default type min;
// step is 1 for numeric, 1s for datetime types.
func applyUniqueIncRule(genRule GenRule, colBaseType string) {
	incRule := GenRule{}

	// resolve min: prefer genRule["min"], fallback to default type min
	resolveMin := func() any {
		if min_, ok := genRule["min"]; ok {
			return min_
		}
		baseType := colBaseType
		if alias, ok := gen.TypeAlias[baseType]; ok {
			baseType = alias
		}
		if defaultRule, ok := gen.DefaultTypeGenRules[baseType].(GenRule); ok {
			return defaultRule["min"]
		}
		return nil
	}

	switch {
	case IsIntegerType(colBaseType) || IsFloatType(colBaseType):
		if min_ := resolveMin(); min_ != nil {
			incRule["start"] = min_
		}
		incRule["step"] = 1
	case IsDateType(colBaseType):
		if min_ := resolveMin(); min_ != nil {
			incRule["start"] = min_
		}
		incRule["step"] = "1d"
	case IsDateTimeType(colBaseType):
		if min_ := resolveMin(); min_ != nil {
			incRule["start"] = min_
		}
		incRule["step"] = "1s"
	default:
		return
	}

	genRule["gen"] = GenRule{"inc": incRule}
}

func buildStreamLoadMapping(visitor *gen.ColumnVisitor, loadColName, colBaseType string) (string, bool) {
	var (
		mapping     string
		needMapping bool
	)
	switch colBaseType {
	case "BITMAP":
		needMapping = true
		mapping = fmt.Sprintf("raw_%s,`%s`=bitmap_from_array(cast(raw_%s as ARRAY<BIGINT(20)>))", loadColName, loadColName, loadColName)
	case "HLL":
		needMapping = true
		mapping = fmt.Sprintf("raw_%s,`%s`=hll_empty()", loadColName, loadColName)
		if fromCol := visitor.GetRule("from"); fromCol != nil {
			mapping = fmt.Sprintf("raw_%s,`%s`=%v", loadColName, loadColName, fromCol)
		}
	case "AGG_STATE":
		needMapping = true
		aggFunc := visitor.GetRule("agg_func").(string)                                //nolint:revive
		aggArgs := visitor.GetRule("agg_args").([]parser.IDataTypeWithNullableContext) //nolint:revive

		argColNames := strings.Join(
			lo.Map(cast.ToStringSlice(lo.Range(len(aggArgs))),
				func(arg string, _ int) string { return fmt.Sprintf("tmp_%s_%s", loadColName, arg) }),
			",",
		)
		mapping = fmt.Sprintf("%s,`%s`=%s(%s)", argColNames, loadColName, aggFunc, argColNames)
	default:
		mapping = "`" + loadColName + "`"
	}
	return mapping, needMapping
}

type refSelf struct {
	currColIdx   int
	sourceColIdx []int
}

type TableGen struct {
	Name          string
	Columns       []string
	DDLFile       string
	Rows          int64
	RefToTable    map[string]struct{} // ref generator to other tables
	RefToSelfCols []refSelf           // ref generator to same table columns

	StreamloadColMapping string
	realColCount         int // number of DDL (non-ghost) columns; ghost columns are at indices [realColCount, len(colGens))
	rowsPerInsert        int64
	lineDelimiter        string
	colGens              []gen.Gen
	ColGenRules          map[string]GenRule
	writeColVal          []gen.WriteColVal
}

func (tg *TableGen) SetRowsPerInsert(r int64) {
	tg.rowsPerInsert = r
}

func (tg *TableGen) SetLineDelimiter(d string) {
	tg.lineDelimiter = d
}

// Gen generates multiple CSV line into writer.
func (tg *TableGen) Gen(ctx context.Context, w *bufio.Writer, rows int64, progress *mpb.Bar) error {
	if tg.StreamloadColMapping != "" {
		if _, err := w.WriteString(tg.StreamloadColMapping); err != nil {
			return err
		}
		w.WriteByte('\n')
	}

	// Get SourceTableData if this table is referenced by others
	sourceData := gen.GetSourceTableData(tg.Name)

	// Pre-compute column mapping for SourceTableData
	var sourceColMapping []int // sourceData col idx -> valCache idx
	if sourceData != nil {
		colIndexes := sourceData.GetColumnIndexes()
		sourceColMapping = make([]int, len(colIndexes))
		for i, c := range tg.Columns {
			if colIdx, ok := colIndexes[c]; ok {
				sourceColMapping[colIdx] = i
			}
		}
	}
	// th col idx that has ref to other cols in same table
	selfRefColIdxs := lo.SliceToMap(tg.RefToSelfCols, func(ref refSelf) (int, struct{}) {
		return ref.currColIdx, struct{}{}
	})

	// Get RowRefGroups that this table depends on (for SelectNewRow)
	refGroups := gen.GetRowRefGroupsForDest(tg.Name)

	var (
		insert          = gen.GenInsertOrCSV
		columnSeparator = gen.ColumnSeparator
		rowDelimiter    = tg.lineDelimiter
		progressStep    = min(max(tg.Rows/100, 1), 1000) // min(1%, 1k) rows

		c = gen.NewGenContext(make([]any, len(tg.colGens)))
	)
	if insert {
		columnSeparator = ','
		rowDelimiter = ","
	}

	for l := range rows {
		currRow := l + 1
		if insert {
			newInsertStmt := l%tg.rowsPerInsert == 0
			if newInsertStmt {
				tg.genInsertPrefix(w)
			}
			w.WriteByte('(')
		}

		// Select new row for each RowRefGroup before generating
		for _, grp := range refGroups {
			c.RefSourceTableRowIdx[grp.SourceTable] = grp.SelectOneRow()
		}

		// generates one row into value cache.
		tg.genOne(c, selfRefColIdxs)
		// Add row data to SourceTableData (shared, only once per source table)
		if sourceData != nil {
			rowData := make([]any, len(sourceColMapping))
			for colIdx, tableIdx := range sourceColMapping {
				rowData[colIdx] = c.ColVals[tableIdx]
			}
			sourceData.AddRow(rowData)
		}

		// write the row into writer (only DDL columns, skip ghost columns)
		for i := range tg.realColCount {
			tg.writeColVal[i](w, c.ColVals[i])
			if i != tg.realColCount-1 {
				w.WriteRune(columnSeparator)
			}
		}

		// clear cache
		c.ClearCache()

		if insert {
			w.WriteByte(')')
			if currRow%tg.rowsPerInsert == 0 || currRow == rows {
				w.WriteString(";\n")
			}
		}

		if l != rows-1 {
			if _, err := w.WriteString(rowDelimiter); err != nil {
				return err
			}
		}

		// check after every progress step
		if currRow%progressStep == 0 || currRow == rows {
			// add progress
			if progress != nil {
				incr := progressStep
				if currRow == rows {
					incr = rows % progressStep
					if incr == 0 {
						incr = progressStep
					}
				}
				progress.IncrInt64(incr)
			}

			// check context done
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	return nil
}

func (tg *TableGen) genInsertPrefix(w *bufio.Writer) {
	w.WriteString("INSERT INTO `")
	w.WriteString(tg.Name)
	w.WriteString("` (`")
	w.WriteString(strings.Join(tg.Columns[:tg.realColCount], "`,`"))
	w.WriteString("`) VALUES ")
}

// GenOne generates one row into cache.
func (tg *TableGen) genOne(c *gen.GenContext, selfRefColIdxs map[int]struct{}) {
	hasSelfRef := selfRefColIdxs != nil
	for i, g := range tg.colGens {
		// skip self ref col idx, will fill later
		if hasSelfRef {
			if _, ok := selfRefColIdxs[i]; ok {
				continue
			}
		}
		c.ColVals[i] = g.Gen(c)
	}

	// fill self ref col idx
	if hasSelfRef {
		for _, ref := range tg.RefToSelfCols {
			c.ColVals[ref.currColIdx] = tg.colGens[ref.currColIdx].Gen(c)
		}
	}
}

func (tg *TableGen) RecordRefTables(ts ...string) {
	if tg.RefToTable == nil {
		tg.RefToTable = map[string]struct{}{}
	}

	for _, t := range ts {
		tg.RefToTable[t] = struct{}{}
	}
}

func (tg *TableGen) RecordRefSelfCols(currCol string, sourceCols ...string) {
	if len(sourceCols) == 0 {
		return
	}

	currColIdx := lo.IndexOf(tg.Columns, currCol)
	if currColIdx < 0 {
		logrus.Fatalf("ref: column '%s' not found in table '%s'", currCol, tg.Name)
	}
	sourceColsIdx := lo.Map(sourceCols, func(sc string, _ int) int {
		i := lo.IndexOf(tg.Columns, sc)
		if i < 0 {
			logrus.Fatalf("ref: source column '%s' not found in table '%s'", sc, tg.Name)
		}
		return i
	})

	tg.RefToSelfCols = append(tg.RefToSelfCols, refSelf{
		currColIdx:   currColIdx,
		sourceColIdx: sourceColsIdx,
	})

	// sort by ref chain
	slices.SortFunc(tg.RefToSelfCols, func(a, b refSelf) int {
		if slices.Contains(a.sourceColIdx, b.currColIdx) {
			return 1
		} else if slices.Contains(b.sourceColIdx, a.currColIdx) {
			return -1
		}
		return cmp.Compare(a.currColIdx, b.currColIdx)
	})
}

func (tg *TableGen) RemoveRefTable(t string) {
	if len(tg.RefToTable) == 0 {
		return
	}
	delete(tg.RefToTable, t)
}

func removeHudiMetaCols(columns []*Column) []*Column {
	columns = lo.Filter(columns, func(c *Column, _ int) bool {
		return !isHudiMetaCol(c.Name)
	})

	return columns
}

func isHudiMetaCol(colName string) bool {
	return strings.HasPrefix(colName, "_hoodie_")
}

type Column struct {
	Name     string
	Type     parser.IDataTypeContext
	Nullable bool
	Unique   bool
}

func ColumnsFromParsed(columns []parser.IColumnDefContext, uniqueKeyCols []string) []*Column {
	uniqueKeySet := lo.SliceToMap(uniqueKeyCols, func(s string) (string, struct{}) { return s, struct{}{} })

	cols := make([]*Column, 0, len(columns))
	for _, col := range columns {
		colName := parser.SqlIdentifier(col.GetColName().GetText())
		_, isUnique := uniqueKeySet[colName]
		cols = append(cols, &Column{
			Name:     colName,
			Type:     col.GetType_(),
			Nullable: col.NOT() == nil || col.GetNullable() == nil,
			Unique:   isUnique,
		})
	}

	return cols
}

// ValidatePartitionColumns checks that all partition columns have YAML config rules.
// partitionCols: partition column names extracted from DDL.
// customColumnRule: column rules from gendata YAML config.
func ValidatePartitionColumns(table string, partitionCols []string, customColumnRule map[string]GenRule) error {
	if len(partitionCols) == 0 {
		return nil
	}

	// Build a lowercase-keyed set for case-insensitive lookup,
	// since Doris column names are case-insensitive.
	lowerRuleKeys := make(map[string]struct{}, len(customColumnRule))
	for k := range customColumnRule {
		lowerRuleKeys[strings.ToLower(k)] = struct{}{}
	}

	var missing []string
	for _, col := range partitionCols {
		if _, ok := lowerRuleKeys[strings.ToLower(col)]; !ok {
			missing = append(missing, col)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"table '%s' has PARTITION BY columns %v, but these columns have no gendata YAML config rules: %v. "+
				"Partition columns must have explicit generation rules (e.g. min/max) to ensure generated data falls within valid partitions",
			table, partitionCols, missing,
		)
	}

	return nil
}
