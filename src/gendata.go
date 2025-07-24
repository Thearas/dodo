package src

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"

	gen "github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/parser"
)

const (
	ColumnSeparator            = '☆' // make me happy
	DefaultGenRowCount         = 1000
	GenDataFileFirstLinePrefix = "columns:" // optional first line prefix if stream load needs 'columns: xxx' header
)

type (
	GenRule         = gen.GenRule
	GenconfEndError = gen.GenconfEndError
)

func NewTableGen(ddlfile, createTableStmt string, stats *TableStats, rows int, streamloadColNames []string) (*TableGen, error) {
	// parse create-table statement
	sqlId := ddlfile
	if stats != nil {
		sqlId += "#" + stats.Name
	}
	p := parser.NewParser(sqlId, createTableStmt)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	if !ok {
		logrus.Fatalln("SQL parser error")
	} else if p.ErrListener.LastErr != nil {
		return nil, p.ErrListener.LastErr
	}

	// get table stats
	table := strings.ReplaceAll(strings.ReplaceAll(c.GetName().GetText(), "`", ""), " ", "")
	colStats := make(map[string]*ColumnStats)
	if stats != nil {
		colStats = lo.SliceToMap(stats.Columns, func(s *ColumnStats) (string, *ColumnStats) {
			s.Count = stats.RowCount
			return s.Name, s
		})
		logrus.Debugf("using stats for table '%s'", table)
	} else {
		logrus.Debugf("stats not found for table '%s'", table)
	}

	// get custom table gen rule
	rowCount, customColumnRule := gen.GetCustomTableGenRule(table)
	colCount := len(c.ColumnDefs().GetCols())
	// decide table row count
	if rows <= 0 {
		rows = DefaultGenRowCount
	}
	if rowCount > 0 {
		rows = rowCount
	}
	tg := &TableGen{
		Name:    table,
		Columns: make([]string, 0, colCount),
		DDLFile: ddlfile,
		Rows:    rows,
		colGens: make([]gen.Gen, 0, colCount),
	}

	streamLoadCols := make([]string, 0, colCount) // construct for streamload header `curl -H 'columns: xxx'`
	hasStreamLoadColMapping := false
	for i, col := range c.ColumnDefs().GetCols() {
		var (
			colName     = strings.Trim(col.GetColName().GetText(), "`")
			colType_    = col.GetType_()
			visitor     = gen.NewTypeVisitor(fmt.Sprintf("%s.%s", table, colName), nil)
			colBaseType = visitor.GetBaseType(colType_)
		)

		// get column gen rule
		visitor.GenRule = newColGenRule(col, colName, colBaseType, colStats, customColumnRule)

		// build column generator
		gen := visitor.GetGen(colType_)
		tg.colGens = append(tg.colGens, gen)
		tg.RecordRefTables(*visitor.TableRefs...)
		tg.Columns = append(tg.Columns, colName)

		// column mapping in streamload header
		loadCol := lo.NthOr(streamloadColNames, i, colName)
		mapping, needMapping := buildStreamLoadMapping(visitor, loadCol, colBaseType)
		streamLoadCols = append(streamLoadCols, mapping)
		hasStreamLoadColMapping = hasStreamLoadColMapping || needMapping
	}

	if hasStreamLoadColMapping {
		tg.StreamloadColMapping = GenDataFileFirstLinePrefix + strings.Join(streamLoadCols, ",")
	}

	return tg, nil
}

func newColGenRule(
	col parser.IColumnDefContext,
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
			if colBaseType != "CHAR" && len(colstats.Min) != len(colstats.Max) {
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
		} else {
			if colstats.Min != "" {
				genRule["min"] = colstats.Min
			}
			if colstats.Max != "" {
				genRule["max"] = colstats.Max
			}
		}
	}

	// 2. Merge rules in global custom rules
	customRule, ok := customColumnRule[colName]
	if !ok || len(customRule) == 0 {
		return genRule
	}
	gen.MergeGenRules(genRule, customRule, true)

	notnull := col.NOT() != nil && col.GetNullable() != nil
	if notnull {
		genRule["null_frequency"] = 0
	}

	return genRule
}

func buildStreamLoadMapping(visitor *gen.TypeVisitor, loadColName, colBaseType string) (string, bool) {
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
		if from := visitor.GetRule("from"); from != nil {
			mapping = fmt.Sprintf("raw_%s,`%s`=hll_hash(%v)", loadColName, loadColName, from)
		}
	default:
		mapping = "`" + loadColName + "`"
	}
	return mapping, needMapping
}

type TableGen struct {
	Name       string
	Columns    []string
	DDLFile    string
	Rows       int
	RefToTable map[string]struct{} // ref generator to other tables

	StreamloadColMapping string
	colGens              []gen.Gen
}

// Gen generates multiple CSV line into writer.
func (tg *TableGen) GenCSV(w *bufio.Writer, rows int) error {
	if tg.StreamloadColMapping != "" {
		if _, err := w.WriteString(tg.StreamloadColMapping); err != nil {
			return err
		}
		w.WriteByte('\n')
	}

	var colIdxRefGens map[int]*gen.RefGen
	colRefGen := gen.GetTableRefGen(tg.Name)
	if len(colRefGen) > 0 {
		colIdxRefGens = make(map[int]*gen.RefGen, len(colRefGen))
		for i, c := range tg.Columns {
			if refgen, ok := colRefGen[c]; ok {
				colIdxRefGens[i] = refgen
			}
		}
	}

	for l := range rows {
		tg.genOne(w, colIdxRefGens)
		if l != rows-1 {
			if err := w.WriteByte('\n'); err != nil {
				return err
			}
		}
	}
	return nil
}

// GenOne generates one CSV line into writer.
func (tg *TableGen) genOne(w *bufio.Writer, colIdxRefGens map[int]*gen.RefGen) {
	for i, g := range tg.colGens {
		val := g.Gen()

		// add value to ref gen
		if len(colIdxRefGens) > 0 {
			if refgen := colIdxRefGens[i]; refgen != nil {
				refgen.AddRefVals(val)
			}
		}

		gen.WriteColVal(w, val)
		if i != len(tg.colGens)-1 {
			w.WriteRune(ColumnSeparator)
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

func (tg *TableGen) RemoveRefTable(t string) {
	if len(tg.RefToTable) == 0 {
		return
	}
	delete(tg.RefToTable, t)
}
