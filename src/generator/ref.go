package generator

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

// SourceTableData (t1)         RowRefGroup (t2→t1)
// ┌─────────────────┐          ┌────────────────┐
// │ Table: "t1"     │◄─────────│ DestTable ─────┼──┐
// │ Limit: 1000     │          │ SourceTable    │  │
// │ Columns: [c1,c2]│          │ sourceData     │  │
// │ rows: [...]     │          │ refColumns     │  │
// └─────────────────┘          └────────────────┘  │
//         ▲                    RowRefGroup (t3→t1) │
//         │                    ┌────────────────┐  │
//         └────────────────────│ DestTable ─────┼──┘
//                              │ SourceTable    │
//                              │ sourceData     │
//                              │ refColumns     │
//                              └────────────────┘

// TODO: use disk to store refVals, now is in-memory impl.

const DefaultRefLimit = 1000

var (
	_ Gen = &RefGen{}

	// sourceTable -> SourceTableData (shared storage)
	sourceTableDataMap     = map[string]*SourceTableData{}
	sourceTableDataMapLock sync.Mutex

	// destTable -> sourceTable -> RowRefGroup (row selection logic only)
	rowRefGroupMap     = map[string]map[string]*RowRefGroup{}
	rowRefGroupMapLock sync.Mutex
)

func CleanRefData() {
	sourceTableDataMapLock.Lock()
	sourceTableDataMap = map[string]*SourceTableData{}
	sourceTableDataMapLock.Unlock()

	rowRefGroupMapLock.Lock()
	rowRefGroupMap = map[string]map[string]*RowRefGroup{}
	rowRefGroupMapLock.Unlock()
}

// SourceTableData stores row data for a source table (shared across all dest tables).
type SourceTableData struct {
	Table      string
	Limit      int
	Columns    []string       // all referenced columns in order
	colIndexes map[string]int // column -> index in row

	rows *[][]any // stored rows
	nth  *atomic.Int32
	lock sync.RWMutex
}

func newSourceTableData(table string, limit int) *SourceTableData {
	return &SourceTableData{
		Table:      table,
		Limit:      limit,
		Columns:    []string{},
		colIndexes: map[string]int{},
		rows:       &[][]any{},
		nth:        &atomic.Int32{},
	}
}

func (s *SourceTableData) addColumn(column string) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if idx, ok := s.colIndexes[column]; ok {
		return idx
	}

	idx := len(s.Columns)
	s.Columns = append(s.Columns, column)
	s.colIndexes[column] = idx
	return idx
}

func (s *SourceTableData) updateLimit(limit int) {
	s.lock.Lock()
	if limit > s.Limit {
		s.Limit = limit
	}
	s.lock.Unlock()
}

// AddRow adds a row of values using reservoir sampling.
func (s *SourceTableData) AddRow(values []any) {
	nth := s.nth.Add(1)
	limit := s.Limit

	// fast path: filling phase
	if int(nth) <= limit {
		s.lock.Lock()
		*s.rows = append(*s.rows, values)
		s.lock.Unlock()
		return
	}

	// reservoir sampling: replace with probability limit/nth
	if idx := rand.IntN(int(nth)); idx < limit {
		s.lock.Lock()
		(*s.rows)[idx] = values
		s.lock.Unlock()
	}
}

// GetColumnIndexes returns column name to index mapping (read-only).
func (s *SourceTableData) GetColumnIndexes() map[string]int {
	return s.colIndexes
}

// GetSourceTableData returns the SourceTableData for a given source table.
func GetSourceTableData(sourceTable string) *SourceTableData {
	sourceTableDataMapLock.Lock()
	defer sourceTableDataMapLock.Unlock()
	return sourceTableDataMap[sourceTable]
}

// RowRefGroup manages row-based references from a dest table to a source table.
// Multiple columns in the dest table referencing the same source table share the same row.
type RowRefGroup struct {
	DestTable   string
	SourceTable string
	sourceData  *SourceTableData // shared source data

	// columns this dest table references (subset of sourceData.Columns)
	refColumns []string
}

func newRowRefGroup(destTable string, sourceData *SourceTableData) *RowRefGroup {
	return &RowRefGroup{
		DestTable:   destTable,
		SourceTable: sourceData.Table,
		sourceData:  sourceData,
		refColumns:  []string{},
	}
}

func (g *RowRefGroup) addColumn(column string) {
	// check if already added to this group
	for _, c := range g.refColumns {
		if c == column {
			return
		}
	}
	g.refColumns = append(g.refColumns, column)
}

// SelectOneRow randomly selects a new row for this generation round.
// Must be called once before generating each row of the dest table.
func (g *RowRefGroup) SelectOneRow() int {
	nRows := len(*g.sourceData.rows)
	if nRows == 0 {
		logrus.Fatalf("Table '%s' has no rows", g.SourceTable)
	}
	return rand.IntN(nRows)
}

// Gen returns the value at sourceColIdx from the currently selected row.
func (g *RowRefGroup) Gen(rowIdx, colIdx int) any {
	rows := *g.sourceData.rows
	if len(rows) == 0 {
		logrus.Fatalf("empty ref values for source table '%s'", g.SourceTable)
	}
	return rows[rowIdx][colIdx]
}

// getRowRefGroups returns all RowRefGroups that reference the given source table.
func getRowRefGroups(sourceTable string) []*RowRefGroup {
	rowRefGroupMapLock.Lock()
	defer rowRefGroupMapLock.Unlock()

	var result []*RowRefGroup
	for _, srcMap := range rowRefGroupMap {
		if grp, ok := srcMap[sourceTable]; ok {
			result = append(result, grp)
		}
	}
	return result
}

// GetRowRefGroupsForDest returns all RowRefGroups for the given dest table.
func GetRowRefGroupsForDest(destTable string) []*RowRefGroup {
	rowRefGroupMapLock.Lock()
	defer rowRefGroupMapLock.Unlock()

	refMap, ok := rowRefGroupMap[destTable]
	if !ok {
		return nil
	}
	return lo.Values(refMap)
}

// RefGen generates values by referencing another table's column.
type RefGen struct {
	Table  string // source table
	Column string // source column

	group        *RowRefGroup
	sourceColIdx int // index in SourceTableData
}

func (g *RefGen) Gen(c *GenContext) any {
	return g.group.Gen(c.RefSourceTableRowIdx[g.Table], g.sourceColIdx)
}

func NewRefGenerator(v *ColumnVisitor, _ parser.IDataTypeContext, r GenRule) (Gen, error) {
	// parse "table.column"
	var sourceTable, sourceColumn string
	ref := cast.ToString(r["ref"])
	parts := strings.Split(ref, ".")
	if len(parts) == 2 {
		sourceTable, sourceColumn = parts[0], parts[1]
	} else if len(parts) == 1 {
		// ref to same table but different column
		return NewRefSelfGenerator(v, nil, r)
	} else {
		return nil, fmt.Errorf("invalid ref format, expect '<table>.<column>', got '%s'", ref)
	}

	// if ref points to the same table, use ref_self
	destTable := v.Table
	if sourceTable == destTable {
		r["ref"] = sourceColumn
		return NewRefSelfGenerator(v, nil, r)
	}

	*v.TableRefs = lo.Uniq(append(*v.TableRefs, sourceTable))

	// parse limit
	limit := DefaultRefLimit
	if l := cast.ToInt(r["limit"]); l > 0 {
		limit = l
	}

	// get or create SourceTableData (shared)
	sourceTableDataMapLock.Lock()
	sourceData, ok := sourceTableDataMap[sourceTable]
	if !ok {
		sourceData = newSourceTableData(sourceTable, limit)
		sourceTableDataMap[sourceTable] = sourceData
	} else {
		sourceData.updateLimit(limit)
	}
	sourceColIdx := sourceData.addColumn(sourceColumn)
	sourceTableDataMapLock.Unlock()

	// get or create RowRefGroup
	rowRefGroupMapLock.Lock()
	if rowRefGroupMap[destTable] == nil {
		rowRefGroupMap[destTable] = map[string]*RowRefGroup{}
	}
	grp, ok := rowRefGroupMap[destTable][sourceTable]
	if !ok {
		grp = newRowRefGroup(destTable, sourceData)
		rowRefGroupMap[destTable][sourceTable] = grp
	}
	grp.addColumn(sourceColumn)
	rowRefGroupMapLock.Unlock()

	return &RefGen{
		Table:        sourceTable,
		Column:       sourceColumn,
		group:        grp,
		sourceColIdx: sourceColIdx,
	}, nil
}
