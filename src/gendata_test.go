package src

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/generator"
	gen "github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/parser"
)

func init() {
	generator.Setup("", 0, false)
}

func TestGendata(t *testing.T) {
	sql := `CREATE TABLE all_type_nullable (
    _hoodie_meta_col string NULL,
	dt_month varchar(6) NULL,
	company_code decimal(20, 1) NULL,
	json1 JSON NULL,
	jsonb1 JSONB NULL,
	variant1 VARIANT NULL,
	date1 date NULL,
	datetime1 datetime NULL,
	t_bitmap BITMAP,
	t_null_string string,
    t_null_varchar varchar(255),
    t_null_char char(10),
    t_null_decimal_precision_2 decimal(2,1),
    t_null_decimal_precision_4 decimal(4,2),
    t_null_decimal_precision_8 decimal(8,4),
    t_null_decimal_precision_17 decimal(17,8),
    t_null_decimal_precision_18 decimal(18,8),
    t_null_decimal_precision_38 decimal(38,16),
    t_str string,
    t_string string,
    t_empty_varchar varchar(255),
    t_varchar varchar(255),
    t_varchar_max_length varchar(255),
    t_char char(10),
    t_int int,
    t_bigint bigint,
    t_float float,
    t_double double,
    t_boolean_true boolean,
    t_boolean_false boolean,
    t_decimal_precision_2 decimal(2,1),
    t_decimal_precision_4 decimal(4,2),
    t_decimal_precision_8 decimal(8,4),
    t_decimal_precision_17 decimal(17,8),
    t_decimal_precision_18 decimal(18,8),
    t_decimal_precision_38 decimal(38,16),
    t_map_string map<string,string>,
    t_map_varchar map<varchar(255),varchar(255)>,
    t_map_char map<char(10),char(10)>,
    t_map_int map<int,int>,
    t_map_bigint map<bigint,bigint>,
    t_map_float map<float,float>,
    t_map_double map<double,double>,
    t_map_boolean map<boolean,boolean>,
    t_map_decimal_precision_2 map<decimal(2,1),decimal(2,1)>,
    t_map_decimal_precision_4 map<decimal(4,2),decimal(4,2)>,
    t_map_decimal_precision_8 map<decimal(8,4),decimal(8,4)>,
    t_map_decimal_precision_17 map<decimal(17,8),decimal(17,8)>,
    t_map_decimal_precision_18 map<decimal(18,8),decimal(18,8)>,
    t_map_decimal_precision_38 map<decimal(38,16),decimal(38,16)>,
    t_array_string array<string>,
    t_array_int array<int>,
    t_array_bigint array<bigint>,
    t_array_float array<float>,
    t_array_double array<double>,
    t_array_boolean array<boolean>,
    t_array_varchar array<varchar(255)>,
    t_array_char array<char(10)>,
    t_array_decimal_precision_2 array<decimal(2,1)>,
    t_array_decimal_precision_4 array<decimal(4,2)>,
    t_array_decimal_precision_8 array<decimal(8,4)>,
    t_array_decimal_precision_17 array<decimal(17,8)>,
    t_array_decimal_precision_18 array<decimal(18,8)>,
    t_array_decimal_precision_38 array<decimal(38,16)>,
    t_struct_bigint struct<s_bigint:bigint>,
    t_complex map<string,array<struct<s_int:int>>>,
    t_struct_nested struct<struct_field:array<string>>,
    t_struct_null struct<struct_field_null:string,struct_field_null2:string>,
    t_struct_non_nulls_after_nulls struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
    t_nested_struct_non_nulls_after_nulls struct<struct_field1:int,struct_field2:string,struct_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
    t_map_null_value map<string,string>,
    t_array_string_starting_with_nulls array<string>,
    t_array_string_with_nulls_in_between array<string>,
    t_array_string_ending_with_nulls array<string>,
    t_variant variant<PROPERTIES("variant_max_subcolumns_count"="10000")>
) ENGINE=OLAP
DUPLICATE KEY(dt_month)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(dt_month) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1", -- usually the BE nodes
'bloom_filter_columns' = "dt_month, company_code"
);`

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok, "SQL parser error")
	assert.NoError(t, p.ErrListener.LastErr)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), nil)

	rows := int64(50)
	tg, err := NewTableGen("create-table.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)
	assert.Len(t, tg.colGens, 74)

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(b.String(), "\n")
	assert.Len(t, resultCSV, int(1+tg.Rows)) // first line is columns info
	assert.True(t, strings.HasPrefix(resultCSV[0], GenDataFileFirstLinePrefix))
}

func TestGendataSelfRef(t *testing.T) {
	sql := `CREATE TABLE employee (
	id int NOT NULL,
	name string NOT NULL,
	manager_id int NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1"
);`

	// Define custom gen rule for self reference
	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "employee",
				"columns": []any{
					generator.GenRule{
						"name": "manager_id",
						"gen": generator.GenRule{
							"ref": "id",
						},
					},
				},
			},
		},
	})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok, "SQL parser error")
	assert.NoError(t, p.ErrListener.LastErr)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), nil)

	rows := int64(10)
	tg, err := NewTableGen("employee.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)
	assert.Len(t, tg.colGens, 3)
	assert.Len(t, tg.RefToSelfCols, 1)
	assert.Equal(t, 2, tg.RefToSelfCols[0].currColIdx)
	assert.Equal(t, []int{0}, tg.RefToSelfCols[0].sourceColIdx)

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(strings.TrimSpace(b.String()), "\n")
	assert.Len(t, resultCSV, int(tg.Rows)) // no columns info because isInternalTable=true but no mapping

	for _, line := range resultCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 3)
		// parts[0] is id, parts[2] is manager_id
		// they should be equal because of the self ref
		assert.Equal(t, parts[0], parts[2], "manager_id should be equal to id for line: %s", line)
	}
}

func TestGendataSelfRefChain(t *testing.T) {
	sql := `CREATE TABLE chain (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 int NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(c1) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	// c3 -> c2 -> c1
	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "chain",
				"columns": []any{
					generator.GenRule{
						"name": "c2",
						"gen":  generator.GenRule{"ref": "c1"},
					},
					generator.GenRule{
						"name": "c3",
						"gen":  generator.GenRule{"ref": "c2"},
					},
				},
			},
		},
	})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), nil)

	tg, err := NewTableGen("chain.sql", "chain", columns, nil, 10, true)
	assert.NoError(t, err)
	assert.Len(t, tg.RefToSelfCols, 2)

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(strings.TrimSpace(b.String()), "\n")
	for _, line := range resultCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 3)
		assert.Equal(t, parts[0], parts[1])
		assert.Equal(t, parts[1], parts[2])
	}
}

func TestGendataGhostColumn(t *testing.T) {
	sql := `CREATE TABLE dept (
	id int NOT NULL,
	name varchar(50) NOT NULL,
	department_id int NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	// Ghost column _ghost_dept generates raw values; department_id refs the ghost column
	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "dept",
				"columns": []any{
					generator.GenRule{
						"name":  "_ghost_dept",
						"ghost": true,
						"gen":   generator.GenRule{"inc": 1, "start": 100},
					},
					generator.GenRule{
						"name": "department_id",
						"gen":  generator.GenRule{"ref": "_ghost_dept"},
					},
				},
			},
		},
	})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	assert.NoError(t, p.ErrListener.LastErr)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), nil)

	rows := int64(10)
	tg, err := NewTableGen("dept.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)
	assert.Equal(t, 3, tg.realColCount, "realColCount should be 3 (DDL columns only)")
	assert.Len(t, tg.colGens, 4, "colGens should be 4 (3 DDL + 1 ghost)")
	assert.Len(t, tg.Columns, 4, "Columns should include ghost column")

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(strings.TrimSpace(b.String()), "\n")
	assert.Len(t, resultCSV, int(rows))

	for i, line := range resultCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 3, "CSV should have 3 columns (ghost excluded), line: %s", line)
		// department_id (parts[2]) should equal the ghost column's incremented value
		expected := fmt.Sprintf("%d", 100+i)
		assert.Equal(t, expected, parts[2], "department_id should match ghost column value at row %d", i)
	}
}

func TestGendataGhostColumnWithFormat(t *testing.T) {
	sql := `CREATE TABLE fmttest (
	col_a varchar(50) NOT NULL,
	col_b int NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(col_a) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	// Ghost column _ghost with inc generator.
	// col_a refs ghost with format (formatted output).
	// col_b refs ghost without format (raw value).
	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "fmttest",
				"columns": []any{
					generator.GenRule{
						"name":  "_ghost",
						"ghost": true,
						"gen":   generator.GenRule{"inc": 1, "start": 1},
					},
					generator.GenRule{
						"name":   "col_a",
						"format": "prefix-{{%05d}}",
						"gen":    generator.GenRule{"ref": "_ghost"},
					},
					generator.GenRule{
						"name": "col_b",
						"gen":  generator.GenRule{"ref": "_ghost"},
					},
				},
			},
		},
	})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), nil)

	rows := int64(5)
	tg, err := NewTableGen("fmttest.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, tg.realColCount)
	assert.Len(t, tg.colGens, 3) // 2 DDL + 1 ghost

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(strings.TrimSpace(b.String()), "\n")
	assert.Len(t, resultCSV, int(rows))

	for i, line := range resultCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 2, "CSV should have 2 columns (ghost excluded)")
		expectedFormatted := fmt.Sprintf("prefix-%05d", i+1)
		expectedRaw := fmt.Sprintf("%d", i+1)
		assert.Equal(t, expectedFormatted, parts[0], "col_a should be formatted at row %d", i)
		assert.Equal(t, expectedRaw, parts[1], "col_b should be raw value at row %d", i)
	}
}

func TestGendataGhostColumnCrossTableRef(t *testing.T) {
	// Source table: employees has a ghost column _ghost_dept_id (inc 1..10).
	// DDL column department_id formats it as "D-00001".
	// Dest table: sales refs the ghost column employees._ghost_dept_id to get the raw value,
	// and applies its own format "1{{%06d}}".
	srcSQL := `CREATE TABLE employees (
	employee_id int NOT NULL,
	department_id varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(employee_id) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	dstSQL := `CREATE TABLE sales (
	sale_id int NOT NULL,
	dp_id varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(sale_id) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "employees",
				"columns": []any{
					generator.GenRule{
						"name":  "_ghost_dept_id",
						"ghost": true,
						"gen":   generator.GenRule{"inc": 1, "start": 1},
					},
					generator.GenRule{
						"name":   "department_id",
						"format": "D-{{%05d}}",
						"gen":    generator.GenRule{"ref": "_ghost_dept_id"},
					},
				},
			},
			generator.GenRule{
				"name": "sales",
				"columns": []any{
					generator.GenRule{
						"name":   "dp_id",
						"format": "1{{%06d}}",
						"gen":    generator.GenRule{"ref": "employees._ghost_dept_id"},
					},
				},
			},
		},
	})

	// 1. Build BOTH table generators first (like cmd/gendata.go does),
	//    so cross-table SourceTableData is registered before generation.
	pSrc := parser.NewParser("create-table", srcSQL)
	cSrc, ok := pSrc.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	colsSrc := ColumnsFromParsed(cSrc.ColumnDefs().GetCols(), nil)

	srcRows := int64(10)
	tgSrc, err := NewTableGen("employees.sql", cSrc.GetName().GetText(), colsSrc, nil, srcRows, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, tgSrc.realColCount, "employees has 2 DDL columns")
	assert.Len(t, tgSrc.colGens, 3, "employees has 3 generators (2 DDL + 1 ghost)")

	pDst := parser.NewParser("create-table", dstSQL)
	cDst, ok := pDst.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	colsDst := ColumnsFromParsed(cDst.ColumnDefs().GetCols(), nil)

	dstRows := int64(20)
	tgDst, err := NewTableGen("sales.sql", cDst.GetName().GetText(), colsDst, nil, dstRows, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, tgDst.realColCount)

	// 2. Generate source table first (employees)
	bSrc := &bytes.Buffer{}
	wSrc := bufio.NewWriter(bSrc)
	assert.NoError(t, tgSrc.Gen(context.Background(), wSrc, tgSrc.Rows, nil))
	assert.NoError(t, wSrc.Flush())

	srcCSV := strings.Split(strings.TrimSpace(bSrc.String()), "\n")
	assert.Len(t, srcCSV, int(srcRows))
	// Verify source output: ghost column excluded, department_id formatted
	for i, line := range srcCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 2, "employees CSV should have 2 columns (ghost excluded)")
		expectedDeptID := fmt.Sprintf("D-%05d", i+1)
		assert.Equal(t, expectedDeptID, parts[1], "employees.department_id at row %d", i)
	}

	// 3. Generate dest table (sales)
	bDst := &bytes.Buffer{}
	wDst := bufio.NewWriter(bDst)
	assert.NoError(t, tgDst.Gen(context.Background(), wDst, tgDst.Rows, nil))
	assert.NoError(t, wDst.Flush())

	dstCSV := strings.Split(strings.TrimSpace(bDst.String()), "\n")
	assert.Len(t, dstCSV, int(dstRows))

	// Collect the raw ghost values generated by employees (1..10)
	ghostVals := make(map[string]bool)
	for i := range int(srcRows) {
		ghostVals[fmt.Sprintf("%d", i+1)] = true
	}

	for _, line := range dstCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 2, "sales CSV should have 2 columns")
		dpID := parts[1]
		// dp_id should be formatted as "1" + 6-digit zero-padded raw ghost value
		assert.True(t, strings.HasPrefix(dpID, "1"), "dp_id should start with '1': %s", dpID)
		assert.Len(t, dpID, 7, "dp_id should be 7 chars (1 + 6 digits): %s", dpID)
		// Extract the raw value: strip "1" prefix and leading zeros
		rawStr := strings.TrimLeft(dpID[1:], "0")
		if rawStr == "" {
			rawStr = "0"
		}
		assert.True(t, ghostVals[rawStr],
			"dp_id raw value '%s' should be one of the ghost column values (1..%d), got dp_id=%s", rawStr, srcRows, dpID)
	}
}

func TestValidatePartitionColumns(t *testing.T) {
	tests := []struct {
		name           string
		table          string
		partitionCols  []string
		customColRules map[string]GenRule
		wantErr        bool
	}{
		{
			name:           "no partition columns",
			table:          "t1",
			partitionCols:  nil,
			customColRules: map[string]GenRule{},
			wantErr:        false,
		},
		{
			name:          "all partition columns have rules",
			table:         "t2",
			partitionCols: []string{"col_date", "col_int"},
			customColRules: map[string]GenRule{
				"col_date": {"min": "2020-01-01", "max": "2025-01-01"},
				"col_int":  {"min": 1, "max": 100},
			},
			wantErr: false,
		},
		{
			name:          "missing one partition column rule",
			table:         "t3",
			partitionCols: []string{"col_date", "col_int"},
			customColRules: map[string]GenRule{
				"col_date": {"min": "2020-01-01", "max": "2025-01-01"},
			},
			wantErr: true,
		},
		{
			name:           "all partition columns missing rules",
			table:          "t4",
			partitionCols:  []string{"col_date"},
			customColRules: map[string]GenRule{},
			wantErr:        true,
		},
		{
			name:          "case-insensitive match",
			table:         "t5",
			partitionCols: []string{"reportDateTime"},
			customColRules: map[string]GenRule{
				"reportdatetime": {"min": "2020-01-01", "max": "2025-01-01"},
			},
			wantErr: false,
		},
		{
			name:          "case-insensitive match reverse",
			table:         "t6",
			partitionCols: []string{"reportdatetime"},
			customColRules: map[string]GenRule{
				"reportDateTime": {"min": "2020-01-01", "max": "2025-01-01"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePartitionColumns(tt.table, tt.partitionCols, tt.customColRules)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "PARTITION BY")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGendataUniqueKeyAutoInc(t *testing.T) {
	sql := `CREATE TABLE unique_test (
	id int NOT NULL,
	price double NOT NULL,
	create_date date NOT NULL,
	name varchar(50) NOT NULL,
	value int NOT NULL
) ENGINE=OLAP
UNIQUE KEY(id, price, create_date)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	generator.SetupGenRulesForTest(generator.GenRule{})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok, "SQL parser error")
	assert.NoError(t, p.ErrListener.LastErr)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), parser.GetUniqueKeyColumns(c))
	PickUniqueIncColumn(columns)

	// Only the first numeric/datetime (non-DATE) column should be marked unique
	assert.True(t, columns[0].Unique, "id should be unique (first numeric column)")
	assert.False(t, columns[1].Unique, "price should not be unique (only one column is picked)")
	assert.False(t, columns[2].Unique, "create_date should not be unique (DATE type excluded)")
	assert.False(t, columns[3].Unique, "name should not be unique")
	assert.False(t, columns[4].Unique, "value should not be unique")

	rows := int64(10)
	tg, err := NewTableGen("unique_test.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)

	// Only the chosen unique column should have inc gen rule
	assert.Contains(t, tg.ColGenRules["id"], "gen")
	assert.NotContains(t, tg.ColGenRules["price"], "gen")
	assert.NotContains(t, tg.ColGenRules["create_date"], "gen")
	// Non-unique columns should NOT have gen rule
	assert.NotContains(t, tg.ColGenRules["name"], "gen")
	assert.NotContains(t, tg.ColGenRules["value"], "gen")

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.Gen(context.Background(), w, tg.Rows, nil))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(strings.TrimSpace(b.String()), "\n")
	assert.Len(t, resultCSV, int(rows))

	// Verify id column generates incrementing values
	ids := make([]string, 0, rows)
	for _, line := range resultCSV {
		parts := strings.Split(line, string(gen.ColumnSeparator))
		assert.Len(t, parts, 5)
		ids = append(ids, parts[0])
	}
	// All ids should be unique (incrementing)
	idSet := make(map[string]struct{})
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	assert.Len(t, idSet, int(rows), "all ids should be unique")
}

func TestGendataUniqueKeyNoSuitableColumn(t *testing.T) {
	// If all unique key columns are string/DATE types, no column should be marked unique
	sql := `CREATE TABLE unique_str (
	create_date date NOT NULL,
	name varchar(50) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(create_date, name)
DISTRIBUTED BY HASH(create_date) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	generator.SetupGenRulesForTest(generator.GenRule{})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok)
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), parser.GetUniqueKeyColumns(c))
	PickUniqueIncColumn(columns)

	assert.False(t, columns[0].Unique, "create_date (DATE) should not be picked")
	assert.False(t, columns[1].Unique, "name (VARCHAR) should not be picked")

	rows := int64(5)
	tg, err := NewTableGen("unique_str.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)

	assert.NotContains(t, tg.ColGenRules["create_date"], "gen")
	assert.NotContains(t, tg.ColGenRules["name"], "gen")
}

func TestGendataUniqueKeyWithUserGen(t *testing.T) {
	// When user provides a custom gen rule, the auto inc should NOT override it
	sql := `CREATE TABLE unique_custom (
	id int NOT NULL,
	name varchar(50) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default:1");`

	generator.SetupGenRulesForTest(generator.GenRule{
		"tables": []any{
			generator.GenRule{
				"name": "unique_custom",
				"columns": []any{
					generator.GenRule{
						"name": "id",
						"gen":  generator.GenRule{"enum": []any{100, 200, 300}},
					},
				},
			},
		},
	})

	p := parser.NewParser("create-table", sql)
	c, ok := p.SupportedCreateStatement().(*parser.CreateTableContext)
	assert.True(t, ok, "SQL parser error")
	columns := ColumnsFromParsed(c.ColumnDefs().GetCols(), parser.GetUniqueKeyColumns(c))

	rows := int64(6)
	tg, err := NewTableGen("unique_custom.sql", c.GetName().GetText(), columns, nil, rows, true)
	assert.NoError(t, err)

	// User-defined gen (enum) should be used, not auto inc
	genRule := tg.ColGenRules["id"]["gen"].(generator.GenRule)
	assert.Contains(t, genRule, "enum")
	assert.NotContains(t, genRule, "inc")
}
