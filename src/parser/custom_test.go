package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModifyProperties(t *testing.T) {
	sql := `/*
multi
line 
comment
	*/CREATE TABLE t1 (-- comment
dt_month varchar(6) NULL COMMENT "",
company_code varchar(40) NULL COMMENT 'c'
) ENGINE=OLAP
DUPLICATE KEY(dt_month)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(dt_month) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1",
'bloom_filter_columns' = "dt_month, company_code"
);
select count(dt_month), data from t1`

	p := NewParser("1", sql, NewListener(true, func(_ string) string { return "foo" }))
	s, err := p.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `CREATE TABLE foo (
foo varchar(6) NULL COMMENT "",
foo varchar(40) NULL COMMENT '*'
) ENGINE=OLAP
DUPLICATE KEY(foo)
COMMENT '****'
DISTRIBUTED BY HASH(foo) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1",
'bloom_filter_columns' = "foo,foo"
);
select count(foo), foo from foo`, s)
}

func TestParser(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t1 (
			dt_month varchar(6) NULL,
			company_code varchar(40) NULL,
			company_name varchar(100) NULL,
			some_code varchar(20) NULL COMMENT 'asdas',
			rating varchar(50) NULL
		) ENGINE=OLAP
		DUPLICATE KEY(dt_month)
		COMMENT 'OLAP'
		DISTRIBUTED BY HASH(dt_month) BUCKETS 10
		PROPERTIES (
			"replication_allocation" = "tag.location.default:1",
			"min_load_replica_num" = "-1",
			"is_being_synced" = "false",
			"storage_medium" = "hdd",
			"storage_format" = "V2",
			"inverted_index_storage_format" = "V1",
			"light_schema_change" = "true",
			"disable_auto_compaction" = "false",
			"binlog.enable" = "false",
			"binlog.ttl_seconds" = "86400",
			"binlog.max_bytes" = "9223372036854775807",
			"binlog.max_history_nums" = "9223372036854775807",
			"enable_single_replica_compaction" = "false",
			"group_commit_interval_ms" = "10000",
			"group_commit_data_bytes" = "134217728"
		);`,
		`SELECT  T2.col_bigint_undef_signed2 AS C1 ,  T2.col_bigint_undef_signed AS C2 ,  T2.col_bigint_undef_signed2 AS C3 ,  T2.col_bigint_undef_signed2 AS C4 ,  T1.pk AS C5 ,  T2.col_bigint_undef_signed2 AS C6 ,  T2.pk AS C7   FROM table_50_undef_partitions2_keys3_properties4_distributed_by53 AS T1  FULL OUTER JOIN  table_50_undef_partitions2_keys3_properties4_distributed_by53 AS T2 ON T1.col_bigint_undef_signed2  >  T2.col_bigint_undef_signed   OR  T1.col_bigint_undef_signed2  <=>  1 + 2 ORDER BY C1, C2, C3, C4, C5, C6, C7  DESC;`,
		"select day(`c`) from `t`; select `TABLE_NAME`, `COLUMN_NAME` from `information_schema`.`columns`                                     where table_schema = 'db_haixin'                                     order by table_name,ordinal_position",
		`select @@abc, GLoBAL.abc, @abc, abc (asdad), ADD(1), json_extract(data,"$.foo1") from table1`,
	}

	for _, sql := range sqls {
		p := NewParser("1", sql, NewListener(false, func(s string) string { return s }))

		sql = strings.ReplaceAll(sql, "`", "")
		s, err := p.ToSQL()
		assert.NoError(t, err)
		assert.Equal(t, sql, s)
	}
}

func TestGetTableNameAndCols(t *testing.T) {
	sql := `
CREATE TABLE table_500_undef_partitions2_keys3_properties4_distributed_by56 (
  col_int_undef_signed int NULL,
  col_date_undef_signed date NULL,
  pk int NULL,
  col_int_undef_signed2 int NULL,
  col_date_undef_signed2 date NULL,
  col_varchar_10__undef_signed varchar(10) NULL,
  col_varchar_1024__undef_signed varchar(1024) NULL
) ENGINE=OLAP
DUPLICATE KEY(col_int_undef_signed, col_date_undef_signed, pk)
PARTITION BY RANGE(col_int_undef_signed, col_date_undef_signed)
(PARTITION p0 VALUES [("-2147483648", '0000-01-01'), ("4", '2023-12-11')),
PARTITION p1 VALUES [("4", '2023-12-11'), ("6", '2023-12-15')),
PARTITION p2 VALUES [("6", '2023-12-15'), ("7", '2023-12-16')),
PARTITION p3 VALUES [("7", '2023-12-16'), ("8", '2023-12-25')),
PARTITION p4 VALUES [("8", '2023-12-25'), ("8", '2024-01-18')),
PARTITION p5 VALUES [("8", '2024-01-18'), ("10", '2024-02-18')),
PARTITION p6 VALUES [("10", '2024-02-18'), ("1147483647", '2056-12-31')),
PARTITION p100 VALUES [("1147483647", '2056-12-31'), ("2147483647", '9999-12-31')))
DISTRIBUTED BY HASH(pk) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
	`
	tableName, cols, err := GetTableNameAndCols("1", sql)
	assert.NoError(t, err)
	assert.Equal(t, "table_500_undef_partitions2_keys3_properties4_distributed_by56", tableName)
	assert.Equal(t, []string{
		"col_int_undef_signed",
		"col_date_undef_signed",
		"pk",
		"col_int_undef_signed2",
		"col_date_undef_signed2",
		"col_varchar_10__undef_signed",
		"col_varchar_1024__undef_signed",
	}, cols)

}

func TestGetPartitionColumns(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "no partition",
			sql: `CREATE TABLE t1 (
				a int NULL,
				b varchar(10) NULL
			) ENGINE=OLAP
			DUPLICATE KEY(a)
			DISTRIBUTED BY HASH(a) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: nil,
		},
		{
			name: "range partition single column",
			sql: `CREATE TABLE t2 (
				col_date date NULL,
				col_int int NULL
			) ENGINE=OLAP
			DUPLICATE KEY(col_date)
			PARTITION BY RANGE (date_trunc(` + "`col_date`" + `, 'month'))
			(PARTITION p1 VALUES [('2020-01-01'), ('2021-01-01')))
			DISTRIBUTED BY HASH(col_date) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"col_date"},
		},
		{
			name: "range partition multiple columns",
			sql: `CREATE TABLE t3 (
				col_int int NULL,
				col_date date NULL,
				pk int NULL
			) ENGINE=OLAP
			DUPLICATE KEY(col_int, col_date, pk)
			PARTITION BY RANGE(col_int, col_date)
			(PARTITION p0 VALUES [("-2147483648", '0000-01-01'), ("4", '2023-12-11')),
			PARTITION p1 VALUES [("4", '2023-12-11'), ("6", '2023-12-15')))
			DISTRIBUTED BY HASH(pk) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"col_int", "col_date"},
		},
		{
			name: "list partition",
			sql: `CREATE TABLE t4 (
				city varchar(50) NULL,
				id int NULL
			) ENGINE=OLAP
			DUPLICATE KEY(city)
			PARTITION BY LIST(city)
			(PARTITION p1 VALUES IN ("Beijing", "Shanghai"))
			DISTRIBUTED BY HASH(city) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"city"},
		},
		{
			name: "auto partition with function",
			sql: `CREATE TABLE t5 (
				dt datetime NULL,
				id int NULL
			) ENGINE=OLAP
			DUPLICATE KEY(dt)
			AUTO PARTITION BY RANGE(date_trunc(dt, 'day'))
			()
			DISTRIBUTED BY HASH(dt) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"dt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser("test", tt.sql)
			c, ok := p.SupportedCreateStatement().(*CreateTableContext)
			assert.True(t, ok, "SQL parser error")
			assert.NoError(t, p.ErrListener.LastErr)

			cols := GetPartitionColumns(c)
			assert.Equal(t, tt.expected, cols)
		})
	}
}

func TestGetUniqueKeyColumns(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "unique key single column",
			sql: `CREATE TABLE t1 (
				id int NOT NULL,
				name varchar(50) NULL
			) ENGINE=OLAP
			UNIQUE KEY(id)
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"id"},
		},
		{
			name: "unique key multiple columns",
			sql: `CREATE TABLE t2 (
				id int NOT NULL,
				dt date NOT NULL,
				value double NULL
			) ENGINE=OLAP
			UNIQUE KEY(` + "`id`, `dt`" + `)
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"id", "dt"},
		},
		{
			name: "duplicate key returns nil",
			sql: `CREATE TABLE t3 (
				id int NOT NULL,
				name varchar(50) NULL
			) ENGINE=OLAP
			DUPLICATE KEY(id)
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: nil,
		},
		{
			name: "aggregate key returns nil",
			sql: `CREATE TABLE t4 (
				id int NOT NULL,
				cnt bigint SUM
			) ENGINE=OLAP
			AGGREGATE KEY(id)
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: nil,
		},
		{
			name: "no key clause returns nil",
			sql: `CREATE TABLE t5 (
				id int NOT NULL,
				name varchar(50) NULL
			) ENGINE=OLAP
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: nil,
		},
		{
			name:     "unique key with backtick-quoted columns",
			sql:      "CREATE TABLE t6 (\n\t\t\t\t`user_id` int NOT NULL,\n\t\t\t\t`order_date` date NOT NULL,\n\t\t\t\tamount double NULL\n\t\t\t) ENGINE=OLAP\n\t\t\tUNIQUE KEY(`user_id`, `order_date`)\n\t\t\tDISTRIBUTED BY HASH(`user_id`) BUCKETS 10\n\t\t\tPROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\");",
			expected: []string{"user_id", "order_date"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser("test", tt.sql)
			c, ok := p.SupportedCreateStatement().(*CreateTableContext)
			assert.True(t, ok, "SQL parser error")
			assert.NoError(t, p.ErrListener.LastErr)

			cols := GetUniqueKeyColumns(c)
			assert.Equal(t, tt.expected, cols)
		})
	}
}

func TestExtractColumnReferences(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string // columns that must be present
		absent   []string // identifiers that must NOT be present (e.g. table names)
	}{
		{
			name:     "simple WHERE",
			sql:      "SELECT * FROM orders WHERE status = 1 AND `price` > 100",
			expected: []string{"status", "price"},
		},
		{
			name:     "qualified column references",
			sql:      "SELECT t1.`user_id`, t2.uid FROM t1 JOIN t2 ON t1.`order_id` = t2.`order_id`",
			expected: []string{"user_id", "uid", "order_id"},
		},
		{
			name:     "GROUP BY and HAVING",
			sql:      "SELECT biz_id, COUNT(*) FROM orders GROUP BY biz_id HAVING SUM(amount) > 100",
			expected: []string{"biz_id", "amount"},
		},
		{
			name:     "ORDER BY",
			sql:      "SELECT name, age FROM users ORDER BY created_at DESC",
			expected: []string{"name", "age", "created_at"},
		},
		{
			name:     "subquery",
			sql:      "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)",
			expected: []string{"user_id", "id", "active"},
		},
		{
			name:     "function arguments",
			sql:      "SELECT date_trunc(`dt`, 'day'), SUM(amount) FROM orders GROUP BY date_trunc(`dt`, 'day')",
			expected: []string{"dt", "amount"},
		},
		{
			name:     "multiple statements",
			sql:      "SELECT a FROM t1; SELECT b FROM t2 WHERE c = 1",
			expected: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := ExtractColumnReferences("test", tt.sql)
			assert.NotNil(t, cols)
			for _, col := range tt.expected {
				assert.True(t, cols[col], "expected column %q not found in %v", col, cols)
			}
			for _, col := range tt.absent {
				assert.False(t, cols[col], "unexpected identifier %q found in %v", col, cols)
			}
		})
	}
}

func TestGetAllStructuralColumns(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "unique key + partition + distributed",
			sql: `CREATE TABLE t1 (
				id int NOT NULL,
				dt date NOT NULL,
				name varchar(50) NULL,
				value double NULL
			) ENGINE=OLAP
			UNIQUE KEY(id, dt)
			PARTITION BY RANGE(dt)
			(PARTITION p1 VALUES [('2020-01-01'), ('2021-01-01')))
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"id", "dt"},
		},
		{
			name: "aggregate key + distributed by different col",
			sql: `CREATE TABLE t2 (
				user_id int NOT NULL,
				city varchar(50) NULL,
				cnt bigint SUM
			) ENGINE=OLAP
			AGGREGATE KEY(user_id)
			DISTRIBUTED BY HASH(city) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"user_id", "city"},
		},
		{
			name: "duplicate key + auto partition with function",
			sql: `CREATE TABLE t3 (
				dt datetime NULL,
				id int NULL,
				data varchar(100) NULL
			) ENGINE=OLAP
			DUPLICATE KEY(dt)
			AUTO PARTITION BY RANGE(date_trunc(dt, 'day'))
			()
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"dt", "id"},
		},
		{
			name: "no key clause",
			sql: `CREATE TABLE t4 (
				id int NOT NULL,
				name varchar(50) NULL
			) ENGINE=OLAP
			DISTRIBUTED BY HASH(id) BUCKETS 10
			PROPERTIES ("replication_allocation" = "tag.location.default: 1");`,
			expected: []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser("test", tt.sql)
			c, ok := p.SupportedCreateStatement().(*CreateTableContext)
			assert.True(t, ok, "SQL parser error")
			assert.NoError(t, p.ErrListener.LastErr)

			cols := GetAllStructuralColumns(c)
			for _, ex := range tt.expected {
				assert.True(t, cols[ex], "expected structural column %q not found in %v", ex, cols)
			}
		})
	}
}

func TestFilterDDLColumns(t *testing.T) {
	tests := []struct {
		name             string
		ddl              string
		keepCols         map[string]bool
		expectContains   []string
		expectNotContain []string
	}{
		{
			name: "nil keepCols returns original",
			ddl: `CREATE TABLE t1 (
  id int NOT NULL,
  name varchar(50) NULL,
  extra int NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			keepCols:       nil,
			expectContains: []string{"id", "name", "extra"},
		},
		{
			name: "keeps structural + query columns, omits others",
			ddl: `CREATE TABLE t1 (
  id int NOT NULL,
  status int NULL,
  name varchar(50) NULL,
  description text NULL,
  extra1 int NULL,
  extra2 int NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			keepCols:         map[string]bool{"status": true},
			expectContains:   []string{"id", "status", "4 more columns omitted"},
			expectNotContain: []string{"description", "extra1", "extra2"},
		},
		{
			name: "keeps partition cols even if not in query",
			ddl: `CREATE TABLE t2 (
  id int NOT NULL,
  dt date NOT NULL,
  price decimal(10,2) NULL,
  unused1 int NULL,
  unused2 varchar(100) NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
PARTITION BY RANGE(dt)
(PARTITION p1 VALUES [('2020-01-01'), ('2021-01-01')))
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			keepCols:         map[string]bool{"price": true},
			expectContains:   []string{"id", "dt", "price", "2 more columns omitted"},
			expectNotContain: []string{"unused1", "unused2"},
		},
		{
			name: "all columns in query returns unmodified",
			ddl: `CREATE TABLE t3 (
  a int NOT NULL,
  b int NULL
) ENGINE=OLAP DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 10;`,
			keepCols:       map[string]bool{"a": true, "b": true},
			expectContains: []string{"a int", "b int"},
		},
		{
			name: "preserves ENGINE/KEY/DISTRIBUTED after columns",
			ddl: `CREATE TABLE t4 (
  id int NOT NULL,
  name varchar(50) NULL,
  unused int NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			keepCols:         map[string]bool{"name": true},
			expectContains:   []string{"id", "name", "ENGINE=OLAP", "UNIQUE KEY", "DISTRIBUTED BY"},
			expectNotContain: []string{"`unused`"},
		},
		{
			name: "multi-byte characters in COMMENT do not corrupt output",
			ddl: "CREATE TABLE `t5` (\n" +
				"  `id` bigint NOT NULL,\n" +
				"  `supplier_code` varchar(150) NULL COMMENT \"供应商编码\",\n" +
				"  `supplier_name` varchar(200) NULL COMMENT \"供应商名称\",\n" +
				"  `price` decimal(16,6) NOT NULL,\n" +
				"  `unused1` int NULL,\n" +
				"  `unused2` varchar(60) NULL\n" +
				") ENGINE=OLAP\n" +
				"UNIQUE KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 3;",
			keepCols:         map[string]bool{"supplier_code": true, "price": true},
			expectContains:   []string{"`id`", "`supplier_code` varchar(150) NULL COMMENT \"供应商编码\"", "`price` decimal(16,6) NOT NULL", "3 more columns omitted"},
			expectNotContain: []string{"supplier_name", "unused1", "unused2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FilterDDLColumns("test", tt.ddl, tt.keepCols)
			for _, s := range tt.expectContains {
				assert.Contains(t, result, s, "expected %q in result:\n%s", s, result)
			}
			for _, s := range tt.expectNotContain {
				assert.NotContains(t, result, s, "unexpected %q in result:\n%s", s, result)
			}
		})
	}
}

func TestFilterDDLColumnsTopN(t *testing.T) {
	tests := []struct {
		name             string
		ddl              string
		topN             int
		expectContains   []string
		expectNotContain []string
	}{
		{
			name: "fewer columns than topN returns unmodified",
			ddl: `CREATE TABLE t1 (
  id int NOT NULL,
  name varchar(50) NULL,
  status int NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			topN:           10,
			expectContains: []string{"id", "name", "status"},
		},
		{
			name: "keeps first 3 + structural, omits rest",
			ddl: `CREATE TABLE t1 (
  id int NOT NULL,
  col1 int NULL,
  col2 int NULL,
  col3 int NULL,
  col4 int NULL,
  col5 int NULL,
  col6 int NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;`,
			topN:             3,
			expectContains:   []string{"id", "col1", "col2", "4 more columns omitted"},
			expectNotContain: []string{"col3", "col4", "col5", "col6"},
		},
		{
			name: "structural columns kept even if beyond topN",
			ddl: `CREATE TABLE t2 (
  a int NULL,
  b int NULL,
  c int NULL,
  d int NULL,
  part_key date NOT NULL,
  hash_col int NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(a)
PARTITION BY RANGE(part_key)
(PARTITION p1 VALUES [('2020-01-01'), ('2021-01-01')))
DISTRIBUTED BY HASH(hash_col) BUCKETS 10;`,
			topN:             2,
			expectContains:   []string{"a", "b", "part_key", "hash_col", "2 more columns omitted"},
			expectNotContain: []string{"`c`", "`d`"},
		},
		{
			name: "topN zero defaults to 10",
			ddl: `CREATE TABLE t3 (
  c0 int, c1 int, c2 int, c3 int, c4 int,
  c5 int, c6 int, c7 int, c8 int, c9 int,
  c10 int, c11 int
) ENGINE=OLAP DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 10;`,
			topN:             0,
			expectContains:   []string{"c0", "c9", "2 more columns omitted"},
			expectNotContain: []string{"`c10`", "`c11`"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FilterDDLColumnsTopN("test", tt.ddl, tt.topN)
			for _, s := range tt.expectContains {
				assert.Contains(t, result, s, "expected %q in result:\n%s", s, result)
			}
			for _, s := range tt.expectNotContain {
				assert.NotContains(t, result, s, "unexpected %q in result:\n%s", s, result)
			}
		})
	}
}
