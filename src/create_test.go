package src

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/parser"
)

func TestCreateParserListener_AddReplicationNum(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		beCount int
		want    string // expected substring in the output
	}{
		{
			name: "add replication_num when no replication property exists",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "file_cache_ttl_seconds" = "0",
"is_being_synced" = "false",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "HOUR",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"dynamic_partition.start" = "-360",
"dynamic_partition.end" = "24",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "46",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "24",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"compression" = "ZSTD",
"estimate_partition_size" = "1TB",
"light_schema_change" = "true",
"compaction_policy" = "time_series",
"time_series_compaction_goal_size_mbytes" = "2048",
"time_series_compaction_file_count_threshold" = "1000",
"time_series_compaction_time_threshold_seconds" = "3600",
"time_series_compaction_empty_rowsets_threshold" = "5",
"time_series_compaction_level_threshold" = "1",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "true",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);`,
			beCount: 3,
			want:    `"replication_num" = "3"`,
		},
		{
			name: "add replication_num with beCount=1",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2"
);`,
			beCount: 1,
			want:    `"replication_num" = "1"`,
		},
		{
			name: "add replication_num with beCount=0 defaults to 1",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2"
);`,
			beCount: 0,
			want:    `"replication_num" = "1"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener := newCreateParserListener("test-sql", tt.beCount)
			p := parser.NewParser("test-sql", tt.sql, listener)

			result, err := p.ToSQL()
			assert.NoError(t, err)
			assert.Contains(t, result, tt.want, "output should contain replication_num property")
		})
	}
}

func TestCreateParserListener_ModifyReplicationAllocation(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		beCount int
		want    string // expected substring in the output
		notWant string // should not contain this
	}{
		{
			name: "modify replication_allocation to replication_num",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default:3"
);`,
			beCount: 3,
			want:    `"replication_num" = "3"`,
			notWant: "replication_allocation",
		},
		{
			name: "modify replication_allocation with value larger than beCount",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default:5"
);`,
			beCount: 3,
			want:    `"replication_num" = "3"`,
			notWant: "replication_allocation",
		},
		{
			name: "modify replication_allocation with value smaller than beCount",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default:1"
);`,
			beCount: 5,
			want:    `"replication_num" = "1"`,
			notWant: "replication_allocation",
		},
		{
			name: "modify existing replication_num",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "5"
);`,
			beCount: 3,
			want:    `"replication_num" = "3"`,
		},
		{
			name: "keep existing replication_num when beCount is larger",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "2"
);`,
			beCount: 5,
			want:    `"replication_num" = "2"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener := newCreateParserListener("test-sql", tt.beCount)
			p := parser.NewParser("test-sql", tt.sql, listener)

			result, err := p.ToSQL()
			assert.NoError(t, err)
			assert.Contains(t, result, tt.want, "output should contain expected replication_num property")
			if tt.notWant != "" {
				assert.NotContains(t, result, tt.notWant, "output should not contain replication_allocation")
			}
		})
	}
}

func TestCreateParserListener_WithOtherProperties(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		beCount int
		want    []string // expected substrings in the output
	}{
		{
			name: "preserve other properties when adding replication_num",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2",
    "bloom_filter_columns" = "id"
);`,
			beCount: 3,
			want: []string{
				`"storage_format" = "V2"`,
				`"bloom_filter_columns" = "id"`,
				`"replication_num" = "3"`,
			},
		},
		{
			name: "preserve other properties when modifying replication_allocation",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2",
    "replication_allocation" = "tag.location.default:3",
    "bloom_filter_columns" = "id"
);`,
			beCount: 2,
			want: []string{
				`"storage_format" = "V2"`,
				`"bloom_filter_columns" = "id"`,
				`"replication_num" = "2"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener := newCreateParserListener("test-sql", tt.beCount)
			p := parser.NewParser("test-sql", tt.sql, listener)

			result, err := p.ToSQL()
			assert.NoError(t, err)
			for _, want := range tt.want {
				assert.Contains(t, result, want, "output should contain: %s", want)
			}
		})
	}
}

func TestCreateParserListener_RemoveIgnoredProperties(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		beCount int
		want    []string
		notWant []string
	}{
		{
			name: "remove storage_vault_id property",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_vault_id" = "xxx",
    "replication_num" = "3"
);`,
			beCount: 3,
			want:    []string{`"replication_num" = "3"`},
			notWant: []string{"storage_vault_id"},
		},
		{
			name: "remove storage_vault_id when it is the last property",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "storage_vault_id" = "xxx"
);`,
			beCount: 3,
			want:    []string{`"replication_num" = "3"`},
			notWant: []string{"storage_vault_id"},
		},
		{
			name: "remove storage_vault_id among multiple properties",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2",
    "storage_vault_id" = "xxx",
    "bloom_filter_columns" = "id"
);`,
			beCount: 3,
			want: []string{
				`"storage_format" = "V2"`,
				`"bloom_filter_columns" = "id"`,
				`"replication_num" = "3"`,
			},
			notWant: []string{"storage_vault_id"},
		},
		{
			name: "no ignored properties present",
			sql: `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2",
    "replication_num" = "3"
);`,
			beCount: 3,
			want:    []string{`"storage_format" = "V2"`, `"replication_num" = "3"`},
			notWant: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener := newCreateParserListener("test-sql", tt.beCount)
			p := parser.NewParser("test-sql", tt.sql, listener)

			result, err := p.ToSQL()
			assert.NoError(t, err)
			for _, want := range tt.want {
				assert.Contains(t, result, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, result, notWant)
			}
		})
	}
}

func TestCreateParserListener_GetTextFromInterval(t *testing.T) {
	// Test that the modified SQL can be retrieved using GetTextFromInterval
	sql := `CREATE TABLE test_table (
    id INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default:3"
);`

	listener := newCreateParserListener("test-sql", 2)
	p := parser.NewParser("test-sql", sql, listener)

	multiStmts, err := p.Parse()
	assert.NoError(t, err)

	for _, s_ := range multiStmts.AllStatement() {
		s, ok := s_.(*parser.StatementBaseAliasContext)
		if !ok {
			continue
		}

		interval := antlr.NewInterval(s.GetStart().GetTokenIndex(), s.GetStop().GetTokenIndex())
		stmt := p.GetTokenStream().GetTextFromInterval(interval)

		assert.Contains(t, stmt, `"replication_num" = "2"`)
		assert.NotContains(t, stmt, "replication_allocation")
	}
}

func TestCreateParserListener_OnlyModifyTableLevelProperties(t *testing.T) {
	const sql = `CREATE TABLE test_table (
    id INT,
    payload VARIANT<PROPERTIES("variant_enabled" = "true")>
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "storage_format" = "V2"
);`

	listener := newCreateParserListener("test-sql", 3)
	p := parser.NewParser("test-sql", sql, listener)

	result, err := p.ToSQL()
	assert.NoError(t, err)
	assert.Contains(t, result, `VARIANT<PROPERTIES("variant_enabled" = "true")>`)
	assert.NotContains(t, result, `VARIANT<PROPERTIES("variant_enabled" = "true", "replication_num" = "3")>`)
	assert.Contains(t, result, `PROPERTIES (
    "storage_format" = "V2", "replication_num" = "3"`)
}

func TestCreateParserListener_DoesNotAddReplicationNumToColumnPropertiesWithoutTableProperties(t *testing.T) {
	const sql = `CREATE TABLE test_table (
    id INT,
    payload VARIANT<PROPERTIES("variant_enabled" = "true")>
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10;`

	listener := newCreateParserListener("test-sql", 3)
	p := parser.NewParser("test-sql", sql, listener)

	result, err := p.ToSQL()
	assert.NoError(t, err)
	assert.Contains(t, result, `VARIANT<PROPERTIES("variant_enabled" = "true")>`)
	assert.NotContains(t, result, `replication_num`)
}
