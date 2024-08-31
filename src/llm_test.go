package src

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
)

// Note: TestExtractTableNameFromDDL and TestParseDBTableFromFileName were removed
// because the functions were moved to cmd/gendata.go during refactoring.
// The parsing logic is now tested indirectly via integration tests.

func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

func TestTrimDDLPartitions(t *testing.T) {
	// Helper to build a DDL with N partitions
	buildDDL := func(n int) string {
		var b strings.Builder
		b.WriteString("CREATE TABLE test.t1 (\n  `id` INT,\n  `dt` DATE\n)\nPARTITION BY RANGE(`dt`) (\n")
		for i := range n {
			if i > 0 {
				b.WriteString(",\n")
			}
			b.WriteString(fmt.Sprintf("PARTITION p_%04d VALUES [('2025-01-%02d'), ('2025-01-%02d'))", i, i+1, i+2))
		}
		b.WriteString("\n)\nDISTRIBUTED BY HASH(`id`) BUCKETS 8;")
		return b.String()
	}

	t.Run("no trim when partitions <= maxKeep", func(t *testing.T) {
		ddl := buildDDL(5)
		result := TrimDDLPartitions(ddl, 5)
		assert.Equal(t, ddl, result)
	})

	t.Run("no trim when no partitions", func(t *testing.T) {
		ddl := "CREATE TABLE t1 (id INT) DISTRIBUTED BY HASH(id) BUCKETS 8;"
		result := TrimDDLPartitions(ddl, 5)
		assert.Equal(t, ddl, result)
	})

	t.Run("trims excessive partitions", func(t *testing.T) {
		ddl := buildDDL(30)
		result := TrimDDLPartitions(ddl, 5)

		// Should have significantly fewer lines
		assert.Less(t, len(result), len(ddl))

		// Should contain the summary comment
		assert.Contains(t, result, "-- ... 26 more partitions omitted ...")

		// First 2 and last 2 partitions should still be present
		assert.Contains(t, result, "PARTITION p_0000")
		assert.Contains(t, result, "PARTITION p_0001")
		assert.Contains(t, result, "PARTITION p_0028")
		assert.Contains(t, result, "PARTITION p_0029")

		// Middle partition should NOT be present
		assert.NotContains(t, result, "PARTITION p_0015")

		// DDL structure should remain intact
		assert.Contains(t, result, "CREATE TABLE")
		assert.Contains(t, result, "DISTRIBUTED BY HASH")
	})

	t.Run("default maxKeep when 0", func(t *testing.T) {
		ddl := buildDDL(20)
		result := TrimDDLPartitions(ddl, 0)
		assert.Contains(t, result, "omitted")
	})

	t.Run("real-world partition format", func(t *testing.T) {
		ddl := `CREATE TABLE journal.orders (
  ` + "`id`" + ` BIGINT,
  ` + "`dt`" + ` DATE
)
PARTITION BY RANGE(` + "`dt`" + `) (
PARTITION p_20251023 VALUES [('2025-10-23'), ('2025-10-24')),
PARTITION p_20251024 VALUES [('2025-10-24'), ('2025-10-25')),
PARTITION p_20251025 VALUES [('2025-10-25'), ('2025-10-26')),
PARTITION p_20251026 VALUES [('2025-10-26'), ('2025-10-27')),
PARTITION p_20251027 VALUES [('2025-10-27'), ('2025-10-28')),
PARTITION p_20251028 VALUES [('2025-10-28'), ('2025-10-29')),
PARTITION p_20251029 VALUES [('2025-10-29'), ('2025-10-30')),
PARTITION p_20251030 VALUES [('2025-10-30'), ('2025-10-31')),
PARTITION p_20251031 VALUES [('2025-10-31'), ('2025-11-01')),
PARTITION p_20251101 VALUES [('2025-11-01'), ('2025-11-02')),
PARTITION p_20251102 VALUES [('2025-11-02'), ('2025-11-03')),
PARTITION p_20251103 VALUES [('2025-11-03'), ('2025-11-04'))
)
DISTRIBUTED BY HASH(` + "`id`" + `) BUCKETS 8;`

		result := TrimDDLPartitions(ddl, 4)
		assert.Contains(t, result, "PARTITION p_20251023")
		assert.Contains(t, result, "PARTITION p_20251024")
		assert.Contains(t, result, "PARTITION p_20251102")
		assert.Contains(t, result, "PARTITION p_20251103")
		assert.Contains(t, result, "-- ... 8 more partitions omitted ...")
		assert.NotContains(t, result, "PARTITION p_20251027")
	})
}

func TestTrimDDLForLLM(t *testing.T) {
	t.Run("strips PROPERTIES block", func(t *testing.T) {
		ddl := "CREATE TABLE `orders` (\n" +
			"  `id` bigint NOT NULL COMMENT \"用户id\",\n" +
			"  `status` bigint NOT NULL COMMENT \"订单状态：1 失败，2 撤单完成\",\n" +
			"  `price` decimal(38,18) NULL COMMENT \"委托价格\"\n" +
			") ENGINE=OLAP\n" +
			"UNIQUE KEY(`id`)\n" +
			"DISTRIBUTED BY HASH(`id`) BUCKETS 8\n" +
			"PROPERTIES (\n" +
			"\"replication_num\" = \"3\",\n" +
			"\"compaction_policy\" = \"time_series\"\n" +
			");"

		result := TrimDDLForLLM(ddl)

		// Column definitions with COMMENTs must be preserved
		assert.Contains(t, result, "`id` bigint NOT NULL COMMENT \"用户id\"")
		assert.Contains(t, result, "`status` bigint NOT NULL COMMENT \"订单状态：1 失败，2 撤单完成\"")
		assert.Contains(t, result, "`price` decimal(38,18) NULL COMMENT \"委托价格\"")
		assert.Contains(t, result, "CREATE TABLE")

		// ENGINE/KEY/DISTRIBUTED are preserved
		assert.Contains(t, result, "ENGINE=OLAP")
		assert.Contains(t, result, "UNIQUE KEY")
		assert.Contains(t, result, "DISTRIBUTED BY")

		// Only PROPERTIES block is stripped
		assert.NotContains(t, result, "PROPERTIES")
		assert.NotContains(t, result, "replication_num")
		assert.NotContains(t, result, "compaction_policy")
	})

	t.Run("strips PARTITIONS and PROPERTIES but keeps first/last partitions", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("CREATE TABLE t (\n  `id` INT,\n  `dt` DATE\n")
		b.WriteString(") ENGINE=OLAP\n")
		b.WriteString("DUPLICATE KEY(`id`)\n")
		b.WriteString("AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))\n(\n")
		for i := range 20 {
			if i > 0 {
				b.WriteString(",\n")
			}
			b.WriteString(fmt.Sprintf("PARTITION p_%04d VALUES [('2025-01-%02d'), ('2025-01-%02d'))", i, i+1, i+2))
		}
		b.WriteString("\n)\nDISTRIBUTED BY HASH(`id`) BUCKETS 8\n")
		b.WriteString("PROPERTIES (\"replication_num\" = \"3\");")

		result := TrimDDLForLLM(b.String())

		// Column definitions should remain
		assert.Contains(t, result, "CREATE TABLE")
		assert.Contains(t, result, "`id` INT")

		// AUTO PARTITION BY RANGE and first/last partitions should be preserved
		assert.Contains(t, result, "AUTO PARTITION BY RANGE")
		assert.Contains(t, result, "PARTITION p_0000")
		assert.Contains(t, result, "PARTITION p_0001")
		assert.NotContains(t, result, "PARTITION p_0002")
		assert.Contains(t, result, "PARTITION p_0018")
		assert.Contains(t, result, "PARTITION p_0019")
		assert.Contains(t, result, "omitted")

		// Middle partition should be trimmed
		assert.NotContains(t, result, "PARTITION p_0010")

		// Closing paren should remain (from ") ENGINE=OLAP")
		assert.Contains(t, result, ")")

		// PROPERTIES stripped
		assert.NotContains(t, result, "PROPERTIES")
		assert.NotContains(t, result, "replication_num")

		// ENGINE/KEY/DISTRIBUTED preserved
		assert.Contains(t, result, "ENGINE=OLAP")
		assert.Contains(t, result, "DUPLICATE KEY")
		assert.Contains(t, result, "DISTRIBUTED BY")
	})

	t.Run("no-op for simple DDL", func(t *testing.T) {
		ddl := "CREATE TABLE t (\n  `id` INT,\n  `name` VARCHAR(100)\n)"
		result := TrimDDLForLLM(ddl)
		assert.Contains(t, result, "CREATE TABLE")
		assert.Contains(t, result, "`id` INT")
		assert.Contains(t, result, "`name` VARCHAR(100)")
	})
}

func TestExtractColumnsFromSQL(t *testing.T) {
	t.Run("backtick and bare identifiers", func(t *testing.T) {
		sqls := []string{
			"SELECT * FROM t1 JOIN t2 ON t1.`user_id` = t2.uid WHERE status = 1 AND `price` > 100",
		}
		cols := ExtractColumnsFromSQL(sqls)
		assert.True(t, cols["user_id"])
		assert.True(t, cols["uid"])
		assert.True(t, cols["status"])
		assert.True(t, cols["price"])
	})

	t.Run("empty sqls", func(t *testing.T) {
		cols := ExtractColumnsFromSQL(nil)
		assert.Empty(t, cols)
	})

	t.Run("GROUP BY and HAVING", func(t *testing.T) {
		sqls := []string{
			"SELECT biz_id, COUNT(*) FROM orders GROUP BY biz_id HAVING SUM(amount) > 100",
		}
		cols := ExtractColumnsFromSQL(sqls)
		assert.True(t, cols["biz_id"])
		assert.True(t, cols["amount"])
	})
}

func TestTrimStatsForLLM(t *testing.T) {
	t.Run("strips data_size and method", func(t *testing.T) {
		statsYAML := `name: orders
row_count: 1000
columns:
  - name: id
    ndv: 500
    null_count: 0
    data_size: 4000
    avg_size_byte: 8
    min: "1"
    max: "1000"
    method: FULL
  - name: price
    ndv: 200
    null_count: 10
    data_size: 16000
    avg_size_byte: 16
    min: "0.5"
    max: "99999.99"
    method: FULL`

		result := TrimStatsForLLM(statsYAML)

		// Kept fields
		assert.Contains(t, result, "name: orders")
		assert.Contains(t, result, "row_count: 1000")
		assert.Contains(t, result, "ndv: 500")
		assert.Contains(t, result, "null_count: 0")
		assert.Contains(t, result, "avg_size_byte: 8")
		assert.Contains(t, result, "min: \"1\"")
		assert.Contains(t, result, "max: \"1000\"")

		// Stripped fields
		assert.NotContains(t, result, "data_size:")
		assert.NotContains(t, result, "method:")
	})

	t.Run("empty input", func(t *testing.T) {
		assert.Equal(t, "", TrimStatsForLLM(""))
	})

	t.Run("stats without data_size/method", func(t *testing.T) {
		statsYAML := "name: t1\nrow_count: 100\ncolumns:\n  - name: a\n    ndv: 10\n    min: \"1\"\n    max: \"10\""
		result := TrimStatsForLLM(statsYAML)
		assert.Contains(t, result, "name: t1")
		assert.Contains(t, result, "ndv: 10")
	})
}

func TestFilterStatsColumns(t *testing.T) {
	statsYAML := `name: orders
row_count: 1000
columns:
  - name: id
    ndv: 500
    min: "1"
    max: "1000"
  - name: status
    ndv: 5
    min: "1"
    max: "5"
  - name: description
    ndv: 800
    avg_size_byte: 50
  - name: extra
    ndv: 100
    min: "0"
    max: "99"`

	t.Run("nil keepCols returns unchanged", func(t *testing.T) {
		result := filterStatsColumns(statsYAML, nil)
		assert.Equal(t, statsYAML, result)
	})

	t.Run("filters to kept columns only", func(t *testing.T) {
		keepCols := map[string]bool{"id": true, "status": true}
		result := filterStatsColumns(statsYAML, keepCols)
		assert.Contains(t, result, "name: id")
		assert.Contains(t, result, "name: status")
		assert.NotContains(t, result, "name: description")
		assert.NotContains(t, result, "name: extra")
		// Header still present
		assert.Contains(t, result, "name: orders")
		assert.Contains(t, result, "row_count: 1000")
	})

	t.Run("empty stats returns empty", func(t *testing.T) {
		result := filterStatsColumns("", map[string]bool{"id": true})
		assert.Equal(t, "", result)
	})
}

// TestLLMGendataConfigIntegration is an integration test that requires API key
// Skip this test in CI by checking for API key
func TestLLMGendataConfigIntegration(t *testing.T) {
	t.Skip("Integration test - requires API key")

	apiKey := "" // Set your API key here for manual testing
	if apiKey == "" {
		t.Skip("API key not set")
	}

	// Create temp DDL files
	tmpDir := t.TempDir()

	ddl1 := `CREATE TABLE t1 (
		a int NULL,
		c varchar(10) NULL,
		other_col string NOT NULL
	) ENGINE=OLAP DUPLICATE KEY(a);`
	ddl2 := `CREATE TABLE t2 (
		b int NULL,
		d varchar(10) NULL
	) ENGINE=OLAP DUPLICATE KEY(b);`
	stats1 := "name: t1\nrow_count: 1000\ncolumns:\n  - name: a\n    min: \"10\"\n    max: \"30\""

	ddlFile1 := tmpDir + "/db1.t1.table.sql"
	ddlFile2 := tmpDir + "/db1.t2.table.sql"
	statsFile := tmpDir + "/db1.stats.yaml"

	_ = writeTestFile(ddlFile1, ddl1)
	_ = writeTestFile(ddlFile2, ddl2)
	_ = writeTestFile(statsFile, stats1)

	// Create LLMTableInput directly (no file reading in llm.go anymore)
	tables := []LLMTableInput{
		{
			DB:           "db1",
			Name:         "t1",
			Type:         "table",
			DDLContent:   ddl1,
			StatsContent: stats1,
		},
		{
			DB:           "db1",
			Name:         "t2",
			Type:         "table",
			DDLContent:   ddl2,
			StatsContent: "",
		},
	}
	sqls := []string{
		`SELECT * FROM t1 JOIN t2 ON t1.a = t2.b WHERE c IN ("a", "b", "c") AND d = 1`,
	}

	ctx := context.Background()
	result, err := LLMGendataConfig(ctx, apiKey, "", "deepseek-v4-flash", "", tables, sqls, nil)
	if err != nil {
		t.Fatalf("LLMGendataConfig error: %v", err)
	}

	t.Log("Result:\n", result)

	// Basic validation - should contain tables config
	if result == "" {
		t.Error("Expected non-empty result")
	}
}

func TestLLMExtractTablesResultParsing(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected []string
		wantErr  bool
	}{
		{
			name:     "simple tables",
			json:     `{"tables": ["db1.t1", "db2.t2"]}`,
			expected: []string{"db1.t1", "db2.t2"},
			wantErr:  false,
		},
		{
			name:     "single table",
			json:     `{"tables": ["mydb.users"]}`,
			expected: []string{"mydb.users"},
			wantErr:  false,
		},
		{
			name:     "empty tables",
			json:     `{"tables": []}`,
			expected: []string{},
			wantErr:  false,
		},
		{
			name:     "multiple databases",
			json:     `{"tables": ["sales.orders", "sales.customers", "hr.employees"]}`,
			expected: []string{"sales.orders", "sales.customers", "hr.employees"},
			wantErr:  false,
		},
		{
			name:     "invalid json",
			json:     `{"tables": invalid}`,
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "missing tables field",
			json:     `{"other": "value"}`,
			expected: nil,
			wantErr:  false, // Will parse successfully but tables will be nil
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result LLMExtractTablesResult
			err := json.Unmarshal([]byte(tt.json), &result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result.Tables) != len(tt.expected) {
				t.Errorf("Expected %d tables, got %d", len(tt.expected), len(result.Tables))
				return
			}

			for i, table := range result.Tables {
				if table != tt.expected[i] {
					t.Errorf("Table[%d] = %q, want %q", i, table, tt.expected[i])
				}
			}
		})
	}
}

// TestLLMExtractTablesIntegration is an integration test that requires API key
// Skip this test in CI by checking for API key
func TestLLMExtractTablesIntegration(t *testing.T) {
	t.Skip("Integration test - requires API key")

	apiKey := "" // Set your API key here for manual testing
	if apiKey == "" {
		t.Skip("API key not set")
	}

	tests := []struct {
		name           string
		sqls           []string
		expectedTables []string // Tables that must be present in result
	}{
		{
			name: "simple select",
			sqls: []string{
				"SELECT * FROM mydb.users WHERE id = 1",
			},
			expectedTables: []string{"mydb.users"},
		},
		{
			name: "join query",
			sqls: []string{
				"SELECT * FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id",
			},
			expectedTables: []string{"sales.orders", "sales.customers"},
		},
		{
			name: "with dodo comment prefix",
			sqls: []string{
				`/*dodo{"db":"testdb"}*/ SELECT * FROM users JOIN orders ON users.id = orders.user_id`,
			},
			expectedTables: []string{"testdb.users", "testdb.orders"},
		},
		{
			name: "subquery",
			sqls: []string{
				"SELECT * FROM mydb.products WHERE category_id IN (SELECT id FROM mydb.categories)",
			},
			expectedTables: []string{"mydb.products", "mydb.categories"},
		},
		{
			name: "CTE query",
			sqls: []string{
				`WITH active_users AS (SELECT * FROM mydb.users WHERE active = 1)
				 SELECT * FROM active_users JOIN mydb.orders ON active_users.id = orders.user_id`,
			},
			expectedTables: []string{"mydb.users", "mydb.orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := LLMExtractTables(ctx, apiKey, "", "deepseek-v4-flash", "", tt.sqls)
			if err != nil {
				t.Fatalf("LLMExtractTables error: %v", err)
			}

			t.Logf("Result: %v", result)

			// Check that all expected tables are present
			for _, expected := range tt.expectedTables {
				found := false
				for _, table := range result {
					if strings.EqualFold(table, expected) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected table %q not found in result %v", expected, result)
				}
			}
		})
	}
}
