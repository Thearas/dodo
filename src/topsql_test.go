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
package src

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractSQL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantDb   string
		wantMeta bool // whether meta should be non-nil
		wantSQL  string
	}{
		{
			name:     "plain SQL without dodo prefix",
			input:    "SELECT * FROM users;",
			wantDb:   "(no db)",
			wantMeta: false,
			wantSQL:  "SELECT * FROM users;",
		},
		{
			name:     "SQL with dodo prefix",
			input:    `/*dodo{"ts":"2024-08-06 23:44:11.041","client":"192.168.48.119:51970","user":"root","db":"mydb","queryId":"abc123","durationMs":10}*/ SELECT * FROM users;`,
			wantDb:   "mydb",
			wantMeta: true,
			wantSQL:  "SELECT * FROM users;",
		},
		{
			name:     "SQL with empty db in dodo prefix",
			input:    `/*dodo{"ts":"2024-08-06 23:44:11.041","client":"192.168.48.119:51970","user":"root","db":"","queryId":"abc123"}*/ SELECT 1;`,
			wantDb:   "(no db)",
			wantMeta: true,
			wantSQL:  "SELECT 1;",
		},
		{
			name:     "SQL with whitespace",
			input:    "  SELECT 1;  ",
			wantDb:   "(no db)",
			wantMeta: false,
			wantSQL:  "SELECT 1;",
		},
		{
			name:     "empty input",
			input:    "",
			wantDb:   "(no db)",
			wantMeta: false,
			wantSQL:  "",
		},
		{
			name:     "malformed dodo prefix (no closing)",
			input:    `/*dodo{"db":"test" SELECT 1;`,
			wantDb:   "(no db)",
			wantMeta: false,
			wantSQL:  `/*dodo{"db":"test" SELECT 1;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDb, gotMeta, gotSQL := extractSQL(tt.input)
			assert.Equal(t, tt.wantDb, gotDb, "db mismatch")
			if tt.wantMeta {
				assert.NotNil(t, gotMeta, "meta should not be nil")
			} else {
				assert.Nil(t, gotMeta, "meta should be nil")
			}
			assert.Equal(t, tt.wantSQL, gotSQL, "sql mismatch")
		})
	}
}

func TestAddSQLPattern(t *testing.T) {
	tests := []struct {
		name         string
		sqls         []struct{ prefix, sql string }
		wantPatterns int
		wantCounts   map[string]int    // pattern -> count
		wantSample   map[string]string // pattern -> expected first sample SQL
	}{
		{
			name: "single SQL",
			sqls: []struct{ prefix, sql string }{
				{"", "SELECT * FROM users WHERE id = 1"},
			},
			wantPatterns: 1,
			wantCounts: map[string]int{
				"select * from `users` where `id` = ?": 1,
			},
			wantSample: map[string]string{
				"select * from `users` where `id` = ?": "SELECT * FROM users WHERE id = 1",
			},
		},
		{
			name: "same pattern different values",
			sqls: []struct{ prefix, sql string }{
				{"", "SELECT * FROM users WHERE id = 1"},
				{"", "SELECT * FROM users WHERE id = 2"},
				{"", "SELECT * FROM users WHERE id = 3"},
			},
			wantPatterns: 1,
			wantCounts: map[string]int{
				"select * from `users` where `id` = ?": 3,
			},
			wantSample: map[string]string{
				// Only first sample is stored
				"select * from `users` where `id` = ?": "SELECT * FROM users WHERE id = 1",
			},
		},
		{
			name: "same pattern more than 3 samples",
			sqls: []struct{ prefix, sql string }{
				{"", "SELECT * FROM t WHERE a = 1"},
				{"", "SELECT * FROM t WHERE a = 2"},
				{"", "SELECT * FROM t WHERE a = 3"},
				{"", "SELECT * FROM t WHERE a = 4"},
				{"", "SELECT * FROM t WHERE a = 5"},
			},
			wantPatterns: 1,
			wantCounts: map[string]int{
				"select * from `t` where `a` = ?": 5,
			},
			wantSample: map[string]string{
				// Only first sample is kept
				"select * from `t` where `a` = ?": "SELECT * FROM t WHERE a = 1",
			},
		},
		{
			name: "different patterns",
			sqls: []struct{ prefix, sql string }{
				{"", "SELECT * FROM users"},
				{"", "SELECT * FROM orders"},
				{"", "INSERT INTO logs VALUES (1)"},
			},
			wantPatterns: 3,
			wantCounts: map[string]int{
				"select * from `users`":         1,
				"select * from `orders`":        1,
				"insert into logs values ( ? )": 1,
			},
		},
		{
			name: "SQL with prefix preserves prefix in sample",
			sqls: []struct{ prefix, sql string }{
				{`/*dodo{"db":"test"}*/`, "SELECT 1"},
			},
			wantPatterns: 1,
			wantCounts: map[string]int{
				"select ?": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patternCounts := make(map[uint64]*SQLPattern)

			for _, s := range tt.sqls {
				_, err := addSQLPattern(s.prefix, s.sql, patternCounts)
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.wantPatterns, len(patternCounts), "pattern count mismatch")

			// Check counts
			for wantPattern, wantCount := range tt.wantCounts {
				found := false
				for _, p := range patternCounts {
					if p.Pattern == wantPattern {
						found = true
						assert.Equal(t, wantCount, p.Count, "count mismatch for pattern: %s", wantPattern)
						break
					}
				}
				assert.True(t, found, "pattern not found: %s", wantPattern)
			}

			// Check sample SQL (first sample is stored)
			for wantPattern, wantSampleSQL := range tt.wantSample {
				for _, p := range patternCounts {
					if p.Pattern == wantPattern {
						assert.Equal(t, wantSampleSQL, p.SampleSQL, "sample SQL mismatch for pattern: %s", wantPattern)
						break
					}
				}
			}
		})
	}
}

func TestCalculateSQLComplexity(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantJoins    int
		wantSubs     int
		wantAggs     int
		wantWinds    int
		wantCTEs     int
		wantDepth    int
		wantMinScore int
	}{
		{
			name:      "simple select",
			sql:       "SELECT * FROM users",
			wantJoins: 0,
			wantSubs:  0,
			wantAggs:  0,
			wantDepth: 0,
		},
		{
			name:      "with JOIN",
			sql:       "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
			wantJoins: 1,
		},
		{
			name:      "with multiple JOINs",
			sql:       "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id INNER JOIN c ON b.id = c.b_id",
			wantJoins: 2,
		},
		{
			name:     "with subquery",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantSubs: 1,
		},
		{
			name:     "with aggregates",
			sql:      "SELECT COUNT(*), SUM(amount), AVG(price) FROM orders",
			wantAggs: 3,
		},
		{
			name:      "with window function",
			sql:       "SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders",
			wantWinds: 1,
		},
		{
			name:     "with CTE",
			sql:      "WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users",
			wantCTEs: 1,
		},
		{
			name:      "nested parentheses",
			sql:       "SELECT * FROM t WHERE a = ((1 + 2) * (3 + 4))",
			wantDepth: 2, // max depth is 2: ((1+2)...)
		},
		{
			name:         "complex query",
			sql:          "WITH cte AS (SELECT * FROM t) SELECT COUNT(*) FROM cte c JOIN (SELECT id FROM users) u ON c.id = u.id",
			wantJoins:    1,
			wantSubs:     2, // CTE's (SELECT...) and JOIN's (SELECT...)
			wantAggs:     1,
			wantCTEs:     1,
			wantMinScore: 11, // 1*2 + 2*3 + 1*1 + 1*2 = 11
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CalculateSQLComplexity(tt.sql)

			if tt.wantJoins > 0 {
				assert.Equal(t, tt.wantJoins, c.Joins, "joins mismatch")
			}
			if tt.wantSubs > 0 {
				assert.Equal(t, tt.wantSubs, c.Subqueries, "subqueries mismatch")
			}
			if tt.wantAggs > 0 {
				assert.Equal(t, tt.wantAggs, c.Aggregates, "aggregates mismatch")
			}
			if tt.wantWinds > 0 {
				assert.Equal(t, tt.wantWinds, c.WindowFunctions, "window functions mismatch")
			}
			if tt.wantCTEs > 0 {
				assert.Equal(t, tt.wantCTEs, c.CTEs, "CTEs mismatch")
			}
			if tt.wantDepth > 0 {
				assert.Equal(t, tt.wantDepth, c.NestingDepth, "nesting depth mismatch")
			}
			if tt.wantMinScore > 0 {
				assert.GreaterOrEqual(t, c.Score, tt.wantMinScore, "score should be at least %d", tt.wantMinScore)
			}
		})
	}
}

func TestSortPatterns(t *testing.T) {
	patterns := []*SQLPattern{
		{Pattern: "a", Count: 10, Complexity: SQLComplexity{Score: 5}},
		{Pattern: "b", Count: 5, Complexity: SQLComplexity{Score: 20}},
		{Pattern: "c", Count: 20, Complexity: SQLComplexity{Score: 2}},
	}

	t.Run("sort by count", func(t *testing.T) {
		p := make([]*SQLPattern, len(patterns))
		copy(p, patterns)
		sortPatterns(p, SortByCount)

		assert.Equal(t, "c", p[0].Pattern) // count=20
		assert.Equal(t, "a", p[1].Pattern) // count=10
		assert.Equal(t, "b", p[2].Pattern) // count=5
	})

	t.Run("sort by complexity", func(t *testing.T) {
		p := make([]*SQLPattern, len(patterns))
		copy(p, patterns)
		sortPatterns(p, SortByComplexity)

		assert.Equal(t, "b", p[0].Pattern) // score=20
		assert.Equal(t, "a", p[1].Pattern) // score=5
		assert.Equal(t, "c", p[2].Pattern) // score=2
	})

	t.Run("sort by combined", func(t *testing.T) {
		p := make([]*SQLPattern, len(patterns))
		copy(p, patterns)
		sortPatterns(p, SortByCombined)

		// combined scores: a=50, b=100, c=40
		assert.Equal(t, "b", p[0].Pattern) // 5*20=100
		assert.Equal(t, "a", p[1].Pattern) // 10*5=50
		assert.Equal(t, "c", p[2].Pattern) // 20*2=40
	})
}

func TestMaxParenDepth(t *testing.T) {
	tests := []struct {
		sql       string
		wantDepth int
	}{
		{"SELECT 1", 0},
		{"SELECT (1)", 1},
		{"SELECT ((1))", 2},
		{"SELECT ((1 + 2) * 3)", 2},
		{"SELECT (((a)))", 3},
		{"SELECT (1) + (2)", 1},
		{"SELECT ((1)) + ((2))", 2},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got := maxParenDepth(tt.sql)
			assert.Equal(t, tt.wantDepth, got)
		})
	}
}

func TestHashString(t *testing.T) {
	// Same string should produce same hash
	h1 := hashString("SELECT * FROM users")
	h2 := hashString("SELECT * FROM users")
	assert.Equal(t, h1, h2)

	// Different strings should produce different hashes
	h3 := hashString("SELECT * FROM orders")
	assert.NotEqual(t, h1, h3)
}

func TestAnalyzeFileLocal_DBFilter(t *testing.T) {
	dir := t.TempDir()
	sqlFile := filepath.Join(dir, "q0.sql")
	content := EncodeReplaySql("2024-08-06 23:44:11.041", "client1", "root", "db1", "q1", "SELECT * FROM users WHERE id = 1", 10) + "\n" +
		EncodeReplaySql("2024-08-06 23:44:12.041", "client1", "root", "db2", "q2", "SELECT * FROM users WHERE id = 2", 10) + "\n" +
		"SELECT * FROM plain_users WHERE id = 3;\n"
	err := os.WriteFile(sqlFile, []byte(content), 0600)
	assert.NoError(t, err)

	localPatterns := make(map[string]map[uint64]*SQLPattern)
	originalSQLs := make([]OriginalSQL, 0)

	count, err := analyzeFileLocal(sqlFile, localPatterns, true, &originalSQLs, 0, 0, map[string]struct{}{
		"db1": {},
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Len(t, localPatterns, 1)
	assert.Contains(t, localPatterns, "db1")
	assert.NotContains(t, localPatterns, "db2")
	assert.Len(t, originalSQLs, 1)
	assert.Equal(t, "db1", originalSQLs[0].DbName)
}
