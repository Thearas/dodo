package src

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPercentile(t *testing.T) {
	tests := []struct {
		name   string
		sorted []int64
		p      float64
		want   int64
	}{
		{"empty", nil, 0.95, 0},
		{"single", []int64{42}, 0.95, 42},
		{"single_p50", []int64{42}, 0.50, 42},
		{"two_p50", []int64{10, 20}, 0.50, 15},
		{"two_p95", []int64{10, 20}, 0.95, 19},
		{"ten_p50", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.50, 5},
		{"ten_p95", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.95, 9},
		{"ten_p0", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.0, 1},
		{"ten_p100", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1.0, 10},
		{"hundred_p95", makeRange(1, 100), 0.95, 95},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentile(tt.sorted, tt.p)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAverage(t *testing.T) {
	assert.Equal(t, int64(0), average(nil))
	assert.Equal(t, int64(5), average([]int64{5}))
	assert.Equal(t, int64(5), average([]int64{1, 5, 10})) // (16/3) = 5 truncated
	assert.Equal(t, int64(55), average([]int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}))
}

func TestPatternStatsFinalize(t *testing.T) {
	s := &PatternStats{
		Count:      5,
		ErrorCount: 1,
		durations:  []int64{100, 50, 200, 150, 300},
	}
	s.Finalize()

	assert.Equal(t, int64(150), s.P50)
	assert.Equal(t, int64(279), s.P95)
	assert.Equal(t, int64(300), s.Max)
	assert.Equal(t, int64(50), s.MinDur)
	assert.Equal(t, int64(160), s.Avg) // (100+50+200+150+300)/5 = 160
	assert.InDelta(t, 0.2, s.ErrorRate, 0.001)
	assert.Nil(t, s.durations, "durations should be freed after Finalize")
}

func TestReadReplayResults(t *testing.T) {
	dir := t.TempDir()

	// Write two result files simulating two clients
	writeResultLines(t, filepath.Join(dir, "client01.result"), []ReplayResult{
		{QueryId: "q1", DurationMs: 100, PatternHash: 111, Complexity: 5, ReturnRows: 10, ReturnRowsHash: "hash-q1"},
		{QueryId: "q2", DurationMs: 200, PatternHash: 111, Complexity: 5, ReturnRows: 20, ReturnRowsHash: "hash-q2"},
		{QueryId: "q3", DurationMs: 50, PatternHash: 222, Stmt: "SELECT 1", ReturnRows: 30, ReturnRowsHash: "hash-q3"},
	})
	writeResultLines(t, filepath.Join(dir, "client02.result"), []ReplayResult{
		{QueryId: "q4", DurationMs: 150, PatternHash: 111, Complexity: 5, ReturnRows: 40, ReturnRowsHash: "hash-q4"},
		{QueryId: "q5", DurationMs: 300, PatternHash: 222, Stmt: "SELECT 2", ReturnRows: 50, ReturnRowsHash: "hash-q5"},
		{QueryId: "q6", DurationMs: 10, PatternHash: 0}, // should be skipped
	})

	stats, err := ReadReplayResults(dir)
	require.NoError(t, err)

	assert.Len(t, stats, 2)

	s111 := stats[111]
	require.NotNil(t, s111)
	assert.Equal(t, 3, s111.Count)
	assert.True(t, s111.P95 >= 194 && s111.P95 <= 195, "P95 should be close to 195, got %d", s111.P95) // 3 values: 100, 150, 200 (linear interp)
	require.Contains(t, s111.QueryResults, "q2")
	assert.Equal(t, 20, s111.QueryResults["q2"].ReturnRows)
	assert.Equal(t, "hash-q2", s111.QueryResults["q2"].ReturnRowsHash)

	s222 := stats[222]
	require.NotNil(t, s222)
	assert.Equal(t, 2, s222.Count)
	assert.Equal(t, "SELECT 1", s222.SampleStmt)
}

func TestReadReplayResults_PreservesSampleQueryIDWhenStmtMissing(t *testing.T) {
	dir := t.TempDir()

	writeResultLines(t, filepath.Join(dir, "client01.result"), []ReplayResult{
		{QueryId: "q1", DurationMs: 100, PatternHash: 111},
		{QueryId: "q2", DurationMs: 200, PatternHash: 111},
	})

	stats, err := ReadReplayResults(dir)
	require.NoError(t, err)
	require.Contains(t, stats, uint64(111))
	assert.Equal(t, "q1", stats[111].SampleQueryId)
	assert.Empty(t, stats[111].SampleStmt)
}

func TestReadReplayResults_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	stats, err := ReadReplayResults(dir)
	require.NoError(t, err)
	assert.Empty(t, stats)
}

func TestDiffReplayAggregate(t *testing.T) {
	before := map[uint64]*PatternStats{
		111: {PatternHash: 111, Count: 10, P95: 100},
		222: {PatternHash: 222, Count: 5, P95: 50},
	}
	after := map[uint64]*PatternStats{
		111: {PatternHash: 111, Count: 10, P95: 200}, // regressed
		333: {PatternHash: 333, Count: 3, P95: 30},   // new pattern
	}

	diffs := DiffReplayAggregate(before, after)
	assert.Len(t, diffs, 3) // 111, 222, 333

	diffMap := make(map[uint64]DiffPatternStats)
	for _, d := range diffs {
		diffMap[d.PatternHash] = d
	}

	// 111: exists in both
	d111 := diffMap[111]
	assert.NotNil(t, d111.Before)
	assert.NotNil(t, d111.After)
	assert.Equal(t, int64(100), d111.Before.P95)
	assert.Equal(t, int64(200), d111.After.P95)

	// 222: only in before
	d222 := diffMap[222]
	assert.NotNil(t, d222.Before)
	assert.Nil(t, d222.After)

	// 333: only in after
	d333 := diffMap[333]
	assert.Nil(t, d333.Before)
	assert.NotNil(t, d333.After)
}

func TestDiffReplayAggregate_ResultMismatches(t *testing.T) {
	before := map[uint64]*PatternStats{
		111: {
			PatternHash: 111,
			QueryResults: map[string]*ReplayResult{
				"q1": {QueryId: "q1", PatternHash: 111, ReturnRows: 10, ReturnRowsHash: "same", Stmt: "SELECT 1"},
				"q2": {QueryId: "q2", PatternHash: 111, ReturnRows: 20, ReturnRowsHash: "hash-before", Stmt: "SELECT 2"},
			},
		},
	}
	after := map[uint64]*PatternStats{
		111: {
			PatternHash: 111,
			QueryResults: map[string]*ReplayResult{
				"q1": {QueryId: "q1", PatternHash: 111, ReturnRows: 11, ReturnRowsHash: "same", Stmt: "SELECT 1"},
				"q2": {QueryId: "q2", PatternHash: 111, ReturnRows: 20, ReturnRowsHash: "hash-after", Stmt: "SELECT 2 changed"},
			},
		},
	}

	diffs := DiffReplayAggregate(before, after)
	require.Len(t, diffs, 1)
	assert.Equal(t, 1, diffs[0].RowsDiffCount)
	assert.Equal(t, 1, diffs[0].HashDiffCount)
	require.Len(t, diffs[0].QueryMismatches, 2)
	assert.Equal(t, "q1", diffs[0].QueryMismatches[0].QueryId)
	assert.True(t, diffs[0].QueryMismatches[0].RowsMismatch)
	assert.False(t, diffs[0].QueryMismatches[0].HashMismatch)
	assert.Equal(t, "q2", diffs[0].QueryMismatches[1].QueryId)
	assert.False(t, diffs[0].QueryMismatches[1].RowsMismatch)
	assert.True(t, diffs[0].QueryMismatches[1].HashMismatch)
	assert.True(t, diffs[0].QueryMismatches[1].SQLDiffers)
}

func TestDiffReplayAggregate_ResultMismatches_PrefersAfterPatternHash(t *testing.T) {
	before := map[uint64]*PatternStats{
		111: {
			PatternHash: 111,
			QueryResults: map[string]*ReplayResult{
				"q1": {QueryId: "q1", PatternHash: 111, ReturnRows: 10, ReturnRowsHash: "same"},
			},
		},
	}
	after := map[uint64]*PatternStats{
		222: {
			PatternHash: 222,
			QueryResults: map[string]*ReplayResult{
				"q1": {QueryId: "q1", PatternHash: 222, ReturnRows: 12, ReturnRowsHash: "same"},
			},
		},
	}

	diffs := DiffReplayAggregate(before, after)
	require.Len(t, diffs, 2)
	diffMap := make(map[uint64]DiffPatternStats, len(diffs))
	for _, d := range diffs {
		diffMap[d.PatternHash] = d
	}
	assert.Equal(t, 1, diffMap[222].RowsDiffCount)
	assert.Len(t, diffMap[222].QueryMismatches, 1)
	assert.Equal(t, uint64(111), diffMap[222].QueryMismatches[0].BeforePattern)
	assert.Equal(t, uint64(222), diffMap[222].QueryMismatches[0].AfterPattern)
	assert.Zero(t, diffMap[111].RowsDiffCount)
}

func TestSortDiffPatternStats(t *testing.T) {
	t.Run("p95-change", func(t *testing.T) {
		diffs := []DiffPatternStats{
			{PatternHash: 1, Before: &PatternStats{P95: 100}, After: &PatternStats{P95: 100}},
			{PatternHash: 2, Before: &PatternStats{P95: 100}, After: &PatternStats{P95: 300}},
			{PatternHash: 3, Before: &PatternStats{P95: 100}, After: &PatternStats{P95: 50}},
		}

		SortDiffPatternStats(diffs, AggregateSortByP95Change)
		assert.Equal(t, uint64(2), diffs[0].PatternHash)
		assert.Equal(t, uint64(3), diffs[1].PatternHash)
		assert.Equal(t, uint64(1), diffs[2].PatternHash)
	})

	t.Run("p50-change", func(t *testing.T) {
		diffs := []DiffPatternStats{
			{PatternHash: 1, Before: &PatternStats{P50: 100}, After: &PatternStats{P50: 100}},
			{PatternHash: 2, Before: &PatternStats{P50: 100}, After: &PatternStats{P50: 300}},
			{PatternHash: 3, Before: &PatternStats{P50: 100}, After: &PatternStats{P50: 50}},
		}

		SortDiffPatternStats(diffs, AggregateSortByP50Change)
		assert.Equal(t, uint64(2), diffs[0].PatternHash)
		assert.Equal(t, uint64(3), diffs[1].PatternHash)
		assert.Equal(t, uint64(1), diffs[2].PatternHash)
	})

	t.Run("avg-change", func(t *testing.T) {
		diffs := []DiffPatternStats{
			{PatternHash: 1, Before: &PatternStats{Avg: 100}, After: &PatternStats{Avg: 100}},
			{PatternHash: 2, Before: &PatternStats{Avg: 100}, After: &PatternStats{Avg: 300}},
			{PatternHash: 3, Before: &PatternStats{Avg: 100}, After: &PatternStats{Avg: 50}},
		}

		SortDiffPatternStats(diffs, AggregateSortByAvgChange)
		assert.Equal(t, uint64(2), diffs[0].PatternHash)
		assert.Equal(t, uint64(3), diffs[1].PatternHash)
		assert.Equal(t, uint64(1), diffs[2].PatternHash)
	})
}

func TestFormatSingleAggregate(t *testing.T) {
	stats := map[uint64]*PatternStats{
		111: {PatternHash: 111, Count: 10, P95: 180, P50: 100, Max: 300, Avg: 150, SampleQueryId: "q1"},
		222: {PatternHash: 222, Count: 5, P95: 40, P50: 30, Max: 60, Avg: 35, ErrorCount: 1, ErrorRate: 0.2, SampleQueryId: "q2"},
	}

	output := FormatSingleAggregate(stats, AggregateSortByP95Change, 10)
	assert.Contains(t, output, "Replay Aggregate Statistics")
	assert.Contains(t, output, "2 patterns")
	assert.Contains(t, output, "15 total queries")
	assert.Contains(t, output, "q1")
	assert.Contains(t, output, "q2")
	assert.Contains(t, output, "P95(ms)")
}

func TestFormatDiffAggregate(t *testing.T) {
	diffs := []DiffPatternStats{
		{
			PatternHash:   111,
			Before:        &PatternStats{Count: 10, P50: 80, P95: 150, Avg: 120},
			After:         &PatternStats{Count: 10, P50: 100, P95: 200, Avg: 180},
			RowsDiffCount: 2,
			HashDiffCount: 1,
		},
	}

	output := FormatDiffAggregate(diffs, AggregateSortByP95Change, 10, false)
	assert.Contains(t, output, "Replay Aggregate Diff")
	assert.Contains(t, output, "+33.3%")
	assert.Contains(t, output, "Regressed: 1")
	assert.Contains(t, output, "P95ms(b→a)")
	assert.Contains(t, output, "P95Change%")
	assert.Contains(t, output, "RowsDiff")
	assert.Contains(t, output, "HashDiff")
	assert.Contains(t, output, "         2          1")
}

func TestFormatDiffAggregate_UsesSelectedChangeMetric(t *testing.T) {
	diffs := []DiffPatternStats{
		{
			PatternHash: 111,
			Before:      &PatternStats{Count: 10, P50: 100, P95: 160, Avg: 100},
			After:       &PatternStats{Count: 10, P50: 150, P95: 200, Avg: 120},
		},
	}

	p50Output := FormatDiffAggregate(diffs, AggregateSortByP50Change, 10, false)
	assert.Contains(t, p50Output, "P50Change%")
	assert.Contains(t, p50Output, "+50.0%")

	avgOutput := FormatDiffAggregate(diffs, AggregateSortByAvgChange, 10, false)
	assert.Contains(t, avgOutput, "AvgChange%")
	assert.Contains(t, avgOutput, "+20.0%")
}

func TestFormatDiffAggregateDetailed(t *testing.T) {
	diffs := []DiffPatternStats{
		{
			PatternHash:   111,
			Before:        &PatternStats{Count: 10, ErrorCount: 1, P50: 80, P95: 150, Avg: 120, Max: 250, SampleQueryId: "q-before", SampleStmt: "SELECT * FROM before_tbl"},
			After:         &PatternStats{Count: 12, ErrorCount: 0, P50: 100, P95: 200, Avg: 180, Max: 450, SampleQueryId: "q-after", SampleStmt: "SELECT * FROM after_tbl"},
			RowsDiffCount: 1,
			HashDiffCount: 1,
			QueryMismatches: []QueryMismatchDetail{
				{
					QueryId:        "q-mismatch",
					BeforeRows:     10,
					AfterRows:      12,
					BeforeRowsHash: "before-hash",
					AfterRowsHash:  "after-hash",
					RowsMismatch:   true,
					HashMismatch:   true,
					SQLDiffers:     true,
					BeforeStmt:     "SELECT * FROM before_detail",
					AfterStmt:      "SELECT * FROM after_detail",
				},
			},
		},
	}

	output := FormatDiffAggregateDetailed(diffs, AggregateSortByP95Change, 10)
	assert.Contains(t, output, "Replay Aggregate Diff")
	assert.Contains(t, output, "Detailed SQL Samples")
	assert.Contains(t, output, "Sample SQL:")
	assert.Contains(t, output, "Result mismatches: rows=1 hash=1")
	assert.Contains(t, output, "QueryId: q-mismatch")
	assert.Contains(t, output, "Rows: 10 -> 12")
	assert.Contains(t, output, "Hash: before-hash -> after-hash")
	assert.Contains(t, output, "SQL differs")
	assert.Contains(t, output, "SQL Before:")
	assert.Contains(t, output, "SQL After:")
	assert.Contains(t, output, "SELECT * FROM before_detail")
	assert.Contains(t, output, "SELECT * FROM after_detail")
	assert.Contains(t, output, "SELECT * FROM before_tbl")
	assert.Contains(t, output, "p50=80ms p95=150ms avg=120ms max=250ms")
	assert.NotContains(t, output, "sampleQueryId=")
}

func TestFormatDiffAggregateDetailed_PrefersAfterWhenBeforeSampleMissing(t *testing.T) {
	diffs := []DiffPatternStats{
		{
			PatternHash: 111,
			Before:      &PatternStats{Count: 10, P95: 200},
			After:       &PatternStats{Count: 12, P95: 400, SampleStmt: "SELECT * FROM after_tbl"},
		},
	}

	output := FormatDiffAggregateDetailed(diffs, AggregateSortByP95Change, 10)
	assert.Contains(t, output, "Sample SQL:")
	assert.Contains(t, output, "SELECT * FROM after_tbl")
}

func TestLoadReplaySQLSampleIndexAndFillSampleStmts(t *testing.T) {
	dir := t.TempDir()
	replayPath := filepath.Join(dir, "replay.sql")
	content := strings.Join([]string{
		EncodeReplaySql("2024-01-01 00:00:00.000", "client1", "root", "test", "q1", "SELECT * FROM t WHERE a = 1", 10),
		EncodeReplaySql("2024-01-01 00:00:01.000", "client1", "root", "test", "q2", "SELECT * FROM t WHERE a = 2", 20),
	}, "\n")
	require.NoError(t, os.WriteFile(replayPath, []byte(content+"\n"), 0600))

	index, err := LoadReplaySQLSampleIndex([]string{replayPath})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 1;", index["q1"])
	assert.Equal(t, "SELECT * FROM t WHERE a = 2;", index["q2"])

	stats := map[uint64]*PatternStats{
		111: {PatternHash: 111, SampleQueryId: "q1"},
		222: {PatternHash: 222, SampleQueryId: "q2", SampleStmt: "SELECT existing"},
	}
	FillSampleStmtsFromIndex(stats, index)
	assert.Equal(t, "SELECT * FROM t WHERE a = 1;", stats[111].SampleStmt)
	assert.Equal(t, "SELECT existing", stats[222].SampleStmt)
}

func TestFillQueryResultStmtsFromIndex(t *testing.T) {
	stats := map[uint64]*PatternStats{
		111: {
			PatternHash: 111,
			QueryResults: map[string]*ReplayResult{
				"q1": {QueryId: "q1"},
				"q2": {QueryId: "q2", Stmt: "SELECT existing"},
			},
		},
	}

	FillQueryResultStmtsFromIndex(stats, map[string]string{
		"q1": "SELECT * FROM t1;",
		"q2": "SELECT * FROM t2;",
	})

	assert.Equal(t, "SELECT * FROM t1;", stats[111].QueryResults["q1"].Stmt)
	assert.Equal(t, "SELECT existing", stats[111].QueryResults["q2"].Stmt)
}

// --- test helpers ---

func writeResultLines(t *testing.T, path string, results []ReplayResult) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	for _, r := range results {
		b, err := json.Marshal(r)
		require.NoError(t, err)
		fmt.Fprintln(f, string(b))
	}
}

func makeRange(start, end int64) []int64 {
	r := make([]int64, 0, end-start+1)
	for i := start; i <= end; i++ {
		r = append(r, i)
	}
	return r
}
