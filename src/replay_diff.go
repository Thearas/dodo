package src

import (
	"bufio"
	"bytes"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/fatih/color"
	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

// PatternStats holds aggregated statistics for a single SQL pattern.
type PatternStats struct {
	PatternHash uint64
	Complexity  int
	Count       int
	ErrorCount  int

	// Computed after Finalize()
	P50    int64
	P95    int64
	Avg    int64
	Max    int64
	MinDur int64

	ErrorRate float64

	SampleStmt    string
	SampleQueryId string

	QueryResults map[string]*ReplayResult

	// Raw durations collected before Finalize(), cleared after.
	durations []int64
}

// Finalize sorts durations and computes aggregate metrics.
// Must be called after all results are collected.
func (s *PatternStats) Finalize() {
	if len(s.durations) == 0 {
		return
	}

	slices.Sort(s.durations)

	s.P50 = percentile(s.durations, 0.50)
	s.P95 = percentile(s.durations, 0.95)
	s.Max = s.durations[len(s.durations)-1]
	s.MinDur = s.durations[0]
	s.Avg = average(s.durations)

	if s.Count > 0 {
		s.ErrorRate = float64(s.ErrorCount) / float64(s.Count)
	}

	// Free memory
	s.durations = nil
}

// DiffPatternStats holds the before/after comparison for a single pattern.
type DiffPatternStats struct {
	PatternHash     uint64
	Before          *PatternStats // nil if pattern only in "after"
	After           *PatternStats // nil if pattern only in "before"
	RowsDiffCount   int
	HashDiffCount   int
	QueryMismatches []QueryMismatchDetail
}

type QueryMismatchDetail struct {
	QueryId        string
	BeforePattern  uint64
	AfterPattern   uint64
	BeforeRows     int
	AfterRows      int
	BeforeRowsHash string
	AfterRowsHash  string
	RowsMismatch   bool
	HashMismatch   bool
	SQLDiffers     bool
	BeforeStmt     string
	AfterStmt      string
}

// AggregateSortBy defines sorting criteria for aggregate results.
type AggregateSortBy string

const (
	AggregateSortByP95Change AggregateSortBy = "p95-change"
	AggregateSortByP50Change AggregateSortBy = "p50-change"
	AggregateSortByAvgChange AggregateSortBy = "avg-change"
	AggregateSortByCount     AggregateSortBy = "count"
)

// ReadReplayResults reads all .result files in a directory and aggregates
// replay results by PatternHash.
func ReadReplayResults(dir string) (map[uint64]*PatternStats, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read replay dir %s: %w", dir, err)
	}

	stats := make(map[uint64]*PatternStats)
	skippedNoPattern := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ReplayResultFileExt) {
			continue
		}

		fpath := filepath.Join(dir, entry.Name())
		if err := readResultFile(fpath, stats, &skippedNoPattern); err != nil {
			logrus.Warnf("Failed to read result file %s: %v", fpath, err)
			continue
		}
	}

	if skippedNoPattern > 0 {
		logrus.Warnf("Skipped %d results with PatternHash == 0 (no pattern info)", skippedNoPattern)
	}

	// Finalize all stats
	for _, s := range stats {
		s.Finalize()
	}

	return stats, nil
}

func readResultFile(path string, stats map[uint64]*PatternStats, skippedNoPattern *int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var r ReplayResult
		if err := json.Unmarshal(line, &r); err != nil {
			logrus.Debugf("Failed to unmarshal result line in %s: %v", path, err)
			continue
		}

		if r.PatternHash == 0 {
			*skippedNoPattern++
			continue
		}

		s, ok := stats[r.PatternHash]
		if !ok {
			s = &PatternStats{
				PatternHash:  r.PatternHash,
				QueryResults: map[string]*ReplayResult{},
			}
			stats[r.PatternHash] = s
		}

		s.Count++
		s.durations = append(s.durations, r.DurationMs)
		if r.Complexity > s.Complexity {
			s.Complexity = r.Complexity
		}
		if r.Err != "" {
			s.ErrorCount++
		}
		if s.SampleQueryId == "" && r.QueryId != "" {
			s.SampleQueryId = r.QueryId
		}
		if s.SampleStmt == "" && r.Stmt != "" {
			s.SampleStmt = r.Stmt
			s.SampleQueryId = r.QueryId
		}
		if r.QueryId != "" {
			rcopy := r
			s.QueryResults[r.QueryId] = &rcopy
		}
	}

	return scanner.Err()
}

// LoadReplaySQLSampleIndex loads original replay SQLs and indexes them by QueryId.
func LoadReplaySQLSampleIndex(paths []string) (map[string]string, error) {
	files, err := FileGlob(paths)
	if err != nil {
		return nil, fmt.Errorf("failed to expand replay sql paths: %w", err)
	}

	index := make(map[string]string)
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open replay sql file %s: %w", path, err)
		}

		client2sqls, _, _, err := DecodeReplaySqls(f, nil, nil, 0, 0, 0)
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode replay sql file %s: %w", path, err)
		}

		for _, sqls := range client2sqls {
			for _, sql := range sqls {
				if sql.QueryId == "" || strings.TrimSpace(sql.Stmt) == "" {
					continue
				}
				if _, exists := index[sql.QueryId]; !exists {
					index[sql.QueryId] = sql.Stmt
				}
			}
		}
	}

	return index, nil
}

// FillSampleStmtsFromIndex fills missing sample SQLs from a QueryId->SQL index.
func FillSampleStmtsFromIndex(stats map[uint64]*PatternStats, queryIDToStmt map[string]string) {
	for _, s := range stats {
		if s == nil || s.SampleQueryId == "" || strings.TrimSpace(s.SampleStmt) != "" {
			continue
		}
		if stmt, ok := queryIDToStmt[s.SampleQueryId]; ok {
			s.SampleStmt = stmt
		}
	}
}

// FillQueryResultStmtsFromIndex fills missing replay result SQL text from a QueryId->SQL index.
func FillQueryResultStmtsFromIndex(stats map[uint64]*PatternStats, queryIDToStmt map[string]string) {
	for _, s := range stats {
		if s == nil {
			continue
		}
		for queryID, result := range s.QueryResults {
			if result == nil || strings.TrimSpace(result.Stmt) != "" {
				continue
			}
			if stmt, ok := queryIDToStmt[queryID]; ok {
				result.Stmt = stmt
			}
		}
	}
}

// DiffReplayAggregate compares two sets of pattern stats and produces
// a list of DiffPatternStats covering all patterns from both sides.
func DiffReplayAggregate(before, after map[uint64]*PatternStats) []DiffPatternStats {
	allHashes := make(map[uint64]struct{}, len(before)+len(after))
	for h := range before {
		allHashes[h] = struct{}{}
	}
	for h := range after {
		allHashes[h] = struct{}{}
	}

	diffs := make([]DiffPatternStats, 0, len(allHashes))
	for h := range allHashes {
		d := DiffPatternStats{PatternHash: h}
		if s, ok := before[h]; ok {
			d.Before = s
		}
		if s, ok := after[h]; ok {
			d.After = s
		}
		diffs = append(diffs, d)
	}

	beforeByQueryID := flattenQueryResultsByID(before)
	afterByQueryID := flattenQueryResultsByID(after)
	diffByPatternHash := make(map[uint64]*DiffPatternStats, len(diffs))
	for i := range diffs {
		diffByPatternHash[diffs[i].PatternHash] = &diffs[i]
	}

	for queryID, beforeResult := range beforeByQueryID {
		afterResult, ok := afterByQueryID[queryID]
		if !ok {
			continue
		}

		rowsMismatch := beforeResult.ReturnRows != afterResult.ReturnRows
		hashMismatch := beforeResult.ReturnRowsHash != afterResult.ReturnRowsHash
		if !rowsMismatch && !hashMismatch {
			continue
		}

		patternHash := afterResult.PatternHash
		if patternHash == 0 {
			patternHash = beforeResult.PatternHash
		}

		d := diffByPatternHash[patternHash]
		if d == nil {
			continue
		}

		if rowsMismatch {
			d.RowsDiffCount++
		}
		if hashMismatch {
			d.HashDiffCount++
		}

		beforeStmt := strings.TrimSpace(beforeResult.Stmt)
		afterStmt := strings.TrimSpace(afterResult.Stmt)
		d.QueryMismatches = append(d.QueryMismatches, QueryMismatchDetail{
			QueryId:        queryID,
			BeforePattern:  beforeResult.PatternHash,
			AfterPattern:   afterResult.PatternHash,
			BeforeRows:     beforeResult.ReturnRows,
			AfterRows:      afterResult.ReturnRows,
			BeforeRowsHash: beforeResult.ReturnRowsHash,
			AfterRowsHash:  afterResult.ReturnRowsHash,
			RowsMismatch:   rowsMismatch,
			HashMismatch:   hashMismatch,
			SQLDiffers:     beforeStmt != "" && afterStmt != "" && beforeStmt != afterStmt,
			BeforeStmt:     beforeStmt,
			AfterStmt:      afterStmt,
		})
	}

	for i := range diffs {
		slices.SortFunc(diffs[i].QueryMismatches, func(a, b QueryMismatchDetail) int {
			return cmp.Compare(a.QueryId, b.QueryId)
		})
	}

	return diffs
}

// SortDiffPatternStats sorts diffs in-place by the given criteria (descending).
func SortDiffPatternStats(diffs []DiffPatternStats, sortBy AggregateSortBy) {
	switch sortBy {
	case AggregateSortByP95Change:
		slices.SortFunc(diffs, func(a, b DiffPatternStats) int {
			return cmp.Compare(p95ChangeAbs(b), p95ChangeAbs(a))
		})
	case AggregateSortByP50Change:
		slices.SortFunc(diffs, func(a, b DiffPatternStats) int {
			return cmp.Compare(p50ChangeAbs(b), p50ChangeAbs(a))
		})
	case AggregateSortByAvgChange:
		slices.SortFunc(diffs, func(a, b DiffPatternStats) int {
			return cmp.Compare(avgChangeAbs(b), avgChangeAbs(a))
		})
	case AggregateSortByCount:
		slices.SortFunc(diffs, func(a, b DiffPatternStats) int {
			return cmp.Compare(maxCount(b), maxCount(a))
		})
	default:
		SortDiffPatternStats(diffs, AggregateSortByP95Change)
	}
}

// SortPatternStats sorts a slice of PatternStats in-place.
func SortPatternStats(stats []*PatternStats, sortBy AggregateSortBy) {
	switch sortBy {
	case AggregateSortByCount:
		slices.SortFunc(stats, func(a, b *PatternStats) int { return cmp.Compare(b.Count, a.Count) })
	case AggregateSortByP50Change:
		slices.SortFunc(stats, func(a, b *PatternStats) int { return cmp.Compare(b.P50, a.P50) })
	case AggregateSortByAvgChange:
		slices.SortFunc(stats, func(a, b *PatternStats) int { return cmp.Compare(b.Avg, a.Avg) })
	default:
		// default to p95
		slices.SortFunc(stats, func(a, b *PatternStats) int { return cmp.Compare(b.P95, a.P95) })
	}
}

// FormatSingleAggregate formats stats for a single replay directory (no comparison).
func FormatSingleAggregate(stats map[uint64]*PatternStats, sortBy AggregateSortBy, topN int) string {
	list := lo.Values(stats)
	SortPatternStats(list, sortBy)

	if topN > 0 && topN < len(list) {
		list = list[:topN]
	}

	var buf bytes.Buffer

	totalCount := lo.SumBy(lo.Values(stats), func(s *PatternStats) int { return s.Count })
	totalErrors := lo.SumBy(lo.Values(stats), func(s *PatternStats) int { return s.ErrorCount })

	fmt.Fprintf(&buf, "\n%s\n", strings.Repeat("=", 100))
	fmt.Fprintf(&buf, "Replay Aggregate Statistics (%d patterns, %d total queries, %d errors)\n",
		len(stats), totalCount, totalErrors)
	fmt.Fprintf(&buf, "%s\n\n", strings.Repeat("=", 100))

	// Header
	fmt.Fprintf(&buf, "%-4s %16s %8s %8s %8s %8s %8s %10s  %s\n",
		"#", "PatternHash", "Count", "Errors", "P50(ms)", "P95(ms)", "Max(ms)", "Avg(ms)", "Sample QueryId")
	fmt.Fprintf(&buf, "%s\n", strings.Repeat("-", 100))

	for i, s := range list {
		errRateStr := fmt.Sprintf("%d(%.0f%%)", s.ErrorCount, s.ErrorRate*100)
		fmt.Fprintf(&buf, "%-4d %16x %8d %8s %8d %8d %8d %10d  %s\n",
			i+1, s.PatternHash, s.Count, errRateStr,
			s.P50, s.P95, s.Max, s.Avg,
			s.SampleQueryId)
	}

	fmt.Fprintf(&buf, "%s\n", strings.Repeat("-", 100))

	return buf.String()
}

// FormatDiffAggregate formats a comparison between two replay result sets.
//
//nolint:revive
func FormatDiffAggregate(diffs []DiffPatternStats, sortBy AggregateSortBy, topN int, useColor bool) string {
	if topN > 0 && topN < len(diffs) {
		diffs = diffs[:topN]
	}

	var buf bytes.Buffer
	changeLabel := changeColumnLabel(sortBy)

	fmt.Fprintf(&buf, "\n%s\n", strings.Repeat("=", 154))
	fmt.Fprintf(&buf, "Replay Aggregate Diff (before → after)\n")
	fmt.Fprintf(&buf, "%s\n\n", strings.Repeat("=", 154))

	// Header
	fmt.Fprintf(&buf, "%-4s %16s %14s %14s %23s %23s %23s %10s %10s %10s\n",
		"#", "PatternHash",
		"Count(b→a)", "Errors(b→a)",
		"P50ms(b→a)", "P95ms(b→a)", "Avgms(b→a)", changeLabel, "RowsDiff", "HashDiff")
	fmt.Fprintf(&buf, "%s\n", strings.Repeat("-", 154))

	for i, d := range diffs {
		var (
			bCount, aCount int
			bErr, aErr     int
			bP50, aP50     int64
			bP95, aP95     int64
			bAvg, aAvg     int64
		)
		if d.Before != nil {
			bCount = d.Before.Count
			bErr = d.Before.ErrorCount
			bP50 = d.Before.P50
			bP95 = d.Before.P95
			bAvg = d.Before.Avg
		}
		if d.After != nil {
			aCount = d.After.Count
			aErr = d.After.ErrorCount
			aP50 = d.After.P50
			aP95 = d.After.P95
			aAvg = d.After.Avg
		}

		beforeMetric, afterMetric := diffChangeValues(d, sortBy)

		changeStr := "N/A"
		if beforeMetric > 0 {
			changePct := float64(afterMetric-beforeMetric) / float64(beforeMetric) * 100
			changeStr = fmt.Sprintf("%+.1f%%", changePct)
		} else if afterMetric > 0 {
			changeStr = "+new"
		}

		line := fmt.Sprintf("%-4d %d %6d→%-6d %6d→%-6d %8d→%-8d %8d→%-8d %8d→%-8d %10s %10d %10d",
			i+1, d.PatternHash,
			bCount, aCount,
			bErr, aErr,
			bP50, aP50,
			bP95, aP95,
			bAvg, aAvg,
			changeStr,
			d.RowsDiffCount,
			d.HashDiffCount)

		if useColor {
			line = colorDiffLine(line, beforeMetric, afterMetric)
		}

		buf.WriteString(line)
		buf.WriteString("\n")
	}

	fmt.Fprintf(&buf, "%s\n", strings.Repeat("-", 154))

	// Summary
	var (
		improved, regressed, unchanged int
	)
	for _, d := range diffs {
		if d.Before == nil || d.After == nil {
			continue
		}
		beforeMetric, afterMetric := diffChangeValues(d, sortBy)
		switch {
		case afterMetric > beforeMetric:
			regressed++
		case afterMetric < beforeMetric:
			improved++
		default:
			unchanged++
		}
	}
	fmt.Fprintf(&buf, "\nSummary: %d patterns shown. ", len(diffs))
	if regressed+improved+unchanged > 0 {
		fmt.Fprintf(&buf, "Regressed: %d, Improved: %d, Unchanged: %d\n", regressed, improved, unchanged)
	}
	onlyBefore := lo.CountBy(diffs, func(d DiffPatternStats) bool { return d.After == nil })
	onlyAfter := lo.CountBy(diffs, func(d DiffPatternStats) bool { return d.Before == nil })
	if onlyBefore > 0 || onlyAfter > 0 {
		fmt.Fprintf(&buf, "Only in before: %d, Only in after: %d\n", onlyBefore, onlyAfter)
	}

	return buf.String()
}

// FormatDiffAggregateDetailed formats a comparison report with sample SQL.
func FormatDiffAggregateDetailed(diffs []DiffPatternStats, sortBy AggregateSortBy, topN int) string {
	if topN > 0 && topN < len(diffs) {
		diffs = diffs[:topN]
	}

	var buf bytes.Buffer

	buf.WriteString(FormatDiffAggregate(diffs, sortBy, len(diffs), false))
	buf.WriteString("\nDetailed SQL Samples\n")
	buf.WriteString(strings.Repeat("=", 130))
	buf.WriteString("\n")

	for i, d := range diffs {
		fmt.Fprintf(&buf, "\n#%d PatternHash: %d\n", i+1, d.PatternHash)
		fmt.Fprintf(&buf, "Before: %s\n", formatPatternStatsForDetail(d.Before))
		fmt.Fprintf(&buf, "After : %s\n", formatPatternStatsForDetail(d.After))
		if len(d.QueryMismatches) > 0 {
			fmt.Fprintf(&buf, "Result mismatches: rows=%d hash=%d\n", d.RowsDiffCount, d.HashDiffCount)
			for _, mismatch := range d.QueryMismatches {
				fmt.Fprintf(&buf, "QueryId: %s\n", mismatch.QueryId)
				if mismatch.BeforePattern != mismatch.AfterPattern {
					fmt.Fprintf(&buf, "PatternHash: %d -> %d\n", mismatch.BeforePattern, mismatch.AfterPattern)
				}
				if mismatch.RowsMismatch {
					fmt.Fprintf(&buf, "Rows: %d -> %d\n", mismatch.BeforeRows, mismatch.AfterRows)
				}
				if mismatch.HashMismatch {
					fmt.Fprintf(&buf, "Hash: %s -> %s\n",
						formatDetailValue(mismatch.BeforeRowsHash),
						formatDetailValue(mismatch.AfterRowsHash))
				}
				if mismatch.SQLDiffers {
					buf.WriteString("SQL differs\n")
					buf.WriteString("SQL Before:\n")
					buf.WriteString(formatSampleStmt(mismatch.BeforeStmt))
					buf.WriteString("\n")
					buf.WriteString("SQL After:\n")
					buf.WriteString(formatSampleStmt(mismatch.AfterStmt))
					buf.WriteString("\n")
				}
			}
		}
		buf.WriteString("Sample SQL:\n")
		buf.WriteString(formatSampleStmt(pickSampleStmt(d)))
		buf.WriteString("\n")
		buf.WriteString(strings.Repeat("-", 154))
		buf.WriteString("\n")
	}

	return buf.String()
}

// --- helpers ---

func formatPatternStatsForDetail(s *PatternStats) string {
	if s == nil {
		return "missing"
	}

	return fmt.Sprintf(
		"count=%d errors=%d p50=%dms p95=%dms avg=%dms max=%dms",
		s.Count,
		s.ErrorCount,
		s.P50,
		s.P95,
		s.Avg,
		s.Max,
	)
}

func formatSampleStmt(stmt string) string {
	if strings.TrimSpace(stmt) == "" {
		return "N/A"
	}
	return stmt
}

func pickSampleStmt(d DiffPatternStats) string {
	if d.Before != nil && strings.TrimSpace(d.Before.SampleStmt) != "" {
		return d.Before.SampleStmt
	}
	if d.After != nil && strings.TrimSpace(d.After.SampleStmt) != "" {
		return d.After.SampleStmt
	}
	return ""
}

func formatDetailValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "<empty>"
	}
	return value
}

func flattenQueryResultsByID(stats map[uint64]*PatternStats) map[string]*ReplayResult {
	results := make(map[string]*ReplayResult)
	for _, s := range stats {
		if s == nil {
			continue
		}
		for queryID, result := range s.QueryResults {
			if result != nil {
				results[queryID] = result
			}
		}
	}
	return results
}

// percentile returns the p-th percentile from a sorted slice.
// p is in [0, 1]. Uses nearest-rank method.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	rank := p * float64(len(sorted)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	// Linear interpolation
	frac := rank - float64(lower)
	return sorted[lower] + int64(frac*float64(sorted[upper]-sorted[lower]))
}

func average(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum / int64(len(values))
}

func changeColumnLabel(sortBy AggregateSortBy) string {
	switch sortBy {
	case AggregateSortByP50Change:
		return "P50Change%"
	case AggregateSortByAvgChange:
		return "AvgChange%"
	default:
		return "P95Change%"
	}
}

func diffChangeValues(d DiffPatternStats, sortBy AggregateSortBy) (int64, int64) {
	var beforeMetric, afterMetric int64
	if d.Before != nil {
		beforeMetric = selectedMetricValue(d.Before, sortBy)
	}
	if d.After != nil {
		afterMetric = selectedMetricValue(d.After, sortBy)
	}
	return beforeMetric, afterMetric
}

func selectedMetricValue(s *PatternStats, sortBy AggregateSortBy) int64 {
	if s == nil {
		return 0
	}
	switch sortBy {
	case AggregateSortByP50Change:
		return s.P50
	case AggregateSortByAvgChange:
		return s.Avg
	default:
		return s.P95
	}
}

func p95ChangeAbs(d DiffPatternStats) float64 {
	if d.Before == nil || d.After == nil {
		return 0
	}
	if d.Before.P95 == 0 {
		return float64(d.After.P95)
	}
	change := float64(d.After.P95-d.Before.P95) / float64(d.Before.P95) * 100
	if change < 0 {
		return -change
	}
	return change
}

func p50ChangeAbs(d DiffPatternStats) float64 {
	if d.Before == nil || d.After == nil {
		return 0
	}
	if d.Before.P50 == 0 {
		return float64(d.After.P50)
	}
	change := float64(d.After.P50-d.Before.P50) / float64(d.Before.P50) * 100
	if change < 0 {
		return -change
	}
	return change
}

func avgChangeAbs(d DiffPatternStats) float64 {
	if d.Before == nil || d.After == nil {
		return 0
	}
	if d.Before.Avg == 0 {
		return float64(d.After.Avg)
	}
	change := float64(d.After.Avg-d.Before.Avg) / float64(d.Before.Avg) * 100
	if change < 0 {
		return -change
	}
	return change
}

func maxCount(d DiffPatternStats) int {
	var m int
	if d.Before != nil && d.Before.Count > m {
		m = d.Before.Count
	}
	if d.After != nil && d.After.Count > m {
		m = d.After.Count
	}
	return m
}

func colorDiffLine(line string, beforeMetric, afterMetric int64) string {
	if afterMetric > beforeMetric {
		return color.RedString(line)
	}
	if afterMetric < beforeMetric {
		return color.GreenString(line)
	}
	return line
}
