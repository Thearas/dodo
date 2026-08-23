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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Thearas/sqlsplit"
	tidbparser "github.com/pingcap/tidb/pkg/parser"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

// SQLComplexity represents the complexity metrics of a SQL query
type SQLComplexity struct {
	Joins           int // Number of JOIN operations
	Subqueries      int // Number of subqueries (nested SELECT)
	Aggregates      int // Number of aggregate functions (COUNT, SUM, etc.)
	SetOperations   int // Number of UNION/INTERSECT/EXCEPT
	CaseWhen        int // Number of CASE expressions
	WindowFunctions int // Number of window functions (OVER)
	CTEs            int // Number of CTEs (WITH ... AS)
	NestingDepth    int // Maximum nesting depth
	Score           int // Weighted complexity score
}

// SQLPattern represents a normalized SQL pattern with its count
type SQLPattern struct {
	Pattern    string        // normalized SQL pattern
	Count      int           // occurrence count
	Complexity SQLComplexity // complexity metrics
	Samples    string        // original SQL samples
	SampleSQL  string        // pure SQL without dodo prefix (for replay output)
}

// OriginalSQL represents an original SQL entry with its pattern hash for replay output
type OriginalSQL struct {
	Meta        *ReplaySqlMeta // parsed dodo meta (nil if no dodo prefix)
	SQL         string         // original SQL statement (without prefix)
	PatternHash uint64         // hash of the normalized pattern
	DbName      string         // database name extracted from dodo prefix
}

// Regex patterns for complexity calculation (compiled once)
var (
	joinRe       = regexp.MustCompile(`(?i)\b(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\b`)
	subqueryRe   = regexp.MustCompile(`(?i)\(\s*SELECT\b`)
	aggregateRe  = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|COLLECT_LIST|COLLECT_SET)\s*\(`)
	setOpRe      = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	caseRe       = regexp.MustCompile(`(?i)\bCASE\b`)
	windowRe     = regexp.MustCompile(`(?i)\bOVER\s*\(`)
	cteRe        = regexp.MustCompile(`(?i)(?:\bWITH\s+(?:RECURSIVE\s+)?|,)\s*([A-Z_][A-Z0-9_]*)\s*(?:\([^)]*\))?\s+AS\s*\(`)
	structFuncRe = regexp.MustCompile(`(?i)\b(STRUCT_ELEMENT|ELEMENT_AT|ARRAY_MAP|ARRAY_FILTER|ARRAY_EXISTS)\s*\(`)
)

// CalculateSQLComplexity calculates complexity metrics for a SQL query
func CalculateSQLComplexity(sql string) SQLComplexity {
	c := SQLComplexity{}

	// Count various SQL constructs
	c.Joins = len(joinRe.FindAllString(sql, -1))
	c.Subqueries = len(subqueryRe.FindAllString(sql, -1))
	c.Aggregates = len(aggregateRe.FindAllString(sql, -1))
	c.SetOperations = len(setOpRe.FindAllString(sql, -1))
	c.CaseWhen = len(caseRe.FindAllString(sql, -1))
	c.WindowFunctions = len(windowRe.FindAllString(sql, -1))
	c.CTEs = len(cteRe.FindAllString(sql, -1))
	c.NestingDepth = maxParenDepth(sql)

	// Count complex functions (struct/array operations common in Doris)
	structFuncs := len(structFuncRe.FindAllString(sql, -1))

	// Calculate weighted score
	// Weights based on typical query complexity impact
	c.Score = c.Joins*2 +
		c.Subqueries*3 +
		c.Aggregates*1 +
		c.SetOperations*2 +
		c.CaseWhen*1 +
		c.WindowFunctions*2 +
		c.CTEs*2 +
		structFuncs*1 +
		max(0, c.NestingDepth-1)

	return c
}

// maxParenDepth calculates the maximum parenthesis nesting depth
func maxParenDepth(sql string) int {
	maxDepth := 0
	currentDepth := 0
	for _, ch := range sql {
		switch ch {
		case '(':
			currentDepth++
			if currentDepth > maxDepth {
				maxDepth = currentDepth
			}
		case ')':
			if currentDepth > 0 {
				currentDepth--
			}
		default:
			// ignore
		}
	}
	return maxDepth
}

// TopSqlSortBy defines the sorting criteria for top SQL patterns
type TopSqlSortBy string

const (
	SortByCount      TopSqlSortBy = "count"      // Sort by occurrence count (default)
	SortByComplexity TopSqlSortBy = "complexity" // Sort by complexity score
	SortByCombined   TopSqlSortBy = "combined"   // Sort by count * complexity (for regression testing)
)

// TopSql analyzes SQL files and finds the most common SQL patterns per database
// minCount filters out patterns with count less than this value (they won't be included in statistics)
// replayOutput generates a replay SQL file with same-pattern SQLs replaced by sample SQL
// parallel specifies the number of parallel workers (0 = number of CPUs)
//
//nolint:revive
func TopSql(ctx context.Context, sqlFiles []string, topN int, sortBy TopSqlSortBy, minCount int, replayOutput bool, fromMs, toMs int64, dbs []string, parallel int) error {
	// Expand glob patterns
	allFiles, err := FileGlob(sqlFiles)
	if err != nil {
		return err
	}

	if len(allFiles) == 0 {
		return fmt.Errorf("no SQL files found matching patterns: %v", sqlFiles)
	}

	dbFilter := lo.SliceToMap(lo.Uniq(dbs), func(db string) (string, struct{}) { return db, struct{}{} })

	// Determine number of workers
	numWorkers := parallel
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	logrus.Infof("Analyzing %d SQL file(s) with %d workers...", len(allFiles), numWorkers)

	// Analyze files concurrently, each file produces its own local results
	type fileResult struct {
		dbPatterns   map[string]map[uint64]*SQLPattern
		originalSQLs []OriginalSQL
		count        int
		err          error
		file         string
	}

	// Use errgroup with limited parallelism
	resultChan := make(chan fileResult, len(allFiles))
	g := ParallelGroup(numWorkers)

	for _, file := range allFiles {
		g.Go(func() error {
			select {
			case <-ctx.Done():
				resultChan <- fileResult{file: file, err: ctx.Err()}
				return ctx.Err()
			default:
			}

			localPatterns := make(map[string]map[uint64]*SQLPattern)
			var localOriginalSQLs []OriginalSQL
			if replayOutput {
				localOriginalSQLs = make([]OriginalSQL, 0)
			}

			count, err := analyzeFileLocal(file, localPatterns, replayOutput, &localOriginalSQLs, fromMs, toMs, dbFilter)
			resultChan <- fileResult{
				dbPatterns:   localPatterns,
				originalSQLs: localOriginalSQLs,
				count:        count,
				err:          err,
				file:         file,
			}
			return nil // Don't propagate file errors, handle them in merge phase
		})
	}

	// Wait for all workers to finish in a separate goroutine
	go func() {
		_ = g.Wait() // Errors are handled via resultChan
		close(resultChan)
	}()

	// Merge results (single-threaded, no locks needed)
	dbPatternCounts := make(map[string]map[uint64]*SQLPattern)
	var originalSQLsByFile map[string][]OriginalSQL
	if replayOutput {
		originalSQLsByFile = make(map[string][]OriginalSQL)
	}
	totalSQLs := 0

	for result := range resultChan {
		if result.err != nil {
			logrus.Warnf("Failed to analyze file %s: %v", result.file, result.err)
			continue
		}

		totalSQLs += result.count

		// Merge patterns
		for dbName, patterns := range result.dbPatterns {
			if _, ok := dbPatternCounts[dbName]; !ok {
				dbPatternCounts[dbName] = make(map[uint64]*SQLPattern)
			}
			for hash, pattern := range patterns {
				if existing, ok := dbPatternCounts[dbName][hash]; ok {
					existing.Count += pattern.Count
				} else {
					dbPatternCounts[dbName][hash] = pattern
				}
			}
		}

		// Group original SQLs by source file
		if replayOutput && len(result.originalSQLs) > 0 {
			originalSQLsByFile[result.file] = result.originalSQLs
		}
	}

	if len(dbPatternCounts) == 0 {
		logrus.Info("No SQL statements found")
		return nil
	}

	// prepare output directory
	outDir := filepath.Join("output", "topsql")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir %s: %w", outDir, err)
	}
	timestamp := time.Now().Unix()
	outFile := filepath.Join(outDir, fmt.Sprintf("topsql_%s_%d.txt", string(sortBy), timestamp))
	outBuilder := bytes.NewBuffer(make([]byte, 0, 1024*1024))

	// Collect sample entries for splitting into multiple files
	var sampleEntries []sampleEntry

	// Sort database names for consistent output
	dbNames := make([]string, 0, len(dbPatternCounts))
	for dbName := range dbPatternCounts {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	totalPatterns := 0
	totalTopNCount := 0    // sum of all databases' top N patterns' counts
	totalSQLsFiltered := 0 // total SQLs after filtering by minCount
	for _, dbName := range dbNames {
		patternCounts := dbPatternCounts[dbName]

		// Convert to slice, filter by minCount, and sort
		patterns := make([]*SQLPattern, 0, len(patternCounts))
		for _, p := range patternCounts {
			if p.Count >= minCount {
				patterns = append(patterns, p)
			}
		}

		// Skip database if no patterns after filtering
		if len(patterns) == 0 {
			continue
		}

		totalPatterns += len(patterns)

		sortPatterns(patterns, sortBy)

		// Calculate total SQLs for this db (only filtered patterns)
		dbTotalSQLs := 0
		for _, p := range patterns {
			dbTotalSQLs += p.Count
		}
		totalSQLsFiltered += dbTotalSQLs

		// Show top N for this database
		showN := min(topN, len(patterns))
		dbHeader := fmt.Sprintf("\n\n%s\nDatabase: %s (%d unique patterns, %d total SQLs)\n%s\n",
			strings.Repeat("=", 80), dbName, len(patterns), dbTotalSQLs, strings.Repeat("=", 80))

		fmt.Print(dbHeader)
		outBuilder.WriteString(dbHeader)

		topNCount := 0 // sum of top N patterns' counts
		for i := range showN {
			p := patterns[i]
			topNCount += p.Count
			percentage := float64(p.Count) / float64(dbTotalSQLs) * 100
			combinedScore := p.Count * p.Complexity.Score
			header := fmt.Sprintf("\n#%d - Count: %d (%.2f%%), Complexity: %d, Combined: %d\n", i+1, p.Count, percentage, p.Complexity.Score, combinedScore)
			details := fmt.Sprintf("     [JOINs:%d, Subqueries:%d, Aggs:%d, Windows:%d, Depth:%d]\n",
				p.Complexity.Joins, p.Complexity.Subqueries, p.Complexity.Aggregates,
				p.Complexity.WindowFunctions, p.Complexity.NestingDepth)
			patternLine := fmt.Sprintf("Pattern: %s\n", p.Pattern)
			var sampleLine string
			if len(p.Samples) > 0 {
				sampleLine = fmt.Sprintf("Sample:  %s\n", p.Samples)
				// collect sample SQL entry for split output
				sqlSample := strings.TrimSpace(p.SampleSQL)
				if len(sqlSample) > 0 {
					// ensure ends with semicolon
					if !strings.HasSuffix(sqlSample, ";") {
						sqlSample = sqlSample + ";"
					}
					// Build entry text with dodo meta
					sampleMeta := ReplaySqlMeta{
						Db:          dbName,
						Complexity:  p.Complexity.Score,
						PatternHash: hashString(p.Pattern),
					}
					var entryText string
					if b, err := json.Marshal(sampleMeta); err == nil {
						entryText = fmt.Sprintf("/*dodo%s*/ %s\n\n", b, sqlSample)
					} else {
						entryText = sqlSample + "\n\n"
					}
					sampleEntries = append(sampleEntries, sampleEntry{
						text:       entryText,
						complexity: p.Complexity.Score,
					})
				}
			}

			// print to stdout
			fmt.Print(header)
			fmt.Print(details)
			fmt.Print(patternLine)
			if sampleLine != "" {
				fmt.Print(sampleLine)
			}
			fmt.Println(strings.Repeat("-", 80))

			// append to output builder
			outBuilder.WriteString(header)
			outBuilder.WriteString(details)
			outBuilder.WriteString(patternLine)
			if sampleLine != "" {
				outBuilder.WriteString(sampleLine)
			}
			outBuilder.WriteString(strings.Repeat("-", 80))
			outBuilder.WriteString("\n")
		}

		// Print top N coverage for this database
		topNCoverage := float64(topNCount) / float64(dbTotalSQLs) * 100
		coverageLine := fmt.Sprintf("\nTop %d patterns cover %.2f%% of SQLs in database %s\n", showN, topNCoverage, dbName)
		fmt.Print(coverageLine)
		outBuilder.WriteString(coverageLine)

		totalTopNCount += topNCount
	}

	// write output files
	if err := os.WriteFile(outFile, outBuilder.Bytes(), 0600); err != nil {
		return fmt.Errorf("failed to write topsql output file %s: %v", outFile, err)
	}
	logrus.Infof("Wrote topsql report to %s", outFile)

	if len(sampleEntries) > 0 {
		if err := writeSampleFiles(outDir, sampleEntries); err != nil {
			return err
		}
	}

	// Generate replay SQL files if enabled (one per input SQL file)
	if replayOutput && len(originalSQLsByFile) > 0 {
		logrus.Infoln("Output replay SQL files")
		for sourceFile, sqls := range originalSQLsByFile {
			if len(sqls) == 0 {
				continue
			}

			baseName := filepath.Base(sourceFile)
			replayFile := filepath.Join(outDir, fmt.Sprintf("replay_%s.sql", baseName))
			var replayBuilder strings.Builder

			for _, origSQL := range sqls {
				// Look up the sample SQL for this pattern
				patternMap, ok := dbPatternCounts[origSQL.DbName]
				if !ok {
					continue
				}
				pattern, ok := patternMap[origSQL.PatternHash]
				if !ok {
					continue
				}

				// Build the replay line: original prefix + sample SQL
				sampleSQL := strings.TrimSpace(pattern.SampleSQL)
				if sampleSQL == "" {
					continue
				}

				// Ensure SQL ends with semicolon
				if !strings.HasSuffix(sampleSQL, ";") {
					sampleSQL = sampleSQL + ";"
				}

				// Combine dodo meta prefix (with pattern hash and complexity) with sample SQL
				if origSQL.Meta != nil {
					origSQL.Meta.PatternHash = origSQL.PatternHash
					origSQL.Meta.Complexity = pattern.Complexity.Score
					if b, err := json.Marshal(origSQL.Meta); err == nil {
						fmt.Fprintf(&replayBuilder, "/*dodo%s*/ ", b)
					}
				}
				replayBuilder.WriteString(sampleSQL)
				replayBuilder.WriteString("\n")
			}

			if replayBuilder.Len() > 0 {
				if err := os.WriteFile(replayFile, []byte(replayBuilder.String()), 0600); err != nil {
					return fmt.Errorf("failed to write replay sql file %s: %v", replayFile, err)
				}
				logrus.Infof("Wrote replay SQLs to %s", replayFile)
			}
		}
	}

	// Print final summary
	if minCount > 0 {
		filteredOut := totalSQLs - totalSQLsFiltered
		fmt.Printf("\nFiltered: patterns with count < %d are excluded (%d SQLs filtered out)\n", minCount, filteredOut)
	}
	fmt.Printf("\nSummary: %d databases, %d unique patterns, %d total SQLs\n", len(dbPatternCounts), totalPatterns, totalSQLsFiltered)
	if totalSQLsFiltered > 0 {
		totalCoverage := float64(totalTopNCount) / float64(totalSQLsFiltered) * 100
		fmt.Printf("Top SQL coverage: %d / %d (%.2f%%)\n", totalTopNCount, totalSQLsFiltered, totalCoverage)
	}

	return nil
}

// sortPatterns sorts patterns by the given criteria
func sortPatterns(patterns []*SQLPattern, sortBy TopSqlSortBy) {
	switch sortBy {
	case SortByComplexity:
		slices.SortFunc(patterns, func(a, b *SQLPattern) int {
			if a.Complexity.Score != b.Complexity.Score {
				return cmp.Compare(b.Complexity.Score, a.Complexity.Score)
			}
			return cmp.Compare(b.Count, a.Count)
		})
	case SortByCombined:
		slices.SortFunc(patterns, func(a, b *SQLPattern) int {
			scoreI := a.Count * a.Complexity.Score
			scoreJ := b.Count * b.Complexity.Score
			if scoreI != scoreJ {
				return cmp.Compare(scoreJ, scoreI)
			}
			return cmp.Compare(b.Count, a.Count)
		})
	default: // SortByCount
		slices.SortFunc(patterns, func(a, b *SQLPattern) int {
			if a.Count != b.Count {
				return cmp.Compare(b.Count, a.Count)
			}
			return cmp.Compare(b.Complexity.Score, a.Complexity.Score)
		})
	}
}

// analyzeFileLocal analyzes a single SQL file with local maps (thread-safe for concurrent use)
// Each call creates its own local data structures, results should be merged after all files are processed
//
//nolint:revive
func analyzeFileLocal(filePath string, localPatterns map[string]map[uint64]*SQLPattern, collectOriginal bool, originalSQLs *[]OriginalSQL, fromMs, toMs int64, dbFilter map[string]struct{}) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	iter, err := sqlsplit.SplitFd(f, "utf8")
	if err != nil {
		return 0, fmt.Errorf("cannot split sqls from file %s: %w", f.Name(), err)
	}

	count := 0
	for sql, err := range iter {
		if err != nil {
			return count, fmt.Errorf("cannot read sql from file %s: %w", f.Name(), err)
		}

		// Extract SQL from dodo format if present: /*dodo{...}*/ SQL
		dbName, meta, sql := extractSQL(sql)
		if sql == "" {
			continue
		}

		// Filter by time range if specified
		if (fromMs > 0 || toMs > 0) && (meta == nil || !meta.matchTime(fromMs, toMs)) {
			continue
		}
		if _, ok := dbFilter[dbName]; len(dbFilter) > 0 && !ok {
			continue
		}

		// Ensure db pattern map exists
		if _, ok := localPatterns[dbName]; !ok {
			localPatterns[dbName] = make(map[uint64]*SQLPattern)
		}

		// Build prefix string for sample display
		var prefix string
		if meta != nil {
			if b, err := json.Marshal(meta); err == nil {
				prefix = fmt.Sprintf("/*dodo%s*/", b)
			}
		}

		hash, err := addSQLPattern(prefix, sql, localPatterns[dbName])
		if err == nil {
			count++
			// Track original SQL for replay output if enabled
			if collectOriginal && originalSQLs != nil {
				*originalSQLs = append(*originalSQLs, OriginalSQL{
					Meta:        meta,
					SQL:         sql,
					PatternHash: hash,
					DbName:      dbName,
				})
			}
		}
	}

	return count, nil
}

// addSQLPattern normalizes SQL and adds it to the pattern counts
// prefix is the dodo prefix (if any), sql is the actual SQL statement
// returns the pattern hash for replay output tracking
func addSQLPattern(prefix, sql string, patternCounts map[uint64]*SQLPattern) (uint64, error) {
	// Normalize the SQL to get the pattern
	normalizedSQL := tidbparser.NormalizeKeepHint(sql)
	if normalizedSQL == "" {
		return 0, errors.New("failed to normalize SQL")
	}

	// Pattern is just the normalized SQL (we group by db separately)
	pattern := normalizedSQL

	// Use hash as map key for better performance
	hash := hashString(pattern)

	// Original SQL with prefix for sample (keep prefix for context)
	originalSQL := sql
	if prefix != "" {
		originalSQL = prefix + " " + sql
	}

	// Update pattern count
	if p, exists := patternCounts[hash]; exists {
		p.Count++
	} else {
		patternCounts[hash] = &SQLPattern{
			Pattern:    pattern,
			Count:      1,
			Complexity: CalculateSQLComplexity(normalizedSQL), // Use normalized SQL for complexity
			Samples:    originalSQL,
			SampleSQL:  sql, // Store pure SQL without prefix for replay output
		}
	}

	return hash, nil
}

// extractSQL extracts the SQL statement from a line (with or without dodo prefix)
// Returns the database name, parsed dodo meta (if any), and the SQL statement
func extractSQL(line string) (dbName string, meta *ReplaySqlMeta, sql string) {
	line = strings.TrimSpace(line)
	dbName = "(no db)" // default for SQLs without dodo prefix

	// Check if line has dodo format: /*dodo{...}*/ SQL
	if strings.HasPrefix(line, ReplaySqlPrefix) {
		suffixIdx := strings.Index(line, ReplaySqlSuffix)
		if suffixIdx > 0 && suffixIdx+len(ReplaySqlSuffix) < len(line) {
			sql = strings.TrimSpace(line[suffixIdx+len(ReplaySqlSuffix):])

			// Decode dodo meta
			metaStart := len(ReplaySqlPrefix) - 1 // include '{'
			metaEnd := suffixIdx                  // exclude '*/'
			if metaEnd > metaStart {
				if m, err := DecodeReplaySqlMeta([]byte(line[metaStart:metaEnd])); err == nil {
					meta = &m
					if m.Db != "" {
						dbName = m.Db
					}
				}
			}

			return dbName, meta, sql
		}
	}

	return dbName, nil, line
}

// sampleEntry represents a single sample SQL entry for split output
type sampleEntry struct {
	text       string // full text including dodo meta prefix and trailing newlines
	complexity int    // complexity score
}

const (
	// sampleMaxBytes is the max byte size per split sample file.
	// 64KB is a reasonable size for LLM consumption (~8K tokens).
	sampleMaxBytes = 32 * 1024

	// sampleMaxComplexity is the max total complexity score per split sample file.
	// Prevents packing too many complex queries into one file for AI processing.
	sampleMaxComplexity = 200
)

// writeSampleFiles splits sample entries into multiple files based on size and complexity budgets.
// If all entries fit within a single file's budget, writes a single sample.sql for backward compatibility.
// Otherwise, writes sample_0.sql, sample_1.sql, etc.
func writeSampleFiles(outDir string, entries []sampleEntry) error {
	// Calculate total size to check if splitting is needed
	totalSize := 0
	totalComplexity := 0
	for _, e := range entries {
		totalSize += len(e.text)
		totalComplexity += e.complexity
	}

	// If everything fits in one file, write as sample.sql (backward compatible)
	if totalSize <= sampleMaxBytes && totalComplexity <= sampleMaxComplexity {
		var buf strings.Builder
		buf.Grow(totalSize)
		for _, e := range entries {
			buf.WriteString(e.text)
		}
		f := filepath.Join(outDir, "sample.sql")
		if err := os.WriteFile(f, []byte(buf.String()), 0600); err != nil {
			return fmt.Errorf("failed to write sample sql file %s: %w", f, err)
		}
		logrus.Infof("Wrote sample SQLs to %s", f)
		return nil
	}

	// Split into multiple files
	fileIdx := 0
	currentSize := 0
	currentComplexity := 0
	var buf strings.Builder

	flush := func() error {
		if buf.Len() == 0 {
			return nil
		}
		f := filepath.Join(outDir, fmt.Sprintf("sample_%d.sql", fileIdx))
		if err := os.WriteFile(f, []byte(buf.String()), 0600); err != nil {
			return fmt.Errorf("failed to write sample sql file %s: %w", f, err)
		}
		logrus.Infof("Wrote sample SQLs to %s", f)
		fileIdx++
		buf.Reset()
		currentSize = 0
		currentComplexity = 0
		return nil
	}

	for _, e := range entries {
		entrySize := len(e.text)
		// Start a new file if adding this entry would exceed either budget
		// (always add at least one entry per file)
		if (currentSize+entrySize > sampleMaxBytes || currentComplexity+e.complexity > sampleMaxComplexity) && buf.Len() > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		buf.WriteString(e.text)
		currentSize += entrySize
		currentComplexity += e.complexity
	}

	return flush()
}

// hashString returns a 64-bit FNV-1a hash of the string
func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
