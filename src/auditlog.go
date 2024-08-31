package src

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"golang.org/x/text/transform"

	"github.com/Thearas/dodo/src/parser"
)

var (
	// filterStmtRe filters out some statements from the audit log.
	filterStmtRe = regexp.MustCompile("(?i)^(EXPLAIN|SHOW|USE)")

	// The field keys in the audit log that needs to be captured.
	auditCapKeys = map[string]int{
		"Timestamp": 0,
		"Client":    1,
		"User":      2,
		"Db":        3,
		"Time":      4,
		"Time(ms)":  4,
		"QueryId":   5,
		"Stmt":      6,
		"IsQuery":   7,
		"State":     8,
	}

	// The field keys that may appear after the "Stmt" field in the audit log.
	auditKeysMayAfterStmt = map[string]bool{
		"CpuTimeMS":                  true,
		"PeakMemoryBytes":            true,
		"ScanBytes":                  true,
		"ScanRows":                   true,
		"ReturnRows":                 true,
		"ShuffleSendRows":            true,
		"ShuffleSendBytes":           true,
		"ScanBytesFromLocalStorage":  true,
		"ScanBytesFromRemoteStorage": true,
		"FuzzyVariables":             true,
		"CommandType":                true,
		"StmtType":                   true,
		"StmtId":                     true,
		"SqlHash":                    true,
		"SqlDigest":                  true,
		"IsNereids":                  true,
		"WorkloadGroup":              true,
		"ComputeGroupName":           true,
	}

	// Will not dump queries perform from these DBs. E.g. "__internal_schema,information_schema,mysql"
	auditWithoutQueryDBsEnv = os.Getenv("DODO_AUDIT_WITHOUT_QUERY_DB")
	auditWithoutQueryDBs    = lo.FilterSliceToMap(strings.Split(auditWithoutQueryDBsEnv, ","), func(db string) (string, struct{}, bool) {
		return db, struct{}{}, db != ""
	})
)

type AuditLogScanOpts struct {
	// filter
	Catalog            string
	DBs                []string
	QueryMinDurationMs int64
	QueryStates        []string
	OnlySelect         bool
	From, To           string

	Strict bool
}

// push down filter to db
func (opts *AuditLogScanOpts) sqlConditions(hasCatalogColumn bool) string { //nolint:revive
	// filter out doris self-executed sqls
	conditions := " client_ip != ''"
	if opts.Catalog != "" && hasCatalogColumn {
		conditions += fmt.Sprintf(" AND `catalog` = '%s'", opts.Catalog)
	}
	if len(opts.DBs) > 0 {
		conditions += fmt.Sprintf(` AND db IN ('%s')`, strings.Join(opts.DBs, `', '`))
	}
	if opts.QueryMinDurationMs > 0 {
		conditions += fmt.Sprintf(` AND query_time >= %d`, opts.QueryMinDurationMs)
	}
	if len(opts.QueryStates) > 0 {
		conditions += fmt.Sprintf(" AND `state` IN ('%s')", strings.Join(opts.QueryStates, `', '`))
	}
	if opts.OnlySelect {
		conditions += ` AND is_query = 1`
	}

	if opts.From != "" {
		conditions += fmt.Sprintf(" AND `time` >= '%s'", opts.From)
	}
	if opts.To != "" {
		conditions += fmt.Sprintf(" AND `time` <= '%s'", opts.To)
	}
	return conditions
}

type SqlWriter interface {
	io.Closer
	WriteSql(s string) error
}

// ExtractQueriesFromAuditLog extracts the query from an audit log.
func ExtractQueriesFromAuditLogs(
	ctx context.Context,
	writers []SqlWriter,
	auditlogPaths []string,
	encoding string,
	opts AuditLogScanOpts,
	parallel int,
) (int, error) {
	logrus.Infof("Extracting queries of database %v, audit logs: %v", opts.DBs, auditlogPaths)

	g := ParallelGroup(parallel)

	counter := &atomic.Int32{}
	for i, auditlogPath := range auditlogPaths {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		g.Go(func() error {
			f, err := os.Open(auditlogPath)
			if err != nil {
				logrus.Errorln("Unable to open audit log file:", auditlogPath)
				return err
			}
			defer f.Close()

			// detect encoding
			enc, err := DetectFileEncoding(encoding, f)
			if err != nil {
				return err
			}

			buf := bufio.NewScanner(transform.NewReader(f, enc.NewDecoder()))
			buf.Buffer(make([]byte, 0, 10*1024*1024), 10*1024*1024)

			logrus.Debugln("Extracting queries from audit log:", auditlogPath, "with encoding:", enc)

			// read log file line by line
			s := NewAuditLogScanner(opts)
			count, err := extractQueriesFromAuditLog(ctx, writers[i], s, buf)
			if err != nil {
				return err
			}

			counter.Add(int32(count))

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return int(counter.Load()), nil
}

func extractQueriesFromAuditLog(
	ctx context.Context,
	w SqlWriter,
	s *AuditLogScanner,
	auditlog *bufio.Scanner,
) (int, error) {
	// read log file line by line
	if !auditlog.Scan() {
		logrus.Warningln("Failed to scan audit log file, maybe empty?")
		return 0, nil
	}
	var (
		line   = auditlog.Bytes()
		lineRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d`)
		eof    = false
		count  = 0
	)

	for !eof {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		oneLog := bytes.Clone(line)

		// one log may have multiple lines
		// a line not starts with 'yyyy-mm-dd HH:MM:SS,S' is considered belonging to the previous line
		for {
			if !auditlog.Scan() {
				eof = true
				break
			}
			line = auditlog.Bytes()

			const minLenToMatch = len("yyyy-mm-dd HH:MM:SS,S")
			if len(line) >= minLenToMatch && lineRe.Match(line[:minLenToMatch+1]) {
				break
			}

			// append to previous line
			oneLog = append(oneLog, '\n')
			oneLog = append(oneLog, line...)
		}

		// parse log
		if err := s.ScanOne(oneLog); err != nil {
			logrus.Errorln("Failed to scan audit log file")
			return 0, err
		}
		// write to file immediately to avoid using too much memory
		count_, err := s.Consume(w)
		if err != nil {
			logrus.Errorln("Failed to output audit log")
			return 0, err
		}
		count += count_
	}

	return count, nil
}

// Not thread safe.
type AuditLogScanner struct {
	AuditLogScanOpts

	sqls             []string
	distinctQueryIds map[string]struct{}
	distinctQueryTs  string
	warnedMissingCtl bool

	dbFilter    map[string]struct{}
	stateFilter map[string]struct{}
}

func NewAuditLogScanner(opts AuditLogScanOpts) *AuditLogScanner {
	return &AuditLogScanner{
		AuditLogScanOpts: opts,
		sqls:             make([]string, 0, 1024),
		distinctQueryIds: make(map[string]struct{}),
		dbFilter:         lo.SliceToMap(opts.DBs, func(db string) (string, struct{}) { return db, struct{}{} }),
		stateFilter:      lo.SliceToMap(opts.QueryStates, func(s string) (string, struct{}) { return s, struct{}{} }),
	}
}

func (s *AuditLogScanner) ScanOne(oneLog []byte) error {
	caps := [9]string{}
	capCount := 0
	catalog := ""
	lastCapIdx := -1
	log := oneLog

	for i := bytes.IndexByte(log, '|'); len(log) != 0; i = bytes.IndexByte(log, '|') {
		isLastField := i == -1
		if isLastField {
			i = len(log)
			newlineIdx := bytes.IndexByte(log, '\n')
			if newlineIdx != -1 {
				i = newlineIdx
			}
		}
		kv := log[:i]
		if isLastField {
			log = log[len(log):]
		} else {
			log = log[i+1:]
		}
		equalIdx := bytes.IndexByte(kv, '=')

		var key string
		if equalIdx >= 2 {
			b := kv[:equalIdx]
			key = *(*string)(unsafe.Pointer(&b))
		}

		// keys too short/long or contains invalid chars are considered invalid
		if equalIdx < 2 || equalIdx > 40 || strings.ContainsFunc(key, func(c rune) bool {
			return !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '(' || c == ')')
		}) {
			if lastCapIdx != -1 {
				// append to the last cap
				caps[lastCapIdx] += "|" + *(*string)(unsafe.Pointer(&kv)) //nolint:gosec
			}
			continue
		}

		// capture field
		if key == "Ctl" && catalog == "" {
			b := kv[equalIdx+1:]
			catalog = *(*string)(unsafe.Pointer(&b))
			continue
		}

		capIdx, ok := auditCapKeys[key]
		if ok && caps[capIdx] == "" {
			lastCapIdx = capIdx
			b := kv[equalIdx+1:]
			caps[capIdx] = *(*string)(unsafe.Pointer(&b))
			capCount++

			// remove Doris self queries
			if key == "Client" && len(b) == 0 {
				return nil
			}
			continue
		}
		// not a valid kv after stmt, append to stmt
		if lastCapIdx == auditCapKeys["Stmt"] && !auditKeysMayAfterStmt[key] {
			// append to the stmt
			caps[lastCapIdx] += "|" + *(*string)(unsafe.Pointer(&kv))
			continue
		}
		lastCapIdx = -1
	}

	// old Doris does not have Timestamp, replace it with log timestamp
	if capCount < len(caps) && caps[auditCapKeys["Timestamp"]] == "" {
		before, _, _ := bytes.Cut(oneLog, []byte{'['})
		caps[auditCapKeys["Timestamp"]] = strings.TrimSpace(*(*string)(unsafe.Pointer(&before)))
		capCount++
	}

	if capCount == len(caps) {
		s.onMatch(caps, catalog, false)
	}

	return nil
}

func (s *AuditLogScanner) Consume(w SqlWriter) (int, error) {
	count := len(s.sqls)
	if count == 0 {
		return 0, nil
	}

	for _, s := range s.sqls {
		if err := w.WriteSql(s); err != nil {
			logrus.Errorln("Failed to output audit log")
			return 0, err
		}
	}
	s.sqls = s.sqls[:0]
	return count, nil
}

func (s *AuditLogScanner) onMatch(caps [9]string, catalog string, skipOptsFilter bool) {
	time, client, user, db, durationMs, queryId, stmt, isQuery, state := caps[0], caps[1], caps[2], caps[3], cast.ToInt64(caps[4]), caps[5], caps[6], caps[7], caps[8]
	time = strings.Replace(time, ",", ".", 1) // 2006-01-02 15:04:05,000 -> 2006-01-02 15:04:05.000
	stmt = strings.TrimSpace(stmt)

	// BUG: Doris may concurrently write many same query_id with same ts.
	// To avoid duplicate queries with same query_id:
	if _, ok := s.distinctQueryIds[queryId]; ok {
		logrus.Debugln("ignore sql with duplicated query_id:", queryId)
		return
	} else if time <= s.distinctQueryTs && len(s.distinctQueryIds) < 1024 {
		s.distinctQueryIds[queryId] = struct{}{}
	} else {
		clear(s.distinctQueryIds)
		s.distinctQueryIds[queryId] = struct{}{}
		s.distinctQueryTs = time
	}

	ok := s.filterStmtFromMatch(time, db, queryId, stmt, isQuery, state, durationMs, catalog, skipOptsFilter)
	if !ok {
		return
	}

	// TODO: May incorrectly unescaped SQLs that originally contain multiline string.
	stmt = s.unescapeStmt(stmt)

	if s.Strict && s.validateSQL(queryId, stmt) != nil {
		return
	}

	// add leading meta comment
	outputStmt := EncodeReplaySql(time, client, user, db, queryId, stmt, durationMs)

	s.sqls = append(s.sqls, outputStmt)
}

//nolint:revive
func (s *AuditLogScanner) filterStmtFromMatch(
	time, db, queryId, stmt, isQuery, state string, durationMs int64, catalog string,
	skipOptsFilter bool,
) bool {
	// remove empty stmt
	if len(stmt) == 0 {
		return false
	}

	if !skipOptsFilter {
		if s.Catalog != "" {
			if catalog == "" {
				if !s.warnedMissingCtl {
					logrus.Warnln("audit log has no 'Ctl' field, skip catalog filter for compatibility")
					s.warnedMissingCtl = true
				}
			} else if catalog != s.Catalog {
				return false
			}
		}

		// remove non-select queries if only_select is set
		if s.OnlySelect && isQuery != "true" {
			return false
		}

		// remove non-matching dbs and states
		if len(s.dbFilter) > 0 {
			if _, ok := s.dbFilter[db]; !ok {
				return false
			}
		} else if _, ok := auditWithoutQueryDBs[db]; ok {
			return false
		}
		if len(s.stateFilter) > 0 {
			if _, ok := s.stateFilter[state]; !ok {
				return false
			}
		}

		if s.From != "" && strings.SplitN(time, ".", 2)[0] < s.From {
			return false
		}
		if s.To != "" && strings.SplitN(time, ".", 2)[0] > s.To {
			return false
		}

		if s.QueryMinDurationMs > 0 {
			if durationMs < s.QueryMinDurationMs {
				return false
			}
		}
	}

	// remove truncated queries (which length is larger than audit_plugin_max_sql_length)
	if logStmtTruncated(queryId, stmt) {
		return false
	}

	// remove dodo self queries
	if strings.HasPrefix(stmt, InternalSqlComment) {
		return false
	}

	// remove explain, show and use statements
	if !s.OnlySelect && filterStmtRe.MatchString(stmt) {
		return false
	}

	return true
}

// unescapeStmt unescapes the \\n, \\t and \\r in SQL statement.
// NOTE: It will not unescape chars in string literals, comments and multi-line comments.
func (*AuditLogScanner) unescapeStmt(stmt string) string {
	var (
		w           = strings.Builder{}
		ignoreUntil = ""
	)
	w.Grow(len(stmt))
	for i := 0; i < len(stmt); i++ {
		curr := stmt[i]

		if i < len(stmt)-1 {
			if ignoreUntil != "" {
				if curr == ignoreUntil[0] && (len(ignoreUntil) < 2 || stmt[i+1] == ignoreUntil[1]) {
					ignoreUntil = ""
				}
			} else if curr == '\'' || curr == '"' {
				ignoreUntil = string(curr)
			} else if curr == '/' && stmt[i+1] == '*' {
				ignoreUntil = "*/"
			} else if curr == '-' && stmt[i+1] == '-' {
				ignoreUntil = "\\n"
			}
		}

		if ignoreUntil == "" && curr == '\\' {
			i++
			if i >= len(stmt) {
				logrus.Errorln("Invalid SQL statement ends with '\\'")
				w.WriteByte(curr)
				break
			}
			switch stmt[i] {
			case 'n':
				w.WriteByte('\n')
			case 't':
				w.WriteByte('\t')
			case 'r':
				w.WriteByte('\r')
			default:
				w.WriteByte('\\')
				w.WriteByte(stmt[i])
			}
		} else {
			w.WriteByte(curr)
		}
	}

	return w.String()
}

func (*AuditLogScanner) validateSQL(queryId, stmt string) error {
	p := parser.NewParser(queryId, stmt)
	_, err := p.Parse()
	return err
}

// Which length is larger than audit_plugin_max_sql_length.
func logStmtTruncated(queryId, stmt string) bool {
	truncated := strings.HasSuffix(stmt, "...") || (strings.HasSuffix(stmt, "*/") && strings.LastIndex(stmt, "... /*") > -1)
	if truncated {
		logrus.Warningln("query has been truncated, query_id:", queryId)
	}
	return truncated
}
