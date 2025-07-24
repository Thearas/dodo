package src

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/dlclark/regexp2"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"

	"github.com/Thearas/dodo/src/parser"
)

var (
	// stmtMatchFmt is the regex pattern to extract the query statement from the audit log.
	//
	// NOTE: A bit hacky, but it works for now.
	//
	// Tested on v2.0.14+, v2.1.x and v3.0.x. Not sure if it also works on others Doris version.
	stmtMatchFmt = `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d*) \[[^\]]+\] \|Client=([^|]+)\|User=([^|]+)(?:\|Ctl=[^|]+)?\|Db=(%s)(?:\|CommandType=[^|]+)?\|State=%s\|(?:.+?)\|Time(?:\(ms\))?=(\d*)\|(?:.+?)\|QueryId=([a-z0-9-]+)\|IsQuery=%s\|(?:.+?)\|Stmt=(.+?)\|CpuTimeMS=` //nolint:all

	// filterStmtRe filters out some statements from the audit log.
	filterStmtRe = regexp.MustCompile("(?i)^(EXPLAIN|SHOW|USE)")
)

// Not thread safe.
type AuditLogScanner interface {
	Init()
	ScanOne(oneLine []byte) error
	Consume(w SqlWriter) (int, error)
	Close()
}

var _ AuditLogScanner = (*SimpleAuditLogScanner)(nil)

type AuditLogScanOpts struct {
	// filter
	DBs                []string
	QueryMinDurationMs int64
	QueryStates        []string
	OnlySelect         bool
	From, To           string

	Strict bool
}

// push down filter to db
func (opts *AuditLogScanOpts) sqlConditions() string {
	// filter out doris self-executed sqls
	conditions := " client_ip != ''"
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
		g.Go(func() error {
			f, err := os.Open(auditlogPath)
			if err != nil {
				logrus.Errorln("Unable to open audit log file:", auditlogPath)
				return err
			}
			defer f.Close()

			// detect encoding
			enc, err := detectAuditLogEncoding(encoding, f)
			if err != nil {
				return err
			}

			buf := bufio.NewScanner(transform.NewReader(f, enc.NewDecoder()))
			buf.Buffer(make([]byte, 0, 10*1024*1024), 10*1024*1024)

			logrus.Debugln("Extracting queries from audit log:", auditlogPath, "with encoding:", enc)

			// read log file line by line
			s := NewAuditLogScanner(opts)
			count, err := extractQueriesFromAuditLog(writers[i], s, buf)
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
	w SqlWriter,
	s AuditLogScanner,
	auditlog *bufio.Scanner,
) (int, error) {
	s.Init()
	defer s.Close()

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

type SimpleAuditLogScanner struct {
	AuditLogScanOpts

	sqls             []string
	distinctQueryIds map[string]struct{}
	distinctQueryTs  string

	re *regexp2.Regexp
}

func NewSimpleAuditLogScanner(opts AuditLogScanOpts) *SimpleAuditLogScanner {
	return &SimpleAuditLogScanner{
		AuditLogScanOpts: opts,
		sqls:             make([]string, 0, 1024),
		distinctQueryIds: make(map[string]struct{}),
	}
}

func (s *SimpleAuditLogScanner) Init() {
	s.re = regexp2.MustCompile(
		auditlogQueryRe(s.DBs, s.QueryStates, s.OnlySelect),
		regexp2.Multiline|regexp2.Singleline|regexp2.Unicode|regexp2.Compiled,
	)
}

func (s *SimpleAuditLogScanner) ScanOne(oneLog []byte) error {
	matches, err := s.re.FindStringMatch(string(oneLog))
	if err != nil {
		logrus.Errorln("Failed to scan audit log file")
		return err
	}
	if matches == nil || len(matches.Groups()) < 2 {
		return nil
	}

	caps := lo.Map(matches.Groups()[1:], func(g regexp2.Group, _ int) string { return g.String() })
	s.onMatch(caps, false)

	return nil
}

func (s *SimpleAuditLogScanner) Consume(w SqlWriter) (int, error) {
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

func (*SimpleAuditLogScanner) Close() {}

func (s *SimpleAuditLogScanner) onMatch(caps []string, skipOptsFilter bool) {
	time, client, user, db, durationMs, queryId, stmt := caps[0], caps[1], caps[2], caps[3], cast.ToInt64(caps[4]), caps[5], caps[6]
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

	ok := s.filterStmtFromMatch(time, queryId, stmt, durationMs, skipOptsFilter)
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
func (s *SimpleAuditLogScanner) filterStmtFromMatch(
	time, queryId, stmt string, durationMs int64,
	skipOptsFilter bool,
) bool {
	// remove empty stmt
	if len(stmt) == 0 {
		return false
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

	if skipOptsFilter {
		return true
	}

	// filter by opts below

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

	return true
}

// unescapeStmt unescapes the \\n, \\t and \\r in SQL statement.
// NOTE: It will not unescape chars in string literals, comments and multi-line comments.
func (*SimpleAuditLogScanner) unescapeStmt(stmt string) string {
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

func (*SimpleAuditLogScanner) validateSQL(queryId, stmt string) error {
	p := parser.NewParser(queryId, stmt)
	_, err := p.Parse()
	return err
}

// Which length is larger than audit_plugin_max_sql_length.
func logStmtTruncated(queryId, stmt string) bool {
	var truncated bool
	if strings.HasSuffix(stmt, "...") {
		truncated = true
	} else if strings.HasSuffix(stmt, "*/") && strings.LastIndex(stmt, "... /*") > -1 {
		truncated = true
	}
	if truncated {
		logrus.Warningln("query has been truncated, query_id:", queryId)
	}
	return truncated
}

//nolint:revive
func auditlogQueryRe(dbs, states []string, onlySelect bool) string {
	var dbFilter string
	if len(dbs) > 0 {
		allowDBs := lo.Map(dbs, func(s string, _ int) string { return regexp.QuoteMeta(s) })
		dbFilter = strings.Join(allowDBs, "|")
	} else {
		dbFilter = "[^|]*"
	}

	var stateFilter string
	if len(states) > 0 {
		allowStates := lo.Map(states, func(s string, _ int) string { return regexp.QuoteMeta(s) })
		stateFilter = strings.Join(allowStates, "|")
	} else {
		stateFilter = "[^|]*"
	}

	isQuery := "[^|]+"
	if onlySelect {
		isQuery = "true"
	}

	return fmt.Sprintf(stmtMatchFmt, dbFilter, stateFilter, isQuery)
}

func detectAuditLogEncoding(encoding string, f *os.File) (encoding.Encoding, error) {
	// detect encoding
	var err error
	if encoding == "auto" || encoding == "" {
		// detect encoding
		encoding, err = DetectCharset(bufio.NewReader(f))
		if err != nil {
			return nil, fmt.Errorf("cannot detect charset of audit log %s: %v", f.Name(), err)
		}
		if _, err = f.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("cannot seek audit log %s: %v", f.Name(), err)
		}
	}
	enc, err := GetEncoding(encoding)
	if err != nil {
		return nil, err
	}

	return enc, nil
}
