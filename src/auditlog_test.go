package src

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

type sqlWriter struct {
	sqls []string
}

func (w *sqlWriter) WriteSql(s string) error {
	w.sqls = append(w.sqls, s)
	return nil
}

func (w *sqlWriter) Close() error {
	return nil
}

func TestExtractQueriesFromAuditLogs(t *testing.T) {
	chroot()
	disableLog()
	t.Parallel()

	type args struct {
		dbs               []string
		auditlogPaths     []string
		encoding          string
		queryMinCpuTimeMs int64
		queryStates       []string
		parallel          int
		unescape          bool
		onlySelect        bool
		strict            bool
		from, to          string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "default",
			args: args{
				auditlogPaths:     []string{"fixture/fe.audit.log"},
				encoding:          "auto",
				queryMinCpuTimeMs: 8,
				unescape:          true,
				onlySelect:        true,
				strict:            true,
			},
			want: 9,
		},
		{
			name: "not_only_select",
			args: args{
				auditlogPaths: []string{"fixture/fe.audit.log"},
				encoding:      "auto",
				unescape:      true,
				onlySelect:    false,
				strict:        true,
			},
			want: 10,
		},
		{
			name: "from_to",
			args: args{
				auditlogPaths: []string{"fixture/fe.audit.log"},
				encoding:      "auto",
				unescape:      true,
				onlySelect:    false,
				strict:        true,
				from:          "2024-08-06 23:44:11",
				to:            "2024-08-06 23:44:12",
			},
			want: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := AuditLogScanOpts{
				Catalog:            "",
				DBs:                tt.args.dbs,
				QueryMinDurationMs: tt.args.queryMinCpuTimeMs,
				QueryStates:        tt.args.queryStates,
				OnlySelect:         tt.args.onlySelect,
				From:               tt.args.from,
				To:                 tt.args.to,
				Strict:             tt.args.strict,
			}
			writers := lo.RepeatBy(len(tt.args.auditlogPaths), func(index int) SqlWriter { return &sqlWriter{} })
			gotCount, err := ExtractQueriesFromAuditLogs(context.Background(), writers, tt.args.auditlogPaths, tt.args.encoding, opts, tt.args.parallel)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractQueriesFromAuditLogs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for _, sql := range writers[0].(*sqlWriter).sqls {
				assert.Contains(t, sql, `"user":"root"`)
				assert.True(t, strings.Contains(sql, `"db":"mydb"`) || strings.Contains(sql, `"db":"mydb2"`))
			}
			if !reflect.DeepEqual(gotCount, tt.want) {
				t.Error(strings.Join(writers[0].(*sqlWriter).sqls, "\n---\n"))
				t.Errorf("ExtractQueriesFromAuditLogs() = %v, want %v", gotCount, tt.want)
			}
		})
	}
}

func TestAuditLogScanner_ScanOne(t *testing.T) {
	disableLog()
	t.Parallel()

	tests := []struct {
		name        string
		opts        AuditLogScanOpts
		log         string
		wantSqlCnt  int
		wantContain string // expected content in the output SQL
	}{
		{
			name:        "basic_select_query",
			opts:        AuditLogScanOpts{},
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b35|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users -- comment |gyi|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT * FROM users -- comment |gyi",
		},
		{
			name:       "filter_by_db",
			opts:       AuditLogScanOpts{DBs: []string{"otherdb"}},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b36|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:        "filter_by_db_match",
			opts:        AuditLogScanOpts{DBs: []string{"mydb"}},
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b37|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT * FROM users",
		},
		{
			name:        "filter_by_catalog_match",
			opts:        AuditLogScanOpts{Catalog: "internal"},
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b37a|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT * FROM users",
		},
		{
			name:       "filter_by_catalog_mismatch",
			opts:       AuditLogScanOpts{Catalog: "other_catalog"},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b37b|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:        "missing_catalog_field_is_compatible",
			opts:        AuditLogScanOpts{Catalog: "internal"},
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b37c|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT 1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT 1",
		},
		{
			name:       "filter_only_select",
			opts:       AuditLogScanOpts{OnlySelect: true},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b38|IsQuery=false|isNereids=true|feIp=172.20.48.119|StmtType=INSERT|Stmt=INSERT INTO users VALUES (1)|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_by_query_state",
			opts:       AuditLogScanOpts{QueryStates: []string{"ERR"}},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b39|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_by_min_duration",
			opts:       AuditLogScanOpts{QueryMinDurationMs: 100},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b40|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name: "stmt_with_pipe_in_value",
			opts: AuditLogScanOpts{},
			// Note: Pipe character in SQL values may be partially parsed since pipe is used as delimiter
			// The stmt gets truncated at the pipe character, which is expected behavior
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b41|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT '|' as pipe FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT '",
		},
		{
			name:        "old_doris_without_timestamp",
			opts:        AuditLogScanOpts{},
			log:         `2024-08-06 23:44:11,041 [query] |Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b42|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0|Stmt=SELECT 1`,
			wantSqlCnt:  1,
			wantContain: "SELECT 1",
		},
		{
			name:       "filter_explain_stmt",
			opts:       AuditLogScanOpts{OnlySelect: false},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b43|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=EXPLAIN SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_truncated_stmt",
			opts:       AuditLogScanOpts{},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b44|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM very_long_table_name...|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_by_time_range_from",
			opts:       AuditLogScanOpts{From: "2024-08-07 00:00:00"},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b45|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_by_time_range_to",
			opts:       AuditLogScanOpts{To: "2024-08-06 00:00:00"},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b46|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "empty_stmt",
			opts:       AuditLogScanOpts{},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b47|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name: "multiline_stmt",
			opts: AuditLogScanOpts{},
			log: `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b48|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT *
FROM users
WHERE id = 1|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "SELECT *\nFROM users\nWHERE id = 1",
		},
		{
			name:       "filter_show_stmt",
			opts:       AuditLogScanOpts{OnlySelect: false},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b49|IsQuery=false|isNereids=true|feIp=172.20.48.119|StmtType=SHOW|Stmt=SHOW databases|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:       "filter_use_stmt",
			opts:       AuditLogScanOpts{OnlySelect: false},
			log:        `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b50|IsQuery=false|isNereids=true|feIp=172.20.48.119|StmtType=USE|Stmt=USE mydb|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt: 0,
		},
		{
			name:        "insert_stmt_allowed",
			opts:        AuditLogScanOpts{OnlySelect: false},
			log:         `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=8cb2e4f433e74463-a0ededde7b648b51|IsQuery=false|isNereids=true|feIp=172.20.48.119|StmtType=INSERT|Stmt=INSERT INTO users VALUES (1, 'test')|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`,
			wantSqlCnt:  1,
			wantContain: "INSERT INTO users VALUES (1, 'test')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewAuditLogScanner(tt.opts)
			err := s.ScanOne([]byte(tt.log))
			assert.NoError(t, err)
			assert.Equal(t, tt.wantSqlCnt, len(s.sqls), "sql count mismatch")
			if tt.wantContain != "" && len(s.sqls) > 0 {
				assert.Contains(t, s.sqls[0], tt.wantContain)
			}
		})
	}
}

func TestAuditLogScanner_ScanOne_DuplicateQueryId(t *testing.T) {
	disableLog()
	t.Parallel()

	s := NewAuditLogScanner(AuditLogScanOpts{})

	// First scan should succeed
	log1 := `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=duplicate-query-id-1234|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM users|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`
	err := s.ScanOne([]byte(log1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(s.sqls))

	// Second scan with same query id should be filtered
	log2 := `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195809|QueryId=duplicate-query-id-1234|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT * FROM orders|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`
	err = s.ScanOne([]byte(log2))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(s.sqls)) // Still 1, duplicate filtered
}

func TestAuditLogScanner_Consume(t *testing.T) {
	disableLog()
	t.Parallel()

	s := NewAuditLogScanner(AuditLogScanOpts{})

	log := `2024-08-06 23:44:11,041 [query] |Timestamp=2024-08-06 23:44:11.041|Client=192.168.48.119:51970|User=root|Ctl=internal|Db=mydb|State=OK|ErrorCode=0|ErrorMessage=|Time(ms)=9|ScanBytes=204800|ScanRows=30|ReturnRows=0|StmtId=30195808|QueryId=consume-test-query-id|IsQuery=true|isNereids=true|feIp=172.20.48.119|StmtType=SELECT|Stmt=SELECT 1|CpuTimeMS=1|ShuffleSendBytes=0|ShuffleSendRows=0|SqlHash=null|peakMemoryBytes=968640|SqlDigest=|cloudClusterName=UNKNOWN|TraceId=|WorkloadGroup=normal|FuzzyVariables=|scanBytesFromLocalStorage=0|scanBytesFromRemoteStorage=0`
	err := s.ScanOne([]byte(log))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(s.sqls))

	w := &sqlWriter{}
	count, err := s.Consume(w)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, len(w.sqls))
	assert.Equal(t, 0, len(s.sqls)) // sqls should be cleared after consume

	// Consume again should return 0
	count, err = s.Consume(w)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestSimpleAuditLogScanner_unescapeStmt(t *testing.T) {
	type fields struct {
		AuditLogScanOpts AuditLogScanOpts
	}
	type args struct {
		stmt string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "unescape",
			args: args{
				stmt: `select *\nfrom\n\tt\nwhere /*multiline\ncomment*/\n\ta = '1''\n1' and b = """sd\tad" and c = '\n' --signleline comment\n\norder by a;`,
			},
			want: `select *
from
	t
where /*multiline\ncomment*/
	a = '1''\n1' and b = """sd\tad" and c = '\n' --signleline comment

order by a;`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &AuditLogScanner{
				sqls:             make([]string, 0, 1024),
				distinctQueryIds: make(map[string]struct{}),
			}
			if got := s.unescapeStmt(tt.args.stmt); got != tt.want {
				t.Errorf("SimpleAuditLogScanner.unescapeStmt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAuditLogScanOpts_sqlConditions(t *testing.T) {
	t.Parallel()

	opts := AuditLogScanOpts{
		Catalog:            "hive",
		DBs:                []string{"db1", "db2"},
		QueryMinDurationMs: 10,
		QueryStates:        []string{"OK"},
		OnlySelect:         true,
		From:               "2024-08-06 00:00:00",
		To:                 "2024-08-07 00:00:00",
	}

	assert.Contains(t, opts.sqlConditions(true), "`catalog` = 'hive'")
	assert.NotContains(t, opts.sqlConditions(false), "`catalog` = 'hive'")
	assert.Contains(t, opts.sqlConditions(true), "db IN ('db1', 'db2')")
	assert.Contains(t, opts.sqlConditions(true), "query_time >= 10")
	assert.Contains(t, opts.sqlConditions(true), "`state` IN ('OK')")
	assert.Contains(t, opts.sqlConditions(true), "is_query = 1")
	assert.Contains(t, opts.sqlConditions(true), "`time` >= '2024-08-06 00:00:00'")
	assert.Contains(t, opts.sqlConditions(true), "`time` <= '2024-08-07 00:00:00'")
}
