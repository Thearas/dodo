package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModifyProperties(t *testing.T) {
	sql := `CREATE TABLE t1 (
dt_month varchar(6) NULL,
company_code varchar(40) NULL
) ENGINE=OLAP
DUPLICATE KEY(dt_month)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(dt_month) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1",
'bloom_filter_columns' = "dt_month, company_code"
);
select count(dt_month), data from t1`

	p := NewParser("1", sql, NewListener(false, func(_ string) string { return "foo" }))
	s, err := p.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `CREATE TABLE foo (
foo varchar(6) NULL,
foo varchar(40) NULL
) ENGINE=OLAP
DUPLICATE KEY(foo)
COMMENT 'OLAP'
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
