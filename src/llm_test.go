package src

import (
	"context"
	"os"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// go test -v -timeout 600s -run ^TestLLMGendataConfig$ github.com/Thearas/dodo/src
func TestLLMGendataConfig(t *testing.T) {
	apikey := os.Getenv("DORIS_DEEPSEEK_API_KEY")
	if apikey == "" {
		t.SkipNow()
		return
	}
	type args struct {
		ctx         context.Context
		apiKey      string
		baseURL     string
		tables      []string
		columnStats []string
		sqls        []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				ctx:    context.Background(),
				apiKey: apikey,
				tables: []string{`
CREATE TABLE c (
    wa varchar(255) NOT NULL COMMENT '********',
    va int(11) NOT NULL COMMENT '********'
) ENGINE = OLAP UNIQUE KEY(wa) COMMENT "****" DISTRIBUTED BY HASH(wa) BUCKETS 4 PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
CREATE TABLE d (
    f datetime NULL COMMENT "",
    g varchar(50) NULL DEFAULT "-" COMMENT '************',
    h varchar(50) NULL DEFAULT "-" COMMENT '************',
    i varchar(255) NULL DEFAULT "-" COMMENT '****************',
    j varchar(255) NULL DEFAULT "-" COMMENT '**************',
    k varchar(50) NULL DEFAULT "-" COMMENT '***********',
    l varchar(50) NULL DEFAULT "-" COMMENT '***********',
    m varchar(50) NULL DEFAULT "-" COMMENT '********',
    xa bigint(20) NULL COMMENT "",
    ya date NULL COMMENT ""
) ENGINE = OLAP DUPLICATE KEY(f, g, h, i, j) COMMENT "********" DISTRIBUTED BY HASH(g, h) BUCKETS 4 PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "colocate_with" = "gp_group"
);
CREATE TABLE n (
    ya date NULL COMMENT "",
    g varchar(500) NULL COMMENT "",
    h varchar(500) NULL COMMENT "",
    f datetime NULL COMMENT "",
    xa bigint(20) NULL COMMENT "",
    o int(11) NULL COMMENT "",
    l varchar(500) NULL COMMENT "",
    k varchar(500) NULL COMMENT "",
    j varchar(500) NULL COMMENT "",
    p varchar(500) NULL COMMENT "",
    q varchar(500) NULL COMMENT "",
    r varchar(500) NULL COMMENT "",
    s varchar(500) NULL COMMENT "",
    t varchar(500) NULL COMMENT "",
    u varchar(500) NULL COMMENT "",
    v varchar(500) NULL COMMENT "",
    w varchar(500) NULL COMMENT "",
    x varchar(500) NULL COMMENT "",
    y varchar(500) NULL COMMENT "",
    z varchar(500) NULL COMMENT "",
    aa varchar(500) NULL COMMENT "",
    ba varchar(500) NULL COMMENT "",
    ca varchar(500) NULL COMMENT "",
    da varchar(500) NULL COMMENT "",
    ea varchar(500) NULL COMMENT "",
    fa varchar(500) NULL COMMENT ""
) ENGINE = OLAP DUPLICATE KEY(ya, g, h) COMMENT "****" DISTRIBUTED BY HASH(g, h) BUCKETS 4 PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
CREATE TABLE ga (
    ya date NULL COMMENT "",
    g varchar(500) NULL COMMENT "",
    h varchar(500) NULL COMMENT "",
    f datetime NULL COMMENT "",
    xa bigint(20) NULL COMMENT "",
    o int(11) NULL COMMENT "",
    l varchar(500) NULL COMMENT "",
    k varchar(500) NULL COMMENT "",
    j varchar(500) NULL COMMENT "",
    p varchar(500) NULL COMMENT "",
    q varchar(500) NULL COMMENT "",
    r varchar(500) NULL COMMENT "",
    s varchar(500) NULL COMMENT "",
    t varchar(500) NULL COMMENT "",
    u varchar(500) NULL COMMENT "",
    v varchar(500) NULL COMMENT "",
    w varchar(500) NULL COMMENT "",
    x varchar(500) NULL COMMENT "",
    y varchar(500) NULL COMMENT "",
    z varchar(500) NULL COMMENT "",
    aa varchar(500) NULL COMMENT "",
    ba varchar(500) NULL COMMENT "",
    ca varchar(500) NULL COMMENT "",
    da varchar(500) NULL COMMENT "",
    ea varchar(500) NULL COMMENT "",
    fa varchar(500) NULL COMMENT ""
) ENGINE = OLAP DUPLICATE KEY(ya, g, h) COMMENT "****" DISTRIBUTED BY HASH(g, h) BUCKETS 4 PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);`,
				},
				columnStats: []string{},
				sqls: []string{`
SELECT 
    ha.ia,
    ha.ja as value
from (
        select ma.ia ia,
            (
                row_number() over (
                    order by ma.ia
                )
            ) as ja
        from c oa
            right join (
                select distinct CONCAT_WS('_', l, k, j, xa) ia
                from d
                where g = 'kkp'
                    and h = 'abc'
                    and f BETWEEN convert_tz(
                        '2020-05-25 00:00:00',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    ) and convert_tz(
                        '2020-05-25 23:59:59',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    )
                    and ya BETWEEN date_sub('2020-05-25', INTERVAL 1 DAY)
                    and date_add('2020-05-25', INTERVAL 1 DAY)
                union all
                select distinct concat('-', CONCAT_WS('_', l, k, j)) ia
                from n
                where g = 'kkp'
                    and h = 'abc'
                    and f BETWEEN convert_tz(
                        '2020-05-25 00:00:00',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    ) and convert_tz(
                        '2020-05-25 23:59:59',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    )
                    and ya BETWEEN date_sub('2020-05-25', INTERVAL 1 DAY)
                    and date_add('2020-05-25', INTERVAL 1 DAY)
                union all
                select distinct concat('-', CONCAT_WS('_', l, k, j)) ia
                from ga
                where g = 'kkp'
                    and h = 'abc'
                    and f BETWEEN convert_tz(
                        '2020-05-25 00:00:00',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    ) and convert_tz(
                        '2020-05-25 23:59:59',
                        'Asia/Shanghai',
                        'Asia/Shanghai'
                    )
                    and ya BETWEEN date_sub('2020-05-25', INTERVAL 1 DAY)
                    and date_add('2020-05-25', INTERVAL 1 DAY)
            ) ma on ma.ia = oa.wa
        where oa.wa is not null
    ) ha
order by 1,2;
				`},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LLMGendataConfig(tt.args.ctx, tt.args.apiKey, tt.args.baseURL, "deepseek-chat", "每张事实表 1w 行，维度表 100 行", tt.args.tables, tt.args.columnStats, tt.args.sqls)
			if (err != nil) != tt.wantErr {
				t.Errorf("LLMGendataConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var gotData map[string]any
			assert.NoError(t, yaml.Unmarshal([]byte(got), &gotData))
			assert.NotNil(t, gotData["tables"])
			assert.IsType(t, []any{}, gotData["tables"])
			assert.True(t, lo.ContainsBy(gotData["tables"].([]any), func(item any) bool {
				table := item.(map[string]any)
				return table["name"] == "c" && table["row_count"].(int) >= 100
			}))
		})
	}
}
