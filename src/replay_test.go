package src

import (
	"cmp"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/Thearas/sqlsplit"

	"github.com/Thearas/dodo/src/parser"
)

func TestDecodeReplaySqls(t *testing.T) {
	t.Parallel()
	chroot()
	disableLog()

	replayFile, err := os.Open("fixture/q0.sql")
	assert.NoError(t, err)
	defer replayFile.Close()

	minTs, err := time.Parse("2006-01-02 15:04:05.000", "2024-08-06 23:44:11.041")
	assert.NoError(t, err)
	filteredMinTs, err := time.Parse("2006-01-02 15:04:05.000", "2024-08-06 23:44:12.044")
	assert.NoError(t, err)
	filteredFrom, err := time.Parse("2006-01-02 15:04:05.000", "2024-08-06 23:44:12.000")
	assert.NoError(t, err)
	filteredTo, err := time.Parse("2006-01-02 15:04:05.000", "2024-08-06 23:44:12.999")
	assert.NoError(t, err)

	type args struct {
		s           *os.File
		dbs         map[string]struct{}
		users       map[string]struct{}
		from        int64
		to          int64
		clientCount int
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]int
		want1   int64
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				s: replayFile,
			},
			want: map[string]int{
				"192.168.48.119:51970": 7,
				"192.168.48.118:51970": 5,
			},
			want1: minTs.UnixMilli(),
		},
		{
			name: "custom client",
			args: args{
				s:           replayFile,
				clientCount: 4,
			},
			want: map[string]int{
				"client1": 3,
				"client2": 3,
				"client3": 3,
				"client4": 3,
			},
			want1: minTs.UnixMilli(),
		},
		{
			name: "filtered min ts",
			args: args{
				s:    replayFile,
				from: filteredFrom.UnixMilli(),
				to:   filteredTo.UnixMilli(),
			},
			want: map[string]int{
				"192.168.48.119:51970": 1,
				"192.168.48.118:51970": 1,
			},
			want1: filteredMinTs.UnixMilli(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replayFile.Seek(0, 0)
			got, got1, _, err := DecodeReplaySqls(tt.args.s, tt.args.dbs, tt.args.users, tt.args.from, tt.args.to, tt.args.clientCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeReplaySqls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			clientSqls := lo.MapToSlice(got, func(k string, v []*ReplaySql) ClientSqls {
				return ClientSqls{Client: k, Sqls: v}
			})

			clientsqls := lo.SliceToMap(clientSqls, func(v ClientSqls) (string, int) {
				return v.Client, len(v.Sqls)
			})
			if !reflect.DeepEqual(clientsqls, tt.want) {
				t.Errorf("DecodeReplaySqls() got = %v, want %v", clientsqls, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("DecodeReplaySqls() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSplitSqls(t *testing.T) {
	t.Parallel()
	chroot()

	var i int
	replayFile, err := os.Open("fixture/q0.sql")
	testF := func() {
		assert.NoError(t, err)
		defer replayFile.Close()

		i = 0
		iter, err := sqlsplit.SplitFd(replayFile)
		assert.NoError(t, err)
		for sql, err := range iter {
			assert.NoError(t, err)
			p := parser.NewParser("test-sql", sql)
			c, err := p.Parse()
			assert.NoError(t, err)
			assert.Len(t, c.AllStatement(), 1)
			i++
		}
	}
	testF()

	replayFile, err = os.Open("fixture/replay.sql")
	testF()

	replayFile, err = os.Open("fixture/sql.sql")
	testF()
	assert.Equal(t, 4, i)

	cliCount := 2
	replayFile, err = os.Open("fixture/sql.sql")
	client2Sqls, _, total, err := decodeSqls(replayFile, "testdb", cliCount)
	assert.NoError(t, err)
	assert.Equal(t, 4, total)
	assert.Equal(t, 2, len(client2Sqls))
	cliFmt := clientNameFormat(cliCount)
	clientSqls := lo.MapToSlice(client2Sqls, func(k string, v []*ReplaySql) ClientSqls {
		return ClientSqls{Client: k, Sqls: v}
	})
	slices.SortFunc(clientSqls, func(l, r ClientSqls) int {
		return cmp.Compare(l.Client, r.Client)
	})
	i = 0
	for _, clientSql := range clientSqls {
		expectedCliName := getClientBySqlIdx(cliFmt, cliCount, "", i)
		assert.Equal(t, expectedCliName, clientSql.Client)
		assert.Equal(t, 2, len(clientSql.Sqls))
		for _, sql := range clientSql.Sqls {
			assert.Equal(t, "testdb", sql.Db)

			p := parser.NewParser("test-sql", sql.Stmt)
			c, err := p.Parse()
			assert.NoError(t, err)
			assert.Len(t, c.AllStatement(), 1)
		}
		i++
	}
}
