package src

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/generator"
)

func init() {
	generator.Setup("", 0)
}

func TestGendata(t *testing.T) {
	sql := `CREATE TABLE all_type_nullable (
	dt_month varchar(6) NULL,
	company_code decimal(20, 1) NULL,
	json1 JSON NULL,
	jsonb1 JSONB NULL,
	variant1 VARIANT NULL,
	date1 date NULL,
	datetime1 datetime NULL,
	t_bitmap BITMAP,
	t_null_string string,
    t_null_varchar varchar(255),
    t_null_char char(10),
    t_null_decimal_precision_2 decimal(2,1),
    t_null_decimal_precision_4 decimal(4,2),
    t_null_decimal_precision_8 decimal(8,4),
    t_null_decimal_precision_17 decimal(17,8),
    t_null_decimal_precision_18 decimal(18,8),
    t_null_decimal_precision_38 decimal(38,16),
    t_str string,
    t_string string,
    t_empty_varchar varchar(255),
    t_varchar varchar(255),
    t_varchar_max_length varchar(255),
    t_char char(10),
    t_int int,
    t_bigint bigint,
    t_float float,
    t_double double,
    t_boolean_true boolean,
    t_boolean_false boolean,
    t_decimal_precision_2 decimal(2,1),
    t_decimal_precision_4 decimal(4,2),
    t_decimal_precision_8 decimal(8,4),
    t_decimal_precision_17 decimal(17,8),
    t_decimal_precision_18 decimal(18,8),
    t_decimal_precision_38 decimal(38,16),
    t_map_string map<string,string>,
    t_map_varchar map<varchar(255),varchar(255)>,
    t_map_char map<char(10),char(10)>,
    t_map_int map<int,int>,
    t_map_bigint map<bigint,bigint>,
    t_map_float map<float,float>,
    t_map_double map<double,double>,
    t_map_boolean map<boolean,boolean>,
    t_map_decimal_precision_2 map<decimal(2,1),decimal(2,1)>,
    t_map_decimal_precision_4 map<decimal(4,2),decimal(4,2)>,
    t_map_decimal_precision_8 map<decimal(8,4),decimal(8,4)>,
    t_map_decimal_precision_17 map<decimal(17,8),decimal(17,8)>,
    t_map_decimal_precision_18 map<decimal(18,8),decimal(18,8)>,
    t_map_decimal_precision_38 map<decimal(38,16),decimal(38,16)>,
    t_array_string array<string>,
    t_array_int array<int>,
    t_array_bigint array<bigint>,
    t_array_float array<float>,
    t_array_double array<double>,
    t_array_boolean array<boolean>,
    t_array_varchar array<varchar(255)>,
    t_array_char array<char(10)>,
    t_array_decimal_precision_2 array<decimal(2,1)>,
    t_array_decimal_precision_4 array<decimal(4,2)>,
    t_array_decimal_precision_8 array<decimal(8,4)>,
    t_array_decimal_precision_17 array<decimal(17,8)>,
    t_array_decimal_precision_18 array<decimal(18,8)>,
    t_array_decimal_precision_38 array<decimal(38,16)>,
    t_struct_bigint struct<s_bigint:bigint>,
    t_complex map<string,array<struct<s_int:int>>>,
    t_struct_nested struct<struct_field:array<string>>,
    t_struct_null struct<struct_field_null:string,struct_field_null2:string>,
    t_struct_non_nulls_after_nulls struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
    t_nested_struct_non_nulls_after_nulls struct<struct_field1:int,struct_field2:string,struct_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
    t_map_null_value map<string,string>,
    t_array_string_starting_with_nulls array<string>,
    t_array_string_with_nulls_in_between array<string>,
    t_array_string_ending_with_nulls array<string>,
    t_array_string_all_nulls array<string>
) ENGINE=OLAP
DUPLICATE KEY(dt_month)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(dt_month) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default:1", -- usually the BE nodes
'bloom_filter_columns' = "dt_month, company_code"
);`

	rows := 50
	tg, err := NewTableGen("create-table.sql", sql, nil, rows, nil)
	assert.NoError(t, err)
	assert.Len(t, tg.colGens, 74)

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	assert.NoError(t, tg.GenCSV(w, tg.Rows))
	assert.NoError(t, w.Flush())

	resultCSV := strings.Split(b.String(), "\n")
	assert.Len(t, resultCSV, 1+tg.Rows) // first line is columns info
	assert.True(t, strings.HasPrefix(resultCSV[0], GenDataFileFirstLinePrefix))
}
