CREATE TABLE `all_type_nullable` (
  `dt_month` varchar(6) NULL,
  `json1` json NULL,
  `jsonb1` jsonb NULL,
  `variant1` variant NULL,
  `date1` date NULL,
  `datetime1` datetime NULL,
  `t_bitmap` bitmap NOT NULL DEFAULT BITMAP_EMPTY,
  `t_null_string` text NULL,
  `t_null_char` char(10) NULL,
  `t_null_decimal_precision_38` decimal(38,16) NULL,
  `t_str` text NULL,
  `t_string` text NULL,
  `t_varchar` varchar(255) NULL,
  `t_varchar2` varchar(255) NULL,
  `t_char` char(10) NULL,
  `t_int` int NULL,
  `t_bigint` bigint NULL,
  `t_float` float NULL,
  `t_map_varchar` map<varchar(255),varchar(255)> NULL,
  `t_array_string` array<text> NULL,
  `t_struct_nested` struct<struct_field:array<text>> NULL
) ENGINE=OLAP
DUPLICATE KEY(`dt_month`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`dt_month`) BUCKETS 10
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
