CREATE TABLE ob (
    za varchar(65533) NOT NULL COMMENT '**************',
    lb datetime NOT NULL COMMENT '**************',
    tb json NULL COMMENT '********'
) ENGINE=OLAP
DUPLICATE KEY(za, lb)
COMMENT '******'
DISTRIBUTED BY HASH(za) BUCKETS 3
PROPERTIES ("replication_allocation" = "tag.location.default: 1");