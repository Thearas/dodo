CREATE TABLE rb (
    lb varchar(200) NULL,
    za varchar(200) NULL,
    ub varchar(20) NULL,
    vb double SUM NULL,
    hb double REPLACE NULL,
    wb double SUM NULL,
    eb double REPLACE NULL,
    xb double SUM NULL,
    bb double REPLACE NULL,
    kb double SUM NULL
) ENGINE=OLAP
AGGREGATE KEY(lb, za, ub)
COMMENT '******'
DISTRIBUTED BY HASH(za) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
