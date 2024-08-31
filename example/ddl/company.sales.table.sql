CREATE TABLE `sales` (
  `sale_id` int NULL,
  `product_id` int NULL,
  `employee_id` int NULL,
  `sale_amount` decimal(10,2) NULL,
  `sale_date` date NULL
) ENGINE=OLAP
DUPLICATE KEY(`sale_id`, `product_id`, `employee_id`)
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
