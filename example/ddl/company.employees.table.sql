CREATE TABLE `employees` (
  `employee_id` int NULL,
  `department_id` int NULL,
  `salary` decimal(10,2) NULL,
  `hire_date` date NULL
) ENGINE=OLAP
DUPLICATE KEY(`employee_id`, `department_id`, `salary`)
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
