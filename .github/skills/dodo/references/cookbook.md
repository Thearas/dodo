# Dodo Usage Cookbook

This document maps common user requests to the correct Dodo commands.

## 导出审计日志 SQL

This usually means dumping SQL from Doris audit logs, not exporting table data.

### If the source is an audit log table

```bash
dodo dump --dump-query \
  --audit-log-table __internal_schema.audit_log \
  --from '2026-03-27 10:00:00' \
  --to '2026-03-27 12:00:00' \
  --host <host> --port <port> --user <user> --password '<password>'
```

If the user wants all statement types instead of only `SELECT`:

```bash
dodo dump --dump-query \
  --audit-log-table __internal_schema.audit_log \
  --from '2026-03-27 10:00:00' \
  --to '2026-03-27 12:00:00' \
  --only-select=false \
  --host <host> --port <port> --user <user> --password '<password>'
```

### If the source is audit log files

```bash
dodo dump --dump-query \
  --audit-logs 'fe.audit.log,fe.audit.log.20260327*' \
  --from '2026-03-27 10:00:00' \
  --to '2026-03-27 12:00:00'
```

Expected output: `output/sql/q0.sql`, `output/sql/q1.sql`, and so on.

## 只导出某个库的表结构

```bash
dodo dump --dump-schema --dbs db1 --host <host> --port <port> --user <user> --password '<password>'
```

Expected output: `output/ddl/`

## 把 dump 出来的 schema 创建到另一个 Doris

```bash
dodo create --dbs db1 --host <target-host> --port <target-port> --user <user> --password '<password>'
```

If DDL files are in a custom directory:

```bash
dodo create --dbs db1 --ddl ./ddl/
```

## 根据 dump 的 SQL 回放负载

```bash
dodo replay --host <host> --port <port> --user <user> --password '<password>' -f output/sql/q0.sql
```

Replay only a time window inside the dumped file:

```bash
dodo replay -f output/sql/q0.sql \
  --from '2026-03-27 10:00:00' \
  --to '2026-03-27 12:00:00' \
  --host <host> --port <port> --user <user> --password '<password>'
```

## 对比 replay 结果

```bash
dodo diff --min-duration-diff 200ms --original-sqls 'output/sql/*.sql' output/replay
```

## 生成测试数据并导入

Generate data:

```bash
dodo gendata --dbs db1 --host <host> --port <port> --user <user> --password '<password>'
```

Import generated data:

```bash
dodo import --dbs db1 --host <host> --http-port <http-port> --user <user> --password '<password>'
```

## 匿名化 SQL

From stdin:

```bash
echo "select * from table1" | dodo anonymize -f -
```

During dump:

```bash
dodo dump --dump-query --audit-logs 'fe.audit.log' --anonymize
```

## 查最热点的 SQL

```bash
dodo topsql -f 'output/sql/*.sql' --top 50
```

## 回答这类问题时的优先级

1. Identify whether the user wants SQL extraction, schema handling, data generation/import, replay analysis.
2. Pick the exact dodo subcommand that matches the request.
3. Return a runnable command with only the necessary flags.
4. Mention the output file or next step if the workflow naturally continues.
