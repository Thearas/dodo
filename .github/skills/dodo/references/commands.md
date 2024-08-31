# Dodo Command Reference

This document is the canonical quick reference for answering Dodo CLI usage questions.

## Global Flags

Common flags shared by many commands:

- `--host`, `--port`, `--user`, `--password`
- `--http-port` for HTTP import workflows, default `8030`
- `--dbs`, `--tables`
- `--output` for output root, default `./output/`
- `--parallel`
- `--config` for YAML config file, usually `$HOME/.dodo.yaml`
- `--log-level`

Common defaults:

- Doris SQL port default: `9030`
- Output root default: `./output/`
- Dodo internal data dir default: `./.dodo/`

## Output Layout

- Dumped DDL and stats: `output/ddl/`
- Dumped SQL workload: `output/sql/`
- Replay result directory: `output/replay/`
- Generated data: `output/gendata/<db>.<table>/`

## Dump

Use `dodo dump` for schema dump and audit-log SQL extraction.

### Dump schema

```bash
dodo dump --dump-schema --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

Useful flags:

- `--dump-schema`
- `--dbs`, `--tables`
- `--analyze` to run `ANALYZE TABLE ... WITH SYNC` before dumping stats
- `--clean` to clear previous dump output first

### Dump query from audit log table

```bash
dodo dump --dump-query --audit-log-table __internal_schema.audit_log \
  --from '2026-03-27 10:00:00' --to '2026-03-27 12:00:00' \
  --host <host> --port <port> --user <user> --password '<password>'
```

### Dump query from audit log files

```bash
dodo dump --dump-query \
  --audit-logs 'fe.audit.log,fe.audit.log.20260327*' \
  --from '2026-03-27 10:00:00' --to '2026-03-27 12:00:00'
```

Useful query flags:

- `--dump-query`
- `--audit-log-table <db.table>`
- `--audit-logs '<file1,file2,...>'`
- `--from`, `--to`
- `--query-min-duration 500ms`
- `--query-states ok,eof,err`
- `--only-select=false` to include non-`SELECT` statements
- `--strict` to filter out SQL that cannot be parsed
- `--audit-log-encoding auto|utf8|gbk|...`
- `--anonymize`
- `--ssh-address`, `--ssh-password`, `--ssh-private-key` when audit logs are fetched over SSH

Key behavior:

- If dumping from files, the generated SQL files are written under `output/sql/` as `q0.sql`, `q1.sql`, and so on.
- If dumping from an audit log table, all queries are written to `output/sql/q0.sql`.
- Dumping queries without `--dump-schema` does not require `--dbs` or `--tables`.

## Create

Use `dodo create` to create dumped DDL objects in another Doris cluster.

```bash
dodo create --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

Or create from a custom DDL directory:

```bash
dodo create --dbs db1 --ddl ./ddl/
```

## Generate Data

Use `dodo gendata` to generate CSV or insert data from table DDL.

### Generate from a DDL file

```bash
dodo gendata --ddl table.sql --rows 10000
```

### Generate insert statements instead of CSV

```bash
dodo gendata --ddl table.sql --output-format insert
```

### Generate for dumped schemas or live databases

```bash
dodo gendata --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

### Generate with config

```bash
dodo gendata --dbs db1 --genconf example/gendata.yaml
```

Useful flags:

- `--ddl`
- `--rows`
- `--rows-per-file`
- `--genconf`
- `--output-format csv|insert`
- `--output-data-dir`
- `--print`
- `--llm`, `--key`, `--query` for AI-assisted generation

## Import

Use `dodo import` to import generated CSV or custom CSV into Doris via Stream Load.

```bash
dodo import --dbs db1,db2 --host <host> --http-port <http-port> --user <user> --password '<password>'
```

Import selected tables:

```bash
dodo import --dbs db1 --tables t1,t2 --http-port <http-port>
```

Import any CSV file into one table:

```bash
dodo import --tables db1.t1 --data data.csv -s ',' --http-port <http-port>
```

## Replay

Use `dodo replay` to replay dumped or custom SQL files.

```bash
dodo replay --host <host> --port <port> --user <user> --password '<password>' -f output/sql/q0.sql
```

Replay with concurrency:

```bash
dodo replay --db db1 -f query.sql --client-count 5
```

Replay with filters and custom result directory:

```bash
dodo replay -f output/sql/q0.sql \
  --from '2026-03-27 10:00:00' --to '2026-03-27 12:00:00' \
  --users 'readonly,root' --dbs 'db1,db2' \
  --speed 0.5 \
  --result-dir output/replay \
  --clean
```

Useful flags:

- `-f`, `--file`
- `--client-count`
- `--from`, `--to`
- `--users`
- `--dbs`
- `--speed`
- `--result-dir`
- `--clean`
- `--max-hash-rows` for diff-oriented replay result hashing

## Diff

Use `dodo diff` to compare replay results.

Compare replay result against original SQL durations:

```bash
dodo diff --min-duration-diff 200ms --original-sqls 'output/sql/*.sql' output/replay
```

Compare two replay result directories:

```bash
dodo diff replay1/ replay2/
```

## Top SQL

Use `dodo topsql` to find frequent and complex SQL.

```bash
dodo topsql -f 'output/sql/*.sql' --top 50
```

## Anonymize

Use `dodo anonymize` to anonymize SQL directly, or `dodo dump --anonymize` while dumping.

```bash
echo "select * from table1" | dodo anonymize -f -
```

```bash
dodo dump --dump-query --audit-logs 'fe.audit.log' --anonymize
```

## Cluster Deploy

Use `dodo cluster deploy` for classic Doris deployment and `dodo cluster deploy-cloud` for storage-compute separated deployment.

```bash
dodo cluster deploy ./local-doris-package.tar.gz
```

```bash
dodo cluster deploy https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.0.1-bin-x64.tar.gz \
  --fe 172.20.48.1 --be 172.20.48.1 \
  --ssh-password '<password>'
```

## Config And Automation

- Config file: `$HOME/.dodo.yaml` or `--config <file>`
- Environment variables use `DODO_` prefix, for example `DODO_HOST`, `DODO_PORT`, `DODO_USER`, `DODO_PASSWORD`
- For automation without interactive confirmation, use `DORIS_YES=1`

## Answering Guidance

- When the user asks for a command, return the shortest valid command that satisfies the request.
- When the user mixes concepts, clarify the command boundary:
  - audit-log SQL extraction -> `dodo dump --dump-query`
- If the user asks for a workflow, provide the minimal sequence of subcommands, not unrelated features.
