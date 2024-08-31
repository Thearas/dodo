---
name: dodo
description: 'Answer questions about Dodo CLI for Doris. Use when a user asks for exact dodo command lines, dump/create/gendata/import/replay/diff/topsql/anonymize/cluster workflows, audit log SQL extraction, schema migration, fake data generation, Stream Load import, config or environment-variable mapping, automation, batch replay, multi-FE replay, or user-case bug reproduction. 中文触发词: dodo 用法, dodo 命令, 审计日志导出, 审计日志导出回放, 回放, 造数, 导入, sql 脱敏, 配置文件, 环境变量, 自动化, 多 FE 回放, 批量回放, 部署集群.'
argument-hint: 'Describe the dodo task, desired workflow, source type (audit log table/file/ddl/sql), and any host/db/table/time-range/output details.'
---

# Dodo CLI

Use this skill when the user wants concrete Dodo command lines, needs help choosing the right dodo subcommand, or asks how to complete a Doris workflow with dodo.

Prefer exact commands over long explanation.

Ground answers in real Dodo commands and defaults from this repository. Do not invent subcommands or flags.

## Install

Download from GitHub Releases (you can use `gh release download`):
- https://github.com/Thearas/dodo/releases/latest

Build locally if needed:

```sh
make build
make install
```

## What To Return

- Return runnable `dodo` commands first.
- Use placeholders only for values the user did not provide.
- Mention the expected output path when it matters, such as `output/sql/`, `output/ddl/`, `output/replay/`, or `output/gendata/<db>.<table>/`.
- Briefly state required missing inputs if a command cannot be made fully concrete.
- If the workflow naturally continues, mention the next subcommand after the main command.
- Prefer the shortest valid command that satisfies the request.
- If the user asks for a workflow, return the minimal sequence of subcommands instead of unrelated feature lists.

## Working Assumptions

- Global defaults commonly used by Dodo:
	- Doris SQL port: `9030`
	- FE HTTP port: `8030`
	- Output root: `./output/`
	- Dodo self data dir: `./.dodo/`
- Common output layout:
	- dumped DDL and stats: `output/ddl/`
	- dumped SQL workload: `output/sql/`
	- replay results: `output/replay/`
	- generated data: `output/gendata/<db>.<table>/`
- Config priority from high to low:
	1. CLI flags
	2. `DODO_*` environment variables
	3. `--config <file>`
	4. default config file `~/.dodo.yaml`
- For non-interactive automation, suggest `DODO_YES=1`.

## Intent Mapping

- Audit log SQL extraction, dump query, 导出审计日志 SQL: `dodo dump --dump-query`
- Schema dump: `dodo dump --dump-schema`
- Create dumped schemas on another Doris: `dodo create`
- Generate fake data: `dodo gendata`
- Import generated CSV or custom CSV: `dodo import`
- Replay dumped or custom SQL: `dodo replay`
- Compare replay results: `dodo diff`
- Find hot or complex SQL: `dodo topsql`
- Anonymize SQL text: `dodo anonymize` or `dodo dump --anonymize`
- Deploy Doris test clusters: `dodo cluster deploy` or `dodo cluster deploy-cloud`
- Explain config/env mapping: `dodo --help` semantics with `DODO_*` and `--config`

## Canonical Workflows

- No data generation: `dump -> replay -> diff`
- Need data generation: `dump schema -> create -> gendata -> import -> replay -> diff`
- Schema only migration: `dump --dump-schema -> create`
- Audit SQL replay from production logs: `dump --dump-query -> replay -> diff`
- SQL anonymization before sharing: `anonymize` or `dump --anonymize`

## Important Distinctions

- If the user says "export audit log SQL" or "导出审计日志 SQL", map that to `dodo dump --dump-query`, not `dodo export`.
- `dodo dump --dump-query` only dumps `SELECT` by default. Add `--only-select=false` when the user wants all statement types.
- `dodo replay` replays SQL files; `dodo diff` compares replay results; do not merge them into one command.
- `dodo gendata` generates fake data from DDL or dumped schema; `dodo import` pushes generated or custom CSV into Doris via Stream Load.
- `dodo anonymize` transforms SQL text directly; `dodo dump --anonymize` anonymizes while extracting.

## Command Playbook

### Dump schema

Use when the user wants `CREATE TABLE`/view definitions or statistics.

```bash
dodo dump --dump-schema --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

Useful flags:

- `--dbs`, `--tables`
- `--analyze` to refresh stats before dumping
- `--clean` to clear previous dump output first

Expected output: `output/ddl/`

### Dump query from audit log table

Use when the source is Doris audit log table, usually `__internal_schema.audit_log`.

```bash
dodo dump --dump-query --audit-log-table __internal_schema.audit_log \
	--from '2026-03-27 10:00:00' --to '2026-03-27 12:00:00' \
	--host <host> --port <port> --user <user> --password '<password>'
```

Key behavior:

- All dumped SQL goes to `output/sql/q0.sql`
- Add `--only-select=false` for non-`SELECT` statements
- Add `--strict` if the user wants syntax filtering

### Dump query from audit log files

Use when the source is FE audit log files.

```bash
dodo dump --dump-query \
	--audit-logs 'fe.audit.log,fe.audit.log.20260327*' \
	--from '2026-03-27 10:00:00' --to '2026-03-27 12:00:00'
```

Key behavior:

- Quote glob patterns in `--audit-logs`
- Generated files are `output/sql/q0.sql`, `q1.sql`, and so on
- If logs are remote, the user may also need SSH-related flags

### Create dumped schema

Use after schema dump when creating objects on another Doris cluster.

```bash
dodo create --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

Or from a custom DDL directory:

```bash
dodo create --dbs db1 --ddl ./ddl/
```

### Generate fake data

Generate from a DDL file:

```bash
dodo gendata --ddl table.sql --rows 10000
```

Generate insert statements instead of CSV:

```bash
dodo gendata --ddl table.sql --output-format insert
```

Generate for dumped schema or live databases:

```bash
dodo gendata --dbs db1,db2 --host <host> --port <port> --user <user> --password '<password>'
```

Generate with config rules:

```bash
dodo gendata --dbs db1 --genconf example/gendata.yaml
```

AI-assisted generation:

```bash
dodo gendata -l 'deepseek-v4-pro' -k '<api-key>' --ddl table.sql --query 'select xxx'
```

Key behavior:

- If local DDL is missing, `dodo gendata` can dump schema automatically
- Default output is under `output/gendata/<db>.<table>/`

### Import generated or custom CSV

Import generated data:

```bash
dodo import --dbs db1,db2 --host <host> --http-port <http-port> --user <user> --password '<password>'
```

Import selected tables:

```bash
dodo import --dbs db1 --tables t1,t2 --http-port <http-port>
```

Import arbitrary CSV files:

```bash
dodo import --tables db1.t1 --data 'my_table/*.csv' -s ',' --http-port <http-port>
```

Key behavior:

- This uses Stream Load under the hood
- If debugging import failures, suggest `-Ldebug` to show the exact `curl` command

### Replay SQL

Replay dumped SQL:

```bash
dodo replay --host <host> --port <port> --user <user> --password '<password>' -f output/sql/q0.sql
```

Replay any SQL file with concurrency:

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

Key behavior:

- Default result directory is `output/replay`
- `--speed` adjusts interval scaling between serial SQLs of the same client
- `--client-count` resets concurrency and may change replay fidelity
- `--speed 999999 --client-count 50` is the quick pattern for no-interval high-concurrency replay when SQLs are independent

### Diff replay results

Compare replay results against original SQL duration:

```bash
dodo diff --min-duration-diff 200ms --original-sqls 'output/sql/*.sql' output/replay
```

Compare two replay result directories:

```bash
dodo diff replay1/ replay2/
```


### Top SQL

```bash
dodo topsql -f 'output/sql/*.sql' --top 50
```

### Anonymize SQL

From stdin:

```bash
echo "select * from table1" | dodo anonymize -f -
```

During dump:

```bash
dodo dump --dump-query --audit-logs 'fe.audit.log' --anonymize
```

Key behavior:

- Anonymization is currently case-insensitive
- For stable `minihash` output across runs, preserve `./dodo_hashdict.yaml` or pass `--anonymize-minihash-dict`
- Relevant flags: `--anonymize-reserve-ids`, `--anonymize-id-min-length`, `--anonymize-method`, `--anonymize-minihash-dict`

### Deploy Doris cluster

Classic deploy:

```bash
dodo cluster deploy ./local-doris-package.tar.gz
```

Remote package deploy:

```bash
dodo cluster deploy https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.0.1-bin-x64.tar.gz \
	--fe 172.20.48.1 --be 172.20.48.1 \
	--ssh-password '<password>'
```

Cloud mode deploy:

```bash
dodo cluster deploy-cloud ./doris-package.tar.gz
```


## Scenario Shortcuts

- "导出两小时审计日志 SQL": use `dodo dump --dump-query` with explicit `--from` and `--to`
- "只导某个库的表结构": use `dodo dump --dump-schema --dbs <db>`
- "把 dump 出来的 schema 建到另一套 Doris": use `dodo create`
- "根据 dump 的 SQL 回放": use `dodo replay -f output/sql/q0.sql`
- "对比回放结果": use `dodo diff`
- "生成并导入测试数据": use `dodo gendata` then `dodo import`
- "查热点 SQL": use `dodo topsql`
- "SQL 脱敏": use `dodo anonymize` or `dodo dump --anonymize`

## Special Cases To Handle

- If the source of audit SQL is unclear, offer both forms:
	- audit log table: `--audit-log-table <db.table> --from ... --to ...`
	- audit log files: `--audit-logs 'fe.audit.log,fe.audit.log.20240802*'`
- If the user asks for all statement types, add `--only-select=false`
- If the user asks for batch replay of a very large time range, prefer hourly or other chunked dump/replay examples
- If the user asks about multi-FE replay, explain that each FE audit log should be dumped and replayed separately and simultaneously
- If the user asks for automation or scripts, mention `DODO_YES=1`
- If the user asks how parameters can be passed without long command lines, mention `DODO_*` env vars and `--config <file>`
- If the user asks how to inspect replay slowness, mention searching `output/replay` directly, for example with `rg '"durationMs":\d{4}' output/replay`
- If the user asks about AI-based reproduction of user cases, point them to `example/usercase/` structure and the `gemini -iyp` workflow from `introduction.md`

## Response Rules

- Do not invent flags.
- Prefer `--dbs` and `--tables`.
- Quote glob patterns and time strings.
- When automation might prompt, suggest `DODO_YES=1`.
- If the user gives a relative time window such as "last 2 hours", convert it to explicit `--from` and `--to` values when the timestamps are known in context. Otherwise, provide a template and state that exact timestamps are needed.
- When the source is unclear, offer both forms:
	- audit log table: `--audit-log-table <db.table> --from ... --to ...`
	- audit log files: `--audit-logs 'fe.audit.log,fe.audit.log.20240802*'`
- Mention default output paths and next-step commands when they are operationally important.
- Keep explanations short unless the user explicitly asks for deeper details such as generation rules, replay semantics, or anonymization parameters.

## References

Read [command reference](./references/commands.md) for canonical subcommands, flags, defaults, and output locations.

Read [usage cookbook](./references/cookbook.md) for common Chinese user intents and ready-to-adapt command examples.
