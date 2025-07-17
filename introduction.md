# Introduction

- [Workflow](#workflow)
- [Dump](#dump)
  - [Dump Tables and Views](#dump-tables-and-views)
  - [Dump Queries](#dump-queries)
  - [Other Dump Parameters](#other-dump-parameters)
- [Create Schemas](#create-schemas)
- [Generate and Import Data](#generate-and-import-data)
  - [Default Generation Rules](#default-generation-rules)
  - [Custom Generation Rules](#custom-generation-rules)
    - [Global Rules vs Table Rules](#global-rules-vs-table-rules)
    - [null_frequency](#null_frequency)
    - [min/max](#minmax)
    - [precision/scale](#precisionscale)
    - [length](#length)
    - [format](#format)
    - [gen](#gen)
      - [inc](#inc)
      - [enum](#enum)
      - [parts](#parts)
      - [ref](#ref)
      - [type](#type)
      - [golang](#golang)
    - [Complex Types](#complex-types-maparraystructjsonvariant)
  - [AI Generation](#ai-generationvia-openaideepseek)
- [Replay](#replay)
  - [Replay Speed and Concurrency](#replay-speed-and-concurrency)
  - [Other Replay Parameters](#other-replay-parameters)
- [Diff Replay Results](#diff-replay-results)
- [Best Practices](#best-practices)
  - [Command-line Prompts and Autocompletion](#command-line-prompts-and-autocompletion)
  - [Environment Variables and Configuration Files](#environment-variables-and-configuration-files)
  - [Monitoring Dump/Replay Process](#monitoring-dumpreplay-process)
  - [Multi-FE Replay](#multi-fe-replay)
  - [Large-scale Batch Replay](#large-scale-batch-replay)
  - [Find SQLs with Replay Duration Exceeding 1s](#find-sqls-with-replay-duration-exceeding-1s)
  - [Automation](#automation)
  - [AI Automatic Reproduction of User BUGs](#ai-automatic-reproduction-of-user-bugs)
- [Anonymization](#anonymization)
- [FAQ](#faq)
  - [How to provide the tool to customers, and is there any impact on the production environment?](#how-to-provide-the-tool-to-customers-and-is-there-any-impact-on-the-production-environment)
  - [The number of dump SQLs is less than in the audit log](#the-number-of-dump-sqls-is-less-than-in-the-audit-log)
  - [Dump SQL has syntax errors](#dump-sql-has-syntax-errors)
  - [Dump statistics do not match the actual ones](#dump-statistics-do-not-match-the-actual-ones)
  - [Replay error: too many connections](#replay-error-too-many-connections)

## Workflow

There are two types of workflows, with each step representing a `dodo` command:

- No data generation needed: `Dump -> Replay -> Diff Replay Results`
- Data generation needed: `Dump -> Create Schemas (Optional) -> Generate and Import Data -> Replay -> Diff Replay Results`

## Dump

`dodo dump --help`

This is divided into two parts: "Dump Tables and Views" and "Dump Queries". Both can be combined into a single `dodo` command.

### Dump Tables and Views

`dodo dump --dump-schema`

Dumps `CREATE` statements for tables and views from a Doris database. By default, it also dumps table statistics. If the statistics differ significantly from the actual data, specifying `--analyze` is recommended. See [Dump statistics do not match the actual ones](#dump-statistics-do-not-match-the-actual-ones).

```sh
# Dump all tables and views from db1 and db2
dodo dump --dump-schema --host xxx --port xxx --user xxx --password xxx --dbs db1,db2

# Default dump to output/ddl:
output
└── ddl
    ├── db1.t1.table.sql
    ├── db1.stats.yaml
    ├── db2.t2.table.sql
    └── db2.stats.yaml
```

### Dump Queries

`dodo dump --dump-query`

Queries can be dump from an audit log table or file. By default, only `SELECT` statements are dump. You can add `--only-select=false` to dump other statements as well.

```sh
# Dump from an audit log table, table name is usually __internal_schema.audit_log
dodo dump --dump-query --audit-log-table <db.table> --from '2024-11-14 17:00:00' --to '2024-11-14 18:00:00' --host xxx --port xxx --user xxx --password xxx

# Dump from audit log files, '*' matches multiple files (note the single quotes)
dodo dump --dump-query --audit-logs 'fe.audit.log,fe.audit.log.20240802*'

# Default dump to output/sql:
output
└── sql
    ├── q0.sql
    └── q1.sql
```

> [!NOTE]
>
> - When dumping from log files, `q0.sql` corresponds to the first log file, `q1.sql` to the second, and so on. However, when dumping from a log table, all queries are written to `q0.sql`.
> - Dump results are appended to previously dump SQL files unless `--clean` is specified, which deletes the previous `output/ddl` and `output/sql` directories.

### Other Dump Parameters

- `--analyze`: Automatically runs `ANALYZE TABLE <table> WITH SYNC` before dumping a table to make statistics more accurate. Default is off.
- `--parallel`: Controls the dump concurrency. Increasing it speeds up the dump; decreasing it uses fewer resources. Default is `min(machine_cores, 10)`.
- `--dump-stats`: Also dumps table statistics when dumping tables. Statistics are dump to `output/ddl/db.stats.yaml`. Default is on.
- `--only-select`: Whether to dump only `SELECT` statements. Default is on.
- `--from` and `--to`: Dump SQL within a specified time range.
- `--query-min-duration`: Minimum execution duration for dump SQL.
- `--query-states`: States of the SQL to be dump, can be `ok`, `eof`, and `err`.
- `-s, --strict`: Validates SQL syntax correctness when dumping from audit logs.
- `--audit-log-encoding`: Audit log file encoding. Default is auto-detect.
- `--anonymize`: Anonymizes data during dump, e.g., `select * from table1` becomes `select * from a`.
- `--anonymize-xxx`: Other anonymization parameters, see [Anonymization](#anonymization).

## Create Schemas

`dodo create --help`

You need to first [Dump Tables and Views](#dump-tables-and-views) locally, then create them in another Doris instance:

```sh
# Create all dump tables and views for db1 and db2
dodo create --dbs db1,db2

# Create dump table1 and table2
dodo create --dbs db1 --tables table1,table2

# Run any create table/view SQL in db1
dodo create --ddl 'dir/*.sql' --db db1
```

## Generate and Import Data

`dodo gendata --help`/`dodo import --help`

You need to first [Dump Tables and Views](#dump-tables-and-views) locally, then generate and import data:

```sh
# Generate data for all dump tables in db1 and db2
dodo gendata --dbs db1,db2

# Generate data for dump table1
dodo gendata --tables db1.table1 # or --dbs db1 --tables table1

# Data can also be generated for any create table SQL without prior dump
# P.S. It might not necessarily be Doris; other databases like Hive also work
dodo gendata --ddl 'ddl/*.sql'

# Generate data with config
dodo gendata ... --genconf gendata.yaml


# Import data for all tables with generated data in db1 and db2
dodo import --dbs db1,db2

# Import data for table1 with generated data
dodo import --tables db1.table1 # or --dbs db1 --tables table1

# Import any CSV data file(s) into table1
dodo import --tables db1.table1 --data 'my_table/*.csv'
```

In implementation, the tool performs these actions in two stages based on the `--dbs` and `--tables` parameters:

1. In the generation stage:

    1. Scans the dump directory `output/ddl/` for matching `<db>.<table>.table.sql` files. The dump directory (or specific `<basename>.sql` files) can be specified with `--ddl`.
    2. Combines the corresponding statistics file `<db>.stats.yaml` with the custom generation rules file (specified by `--genconf`) to determine the final generation rules.
    3. Generates CSV files into the data generation directory `output/gendata/<db>.<table>/` (or `output/gendata/<basename>/`) according to the generation rules.
2. In the import stage:

    1. Scans the data generation directory `output/gendata/` for matching `<db>.<table>/*` data files. The data generation directory can be specified with `--data`.
    2. Uses the `curl` command to run StreamLoad for data import.

> [!TIP]
>
> - Specifying `-Ldebug` during import shows the specific `curl` command, which is helpful for reproducing and troubleshooting issues.

### Default Generation Rules

By default, `NULL` values are not generated. This can be changed by specifying `null_frequency` in [Custom Generation Rules](#custom-generation-rules).

Default generation rules for various types:

| Type | Length | Min - Max | Structure |
| --- | --- | --- | --- |
| ARRAY | 1 - 3 |  |  |
| MAP | 1 - 3 |  |  |
| JSON/JSONB |  |  | STRUCT<col1:SMALLINT, col2:SMALLINT> |
| VARIANT |  |  | STRUCT<col1:SMALLINT, col2:SMALLINT> |
| BITMAP | 5 | element: 0 - MaxInt32 |  |
| HLL |  | hll_empty() |  |
| TEXT/STRING/VARCHAR | 1 - 10 |  |  |
| TINYINT |  | MinInt8 - MaxInt8 |  |
| SMALLINT |  | MinInt16 - MaxInt16 |  |
| INT |  | MinInt32 - MaxInt32 |  |
| BIGINT |  | MinInt32 - MaxInt32 |  |
| LARGEINT |  | MinInt32 - MaxInt32 |  |
| FLOAT |  | MinInt16 - MaxInt16 | |
| DOUBLE |  | MinInt32 - MaxInt32 |  |
| DECIMAL |  | MinInt32 - MaxInt32 |  |
| DATE |  | 10 years ago - now |  |
| DATETIME |  | 10 years ago - now |  |

- The `string` type letter is randomly generated, and the charset is alphanumeric (a-z, A-Z, 0-9)

### Custom Generation Rules

When generating data, specify the configuration file using `--genconf gendata.yaml`. For a complete example, see [example/gendata.yaml](./example/gendata.yaml).

#### Global Rules vs. Table Rules

Generation rules can be divided into global and table levels. Table-level configurations will override global configurations.

Example of global rules:

```yaml
# Global default NULL frequency
null_frequency: 0

# Global type generation rules
type:
  bigint:
    min: 0
    max: 100
  date:
    min: 1997-02-16
    max: 2025-06-12
```

Example of table-level rules:

```yaml
tables:
  - name: employees
    row_count: 100  # Optional, default is 1000 (can also be specified by --rows)
    columns:
      - name: department_id
        null_frequency: 0.1  # 10% NULL
        min: 1
        max: 10
```

#### null_frequency

Specifies the proportion of NULL values for a field, with a value range of 0-1. For example:

```yaml
null_frequency: 0.1  # 10% probability of generating NULL
```

#### min/max

Specifies the value range for numeric type fields. For example:

```yaml
columns:
  - name: salary
    min: 15000.00
    max: 16000.00
  - name: hire_date
    min: "1997-01-15"
    max: "1997-01-15"
```

#### precision/scale

Specifies the precision and scale for DECIMAL types. For example:

```yaml
columns:
  - name: t_decimal
    precision: 10
    scale: 3
    min: 100
    max: 102  # Actual maximum value is 102.999
```

#### length

Specifies the length range for bitmap, string, array or map types. For example, randomly generates a `string` in 1 - 5 length:

```yaml
columns:
  - name: t_str
    # or just `length: <int>` if min and max are the same, like `length: 5`
    length:
      min: 1
      max: 5
```

#### format

No matter what generation rule, there always can have a `format`, which will run after the column data generation, generate a string basing on the template, and then output it to CSV file. There're two types of tags (aka. placeholders) can be used in `format`:

1. Format the return value of the column, such as `{{%s}}` or `{{%d}}`, etc., with the same syntax as Go's `fmt.Sprintf()`. There can only be one such label in a `format` (except using [`parts`](#parts)).
2. Built-in tags such as `{{month}}`, `{{year}}`, etc, all built-in tags can be found in: [src/generator/README.md](./src/generator/README.md#format-tags).

For example:

```yaml
columns:
  - name: t_str
    format: 'substr length 1-5: {{%s}} and a build-in tags: {{preposition_simple}}'
    length:
      min: 1
      max: 5
```

Note: If the generator returns NULL, format will also return NULL.

#### gen

Optional custom generator, supports the following types, MUST be defined under `gen:`:

> [!IMPORTANT]
>
> - `gen:` will override the gen rules at the column level (except `null_frequency` and `format`), makes `length`, `min/max` no longer effective.
> - Only one generator can be specified at the same time, for example, if the `inc` generator is specified, the `enum` generator cannot be specified

##### inc

Auto-increment generator, can specify start value and step:

```yaml
columns:
  - name: t_string
    format: "string-inc-{{%d}}"
    # `length` won't work, override by `gen`
    # length: 10
    gen:
      inc: 2      # Step is 2 (default 1)
      start: 100  # Starts from 100 (default 0)
```

##### enum

Enum generator, randomly selects from given values, values can be literals or generation rules. There is an optional config `weights` (can only be used with `enum`):

```yaml
columns:
  - name: t_null_string
    gen:
      enum: [foo, bar, foobar]
      weights: [0.2, 0.6, 0.2]  # Optional, specifies the probability of each value being selected

  - name: t_str
    gen:
      # randomly choose one generation rule to generate value, each has 25% probability
      enum:
        - length: 5
        - length: {min: 5, max: 10}
        - format: "my name is {{username}}"
        - gen:
            enum: [1, 2, 3]
```

##### parts

Must be used together with [`format`](#format). Flexibly combine multiple values ​​to produce the final result.

`parts` generates multiple values ​​at a time and fills them into `{{%xxx}}` of [`format`](#format) in order. The value of each part can be a literal or a generation rule:

```yaml
columns:
  - name: date1 # date
    format: "{{year}}-{{%02d}}-{{%02d}}"
    gen:
      parts:
        - gen: # month
            type: int
            min: 1
            max: 12
        - gen: # day
            type: int
            min: 1
            max: 20

  - name: t_null_char # char(10)
    format: "{{%s}}--{{%02d}}" # parts must be used with format
    gen:
      parts:
        - "prefix"
        - gen:
            enum: [2, 4, 6, 8, 10]
```

##### ref

Reference generator, randomly uses values from other `table.column`.
Typically used for relational columns, like `t1 JOIN t2 ON t1.c1 = t2.c1` or `WHERE t1.c1 = t2.c1`:

```yaml
columns:
  - name: t_int
    # format: "1{{%6d}}"
    gen:
      ref: employees.department_id
      limit: 100  # Randomly select 100 values (default 1000)

  - name: t_struct # struct<dp_id:int, name:text>
    fields:
      - name: dp_id
        gen:
          ref: employees.department_id
      - name: name
        gen:
          ref: employees.name
```

> [!IMPORTANT]
>
> - The source tables that be referenced to must be generated together
> - The references must not have deadlock

##### type

Uses the generator of another type. For example, generating values for a `varchar` column using an `int` type generator:

```yaml
columns:
  - name: t_varchar2
    format: "year: {{%d}}, month: {{month}}"
    gen:
      type: int
      min: 1997
      max: 2097
```

Another example, a `varchar` type column using `json` (or `struct`) format for generation:

```yaml
columns:
  - name: t_varchar2
    gen:
      type: struct<foo:int, bar:text>
      # fields: # Optional: Define rules for foo and bar if needed
      #   - name: foo
      #     gen:
      #       inc:
      #       start: 1000
```

##### golang

Uses Go code for a custom generator, supports Go stdlib:

```yaml
columns:
  - name: t_varchar
    gen:
      golang: |
        import "fmt"

        var i int
        func gen() any {
            i++
            return fmt.Sprintf("Is odd: %v.", i%2 == 1)
        }
```

#### Complex types map/array/struct/json/variant

Complex types have special generation rules:

- For MAP types, you can specify generation rules for `key` and `value` separately:

    ```yaml
      columns:
        - name: t_map_varchar  # map<varchar(255),varchar(255)>
          key:
            format: "key-{{%d}}"
            gen:
              # Auto-increment starting from 0, step is 1
              inc:
          value:
            length: {min: 20, max: 50}
    ```

- For ARRAY types, use `element` to specify the generation rules for its elements:

    ```yaml
    columns:
      - name: t_array_string  # array<text>
        length: {min: 1, max: 10} # Specifies the number of elements in the array
        element: # Specifies the rules for each element
          gen:
            enum: [foo, bar, foobar]
    ```

- For STRUCT types, use `fields` or `field` to specify the generation rules for each field:

    ```yaml
    columns:
      - name: t_struct_nested  # struct<foo:text, struct_field:array<text>>
        fields:
          - name: foo
            length: 3
          - name: struct_field
            length: 10 # Refers to the length of the array for struct_field
            element: # Specifies rules for elements if struct_field is an array or map
              null_frequency: 0
              length: 2 # Refers to the length of each string element in the array
    ```

- For JSON/JSONB/VARIANT types, use `structure` to specify the structure:

    ```yaml
    columns:
      - name: json1
        structure: |
          struct<
            c1: varchar(3),
            c2: struct<array_field: array<text>>,  # Supports nested types
            c3: boolean
          >
        fields: # Corresponds to the fields defined in 'structure'
          - name: c1 # Rules for c1
            length: 1
            null_frequency: 0
          - name: c2 # Rules for c2 (which is a struct)
            fields: # Nested fields for c2
              - name: array_field # Rules for array_field within c2
                length: 1 # Length of the array
                element: # Rules for elements of array_field
                  format: "nested array element: {{%s}}"
                  null_frequency: 0
                  length: 2 # Length of each string element in the array
    ```

- For HLL types, the default value is `hll_empty()`, you can set its value from other column at the same table:

    ```yaml
    columns:
      - name: t_hll # The value of t_hll will be `hll_hash(t_str)`
        from: t_str
    ```

### AI Generation（via OpenAI/Deepseek）

When generating data using AI, you can pass in a query to ensure that the generated data can be retrieved by that query.

You must pass the `--llm` and `--llm-api-key` parameters. The former represents the OpenAI/Deepseek model name (e.g., `deepseek-chat` and `deepseek-reasoner`), and the latter is the API Key:

```bash
# Generate data from exported t1, t2 tables
dodo gendata --dbs db1 --tables t1,t2 \
    --llm 'deepseek-chat' --llm-api-key 'sk-xxx' \
    --query 'select * from t1 join t2 on t1.a = t2.b where t1.c IN ("a", "b", "c") and t2.d = 1' \
    --anonymize # anonymize sql before sending to LLM

# Generate data from any create-table and query
cd example/usercase
dodo gendata -C example.dodo.yaml --ddl 'ddl/*.sql' --query "$(cat sql/*)"

# Use --prompt to add additional hints
dodo gendata ... --prompt 'Generate 1000 rows for each table'
```

## Replay

`dodo replay --help`

You need to first [Dump Queries](#dump-queries), then replay based on the dump SQL files.

```sh
# Dump
dodo dump --dump-query --audit-logs fe.audit.log

# Replay, results are placed in the `output/replay` directory by default. Each file represents a client, and each line in the file represents the result of a SQL query.
dodo replay -f output/q0.sql
```

> [!NOTE]
> Executing the replay command multiple times will append results to the previous results, unless `--clean` is specified, which deletes the previous `output/replay` directory.

---

### Replay Speed and Concurrency

The principle of replay is that SQL from different clients runs concurrently, while SQL from the same client runs serially with an interval, strictly calculated according to the audit log:

```sh
# sql1 and sql2 are two consecutive SQLs executed by the same client
Interval duration = sql2 start time - sql1 start time - sql1 execution duration
```

#### Custom Speed and Concurrency

Controlled by the following parameters:

- `--speed`: Controls the replay speed, affecting the "interval duration" mentioned above. For example, `--speed 0.5` means slowing down by half, while `--speed 2` means speeding up twice. The principle is to proportionally increase or decrease the interval duration between consecutive SQLs from the same client. Note that if the SQL execution time itself is too long, `--speed` may not be effective.
- `--client-count int`: Resets the number of clients, and all SQLs will be evenly distributed among the clients to run in parallel. **Setting this value makes the replay effect unpredictable!** By default, it's the same as the number of clients in the log to achieve the same effect as online.

> [!TIP]
> If you only want to replay with 50 concurrency without intervals, and each SQL is independent, you can set `--speed 999999 --client-count 50`.

---

### Other Replay Parameters

- `-c, --cluster`: The cluster for replay, only useful in Cloud mode.
- `--result-dir`: Replay result directory, default `output/replay`.
- `--users`: Only replay SQL initiated by these users, default is to replay for all users.
- `--from` and `--to`: Replay SQL within a specified time range.
- `--max-hash-rows`: Maximum number of hash result rows to record during replay, used to compare if two replay results are consistent. Default is no hashing.
- `--max-conn-idle-time`: Maximum idle time for a client connection. If the interval duration between consecutive SQLs from the same client exceeds this value, the connection will be recycled. Default is `5s`.

## Diff Replay Results

`dodo diff --help`

There are two ways:

1. Compare two replay results:

    ```sh
    dodo diff output/replay1 output/replay2
    ```

2. Compare dump SQL with its replay result:

    ```sh
    dodo diff --min-duration-diff 2s --original-sqls 'output/sql/*.sql' output/replay
    ```

> `--min-duration-diff` means print SQLs whose execution duration difference exceeds this value. Default is `100ms`.

## Best Practices

### Command-line Prompts and Autocompletion

`dodo completion --help`

When the installation is complete or when you execute the command above, it will provide instructions on how to enable autocompletion.

---

### Environment Variables and Configuration Files

`dodo --help`

Besides command-line arguments, there are two other ways:

1. Pass parameters through uppercase environment variables prefixed with `DORIS_xxx`, e.g., `DORIS_HOST=xxx` is equivalent to `--host xxx`.
2. Pass parameters through a configuration file, e.g., `dodo --config xxx.yaml`. See [example](./example/example.dodo.yaml).

Parameter priority from high to low:

1. Command-line arguments
2. Environment variables
3. Configuration file specified by `--config`
4. Default configuration file `~/.dodo.yaml`

---

### Monitoring Dump/Replay Process

`--log-level debug/trace`

`debug` outputs a brief process, while `trace` shows a detailed process, such as SQL execution time and details during replay.

---

### Multi-FE Replay

Each FE's audit log is separate. When dumping, they must be dump separately. When replaying, they must also be replayed separately and simultaneously. For example, for a 2 FE cluster:

```sh
# Dump audit logs for fe1 and fe2 separately
dodo dump --dump-query --audit-logs fe1.audlt.log -O fe1
dodo dump --dump-query --audit-logs fe2.audlt.log -O fe2

# Replay audit logs for fe1 and fe2 simultaneously
nohup dodo replay -H <fe1.ip> -f fe1/sql/q0.sql -O fe1 &
nohup dodo replay -H <fe2.ip> -f fe2/sql/q0.sql -O fe2 &
```

---

### Large-scale Batch Replay

When the volume of SQL to be replayed is too large, for example, replaying logs for a whole month (31 days), it's best to replay in hourly batches. Use `--from` and `--to` during dump to batch (or batch manually after dump). Example:

```sh
dump YEAR_MONTH="2025-03" # <-- Change this line
dump DORIS_YES=1

for day in {1..31} ; do
  day=$(printf "%02d" $day)

  for hour in {0..23} ; do
      hour=$(printf "%02d" $hour)
      output=output/$day/$hour
      sql=$output/q0.sql

      echo "dumping and replaying at $day-$hour"

      # Dump
      dodo dump --dump-query --from "$YEAR_MONTH-$day $hour:00:00" --to "$YEAR_MONTH-$day $hour:59:59" --audit-log-table __internal_schema.audit_log --output "$output"

      # Replay, clear previous replay results, 50 clients concurrently, run continuously
      dodo replay -f "$sql" --result-dir result --clean --client-count 50 --speed 999999

      # View replay results
      dodo diff --min-duration-diff 1s --original-sqls $sql result -Ldebug 2>&1 | tee -a "result-$day.txt"
  done
done
```

---

### Find SQLs with Replay Duration Exceeding 1s

Search the replay result directory directly. It is recommended to use [ripgrep](https://github.com/BurntSushi/ripgrep); using `grep` is similar:

```sh
# Find SQLs with execution time exceeding 1s
rg '"durationMs":\d{4}' output/replay

# Find SQLs with execution time exceeding 6s
rg -e '"durationMs":[6-9]\d{3}' -e '"durationMs":\d{5}' output/replay
```

---

### Automation

For example, when writing scripts to dump/replay multiple files, it's inconvenient to manually input `y` for confirmation. You can set the environment variable `DORIS_YES=1` or `DORIS_YES=0` to automatically confirm or deny.

---

### AI Automatic Reproduction of User BUGs

You’ll need a directory to store user scenarios, with the following structure (example location: [example/usercase](./example/usercase)):  

```sh
├── ddl                       # User's DDL statements (preferably named <db>.<table>.table.sql)
│   ├── example.ob.table.sql
│   └── example.rb.table.sql
├── sql                       # User's queries
│   └── q0.sql
└── prompt.txt         # Gemini prompts (e.g., "generate 100k rows per table"). Usually copy-pasted from examples.
```

Three steps:

1. Install [`dodo`](./README.md#install), [`Gemini CLI`](https://github.com/google-gemini/gemini-cli), and the `mysql` command
2. Clone the [dodo](https://github.com/Thearas/dodo) repository locally and create a dodo config file `dodo.yaml`:

    ```sh
    git clone https://github.com/Thearas/dodo.git
    cd dodo

    cat > dodo.yaml <<EOF
    host: 127.0.0.1
    port: 9030
    http-port: 8030
    user: root
    dbs: [example]

    llm: deepseek-chat    # or o3-mini, etc.
    llm-api-key: sk-xxxx  # Your LLM API key
    EOF
    ```

3. Run `gemini -s` in the CLI, then type `dodo --config: @dodo.yaml, prompt: @example/usercase/prompt.txt` in the Gemini dialog and press Enter.

It will then run autonomously. We only need to approve its execution plans.

---

## Anonymization

`dodo anonymize --help`

For basic usage, see [README.md](./README.md#anonymize).

Anonymization uses the Go version of Doris Antlr4 Parser, which is currently case-insensitive. For example, `table1` and `TABLE1` will produce the same result.

### Parameters

- `-f, --file`: Read SQL from a file. If '-', read from standard input.
- `--anonymize-reserve-ids`: Reserve ID fields, do not anonymize them.
- `--anonymize-id-min-length`: ID fields with a length less than this value will not be anonymized. Default is `3`.
- `--anonymize-method`: Hash method, `hash` or `minihash`. The latter generates a concise dictionary based on the former, making anonymized IDs shorter. Default is `minihash`.
- `--anonymize-minihash-dict`: When the hash method is `minihash`, specify the concise dictionary file. Default is `./dodo_hashdict.yaml`.

## FAQ

### How to provide the tool to customers, and is there any impact on the production environment?

If customers cannot access the internet, download the [latest binary](https://github.com/Thearas/dodo/releases) and provide it directly. The Linux version has no dependencies and can run directly on the machine. By default, the tool will not perform any write operations on the cluster.

If you are concerned about resource consumption during dump, you can set `--parallel=1`. Memory consumption will be at most tens of megabytes, and execution time is generally in seconds.

When replaying, please execute in batches and ensure sufficient resources.

---

### The number of dump SQLs is less than in the audit log

First, check the [Dump Queries](#dump-queries) parameters to see if any SQLs were filtered out.

Then, enable `--log-level=debug` to see if any of the following situations occurred:

- `ignore sql with duplicated query_id`: Duplicate `query_id`s will be ignored. This is a bug in Doris itself.
- `query has been truncated`: SQL that is too long will be truncated. Please check Doris's [`audit_plugin_max_sql_length`](https://doris.apache.org/docs/admin-manual/audit-plugin#audit-log-configuration) configuration.

---

### Dump SQL has syntax errors

It is recommended to add the `-s, --strict` parameter during dump to validate SQL syntax correctness. Then, check the `query_id` output by the tool and find it in the audit log. It could be one of the following two situations:

1. The tool unescapes `\r`, `\n`, and `\t` in log SQL. However, if the original SQL itself contains these characters, it may cause syntax errors.
2. There is an issue with the audit log itself.

Generally, errors are infrequent. You can manually modify the dump SQL.

---

### Dump statistics do not match the actual ones

Check the `method` field of columns in the dump `stats.yaml`. If it is `SAMPLE` (i.e., sampling), then there might be a significant deviation from the actual values.

```yaml
columns:
  - name: col_int
    ndv: 10
    null_count: 4969
    data_size: 800000
    avg_size_byte: 8
    min: "2022"
    max: "2030"
    method: SAMPLE # <-- here
```

It is recommended to specify `--analyze` during dump, or first manually execute `ANALYZE DATABASE WITH SYNC`/`ANALYZE TABLE WITH SYNC`, and then dump.

---

### Replay error: too many connections

There are two situations:

1. Because audit logs do not contain a `connection id`, the tool replays based on clients (`ip:port`) rather than connections. However, a client may consist of multiple serial connections. In this case, the tool does not know when to disconnect, leading to connections not being released promptly.

    By default, replay connections are automatically released after 5s of inactivity. However, sometimes too many connections and loss of session variables can still occur. You can adjust `--max-conn-idle-time`.
2. If `--speed` is set too high, too many SQLs are squeezed into a short period for execution. Reducing the `--speed` value can solve this. Refer to [Custom Speed and Concurrency](#custom-speed-and-concurrency).

Additionally, there is a general solution: increase the user's maximum number of connections [`max_user_connections`](https://doris.apache.org/docs/admin-manual/config/user-property#max_user_connections).
