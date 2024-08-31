# Introduction

- [Workflow](#workflow)
- [Deploy Cluster](#deploy-cluster)
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
    - [charset/letters](#charsetletters)
    - [length](#length)
    - [format](#format)
    - [gen](#gen)
      - [inc](#inc)
      - [ref](#ref)
      - [enum](#enum)
      - [parts](#parts)
      - [type](#type)
      - [golang](#golang)
    - [Ghost Columns](#ghost-columns)
    - [Complex Types](#complex-types-maparraystructjsonvariant)
    - [Generate High-Precision Large Numbers](#generate-high-precision-large-numbers)
  - [AI Generation](#ai-generationvia-openaideepseek)
  - [Generate and Import External Table Data](#generate-and-import-external-table-data)
- [Replay](#replay)
  - [Replay Speed and Concurrency](#replay-speed-and-concurrency)
  - [Other Replay Parameters](#other-replay-parameters)
- [Diff Replay Results](#diff-replay-results)
- [Export table data](#export-table-data)
- [Best Practices](#best-practices)
  - [Command-line Prompts and Autocompletion](#command-line-prompts-and-autocompletion)
  - [Environment Variables and Configuration Files](#environment-variables-and-configuration-files)
  - [Monitoring Dump/Replay Process](#monitoring-dumpreplay-process)
  - [Multi-FE Replay](#multi-fe-replay)
  - [Large-scale Batch Replay](#large-scale-batch-replay)
  - [Find SQLs with Replay Duration Exceeding 1s](#find-sqls-with-replay-duration-exceeding-1s)
  - [Automation](#automation)
  - [AI Automatic Reproduction of User BUGs](#ai-automatic-reproduction-of-user-bugs)
- [Spark](#spark)
- [Anonymization](#anonymization)
- [FAQ](#faq)
  - [How to provide the tool to customers, and is there any impact on the production environment?](#how-to-provide-the-tool-to-customers-and-is-there-any-impact-on-the-production-environment)
  - [The number of dump SQLs is less than in the audit log](#the-number-of-dump-sqls-is-less-than-in-the-audit-log)
  - [Dump SQL has syntax errors](#dump-sql-has-syntax-errors)
  - [Dump statistics do not match the actual ones](#dump-statistics-do-not-match-the-actual-ones)
  - [Replay error: too many connections](#replay-error-too-many-connections)

## Workflow

There are two types of workflows:

- No data generation needed: `Dump -> Replay -> Diff Replay Results`
- Data generation needed: `Dump -> Create Schemas -> Generate and Import Data -> Replay -> Diff Replay Results`

## Deploy Cluster

`dodo cluster deploy --help`/`dodo cluster deploy-cloud --help`

Quickly deploy a Doris (cloud) cluster for testing.

```sh
# quickly start 1fe + 1be on local machine
dodo cluster deploy ./local-doris-package.tar.gz
# or:
dodo cluster deploy https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.0.1-bin-x64.tar.gz

# deploy Doris 4.0.1 on single node
dodo cluster deploy https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.0.1-bin-x64.tar.gz \
    --fe 172.20.48.1 --be 172.20.48.1 \
    --ssh-password '***'

# deploy daily build Doris(branch-4.0 from OSS) on 3 nodes (2fe + 3be)
 dodo cluster deploy oss://daily-build/doris/branch-4.0_release_output.tar.gz \
    --fe 172.20.48.1,172.20.48.2 --be 172.20.48.1,172.20.48.2,172.20.48.3 \
    --oss-access-key '***' --oss-secret-key '***' \
    --ssh-password '***'


# for downloading FDB
export https_proxy=http://172.20.64.7:8123

# Deploy cloud cluster locally: 1FDB, 1MS, 1FE and 1BE
dodo cluster deploy-cloud ./doris-package.tar.gz

# Deploy cloud cluster with 1FDB, 2MS, 2FE and 3BE
dodo cluster deploy-cloud oss://daily-build/doris/master_release_output.tar.gz \
    --fdb 172.20.48.1 --ms 172.20.48.1,172.20.48.2 \
    --fe 172.20.48.1,172.20.48.2 --be 172.20.48.2,172.20.48.3,172.20.48.4 \
    --oss-access-key '***' --oss-secret-key '***' \
    --ssh-password '***'
```

> [!TIP]
>
> You may want to pass flags by config file, see: [example/example-deploy.dodo.yaml](./example/example-deploy.dodo.yaml)

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
> - Each dump will overwrite the previous dump SQL file.

### Other Dump Parameters

- `--analyze`: Automatically runs `ANALYZE TABLE <table> WITH SYNC` before dumping a table to make statistics more accurate. Default is off.
- `--parallel`: Controls the dump concurrency. Increasing it speeds up the dump; decreasing it uses fewer resources. Default is `min(machine_cores-2, 10)`.
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
dodo create --dbs db1,db2 --host <host> --port <port> --user root --password '***'

# Create dump table1 and table2
dodo create --dbs db1 --tables table1,table2

# Create dump schemas of db1 under './ddl/'
dodo create --dbs db1 --ddl ddl/

# Keep creating remaining schemas and summarize failures at the end
dodo create --dbs db1,db2 --continue-on-error
```

## Generate and Import Data

`dodo gendata --help`/`dodo import --help`

`dodo gendata` will automatically [Dump Tables](#dump-tables-and-views) if no local DDLs are found.

```sh
# Generate data for all tables in db1 and db2
dodo gendata --dbs db1,db2

# Generate data for table1
dodo gendata --tables db1.table1 # or --dbs db1 --tables table1

# Generate data with config
dodo gendata ... --genconf gendata.yaml

# Generate insert SQLs instead of CSV and print them to stdout
dodo gendata ... --output-format insert --print


# Import data for all tables with generated data in db1 and db2, --http-port defaults to 8030
dodo import --http-port <http-port> --dbs db1,db2

# Import data for table1 with generated data
dodo import --http-port <http-port> --tables db1.table1 # or --dbs db1 --tables table1

# Import any CSV data file(s) into table1
dodo import --http-port <http-port> --tables db1.table1 --data 'my_table/*.csv'
```

> [!TIP]
>
> - Specifying `-Ldebug` during import shows the specific `curl` command, which is helpful for reproducing and troubleshooting issues.

<details>

<summary>Tool implementation logic</summary>

In implementation, the tool performs these actions in two stages based on the `--dbs` and `--tables` parameters:

1. In the generation stage:

    1. Scans the dump directory `output/ddl/` for matching `<db>.<table>.table.sql` files and dump them if not found locally. The dump directory (or specific `<basename>.sql` files) can be specified with `--ddl`.
    2. Combines the corresponding statistics file `<db>.stats.yaml` with the custom generation rules file (specified by `--genconf`) to determine the final generation rules.
    3. Generates CSV files into the data generation directory `output/gendata/<db>.<table>/` (or `output/gendata/<basename>/`) according to the generation rules.
2. In the import stage:

    1. Scans the data generation directory `output/gendata/` for matching `<db>.<table>/*` data files. The data generation directory can be specified with `--data`.
    2. Uses the `curl` command to run StreamLoad for data import.

</details>

### Default Generation Rules

- By default, `NULL` values are not generated. This can be changed by specifying `null_frequency` in [Custom Generation Rules](#custom-generation-rules)
- Columns without custom generation rules will use the default rules below
- Remember that the `string/text/varchar/char` letter is randomly generated, unpredictable, the charset is alphanumeric (a-z, A-Z, 0-9)

Default generation rules for various types:

| Type | Length | Min - Max or Default | Structure |
| --- | --- | --- | --- |
| ARRAY | 1 - 3 | | |
| MAP | 1 - 3 | | |
| JSON/JSONB | | | STRUCT<col1:SMALLINT, col2:SMALLINT> |
| VARIANT | | | STRUCT<col1:SMALLINT, col2:SMALLINT> |
| BITMAP | 5 | element: 0 - MaxInt32 | |
| HLL | | hll_empty() | |
| TEXT/STRING/VARCHAR | 1 - min(length in type, 10) | | |
| CHAR | length in type | | |
| TINYINT | | 0 - MaxInt8 | |
| SMALLINT | | 0 - MaxInt16 | |
| INT | | 0 - MaxInt32 | |
| BIGINT | | 0 - MaxInt32 | |
| LARGEINT | | 0 - MaxInt32 | |
| FLOAT | | 0 - MaxInt16 | |
| DOUBLE | | 0 - MaxInt32 | |
| DECIMAL | | 0 - MaxInt32, precision: 12, scale: 2 | |
| DATE | | 10 years ago - now | |
| DATETIME | | 10 years ago - now | |
| IPV4 | | random | |
| IPV6 | | random | |

### Custom Generation Rules

Generate data using configuration files specified via `dodo gendata --genconf gendata.yaml`. For a full reference, see [example/gendata.yaml](./example/gendata.yaml):

```yaml
# gendata.yaml
type:
  bigint:
    min: 0
    max: 1000000
  date:
    min: 1997-02-16
    max: 2025-06-12
tables:
  - name: employees
    row_count: 1000  # Optional, default 1000 (can be overridden by --rows)
    columns:
      - name: employee_id
        gen:
          inc: 1      # step 1 (default 1)
          start: 1000 # start from 1000 (default 1)
      - name: department_id
        null_frequency: 0.1  # 10% NULL
        min: 1
        max: 10
      - name: salary
        min: 15000.00
        max: 16000.00
      - name: hire_date
        min: "1997-01-15"
        max: "1997-01-15"
```

> [!NOTE]
>
> When custom generation rules are specified, only tables declared in the `tables` section of `gendata.yaml` will be generated; undeclared tables are skipped. The same applies to AI-generated `gendata.yaml`.

You can concatenate multiple `gendata.yaml` contents in one file (separated by `---`). It equals to call `dodo gendata --genconf <file>` multiple rounds. Example:

```yaml
# Dataset 1
tables:
  ...
---
# Dataset 2
null_frequency: 0.05
type:
  ...
tables:
  ...
```

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

Example of table-level rules, the columns that are not in the table rules will use the global default rules:

```yaml
tables:
  - name: employees
    row_count: 100  # Optional, default is 1000 (can be overridden by --rows)
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
    precision: 12
    scale: 3
    min: 100
    max: 102  # Actual maximum value is 102.999
```

#### charset/letters

Specifies the charset **or** letters for string types. Only one of them can be specified. `charset` currently only supports `english` and `alphanumeric` (default). For example:

```yaml
columns:
  - name: t_str1
    charset: english
  - name: t_str2
    letters: "123456"
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

No matter what generation rule, there always can have a `format`, which will format the generated column data basing on the template and **return a string**, and then output it to CSV file, so it is the last step of data generation.

You can use tags to format the return value of the column, such as `{{%s}}` or `{{%d}}`, etc., with the same syntax as Go's `fmt.Sprintf()`, but it does not support multiple formatting verbs within a single tag, such as `{{%Y-%m-%d}}`.
There can only be one such label in a `format` (except using [`parts`](#parts)).

For example:

```yaml
columns:
  - name: t_str
    format: 'substr length 1-5: {{%s}}'
    length:
      min: 1
      max: 5
```

Note: If the generator returns NULL, format will also return NULL.

#### gen

Optional custom generator.

> [!IMPORTANT]
>
> - One of the following generator key MUST be defined under a `gen`: `inc`, `enum`, `parts`, `ref`, `type`, and they can only be defined under `gen`
> - Only one generator can be specified at the same time, for example, if the `inc` generator is specified, the `enum` generator cannot be specified
> - `gen` will override the gen rules at the column level (except `null_frequency` and `format`), makes `length`, `min/max` no longer effective

##### inc

Return type: `int64` for integer types, `float64` for float types, `string` (formatted time) for date/datetime types

Auto-increment generator, can specify start value and step. Supports integer, float, and date/datetime column types:

```yaml
columns:
  # Integer increment (default)
  - name: t_string
    format: "string-inc-{{%d}}"
    # `length` won't work, override by `gen`
    # length: 10
    gen:
      inc: 2      # Step is 2 (default 1)
      start: 100  # Starts from 100 (default 1)
      end: 200    # Optional, maximum value, loops back to start when exceeded (default no loop)

  # Float increment
  - name: price     # float or double column
    gen:
      inc:
        start: 1.0
        step: 0.5   # Step is 0.5, generates 1.0, 1.5, 2.0, ...
        end: 100.0  # Optional, loops back to start when exceeded

  # Date increment
  - name: dt        # date column
    gen:
      inc:
        start: "2025-01-01"
        step: "24h"   # Step is 1 day (default for DATE type), generates 2025-01-01, 2025-01-02, ...
                      # Supports Go duration format: "1h", "30m", "24h", etc.

  # Datetime increment
  - name: created   # datetime column
    gen:
      inc:
        start: "2025-01-01 00:00:00"
        step: "1s"    # Step is 1 second (default for DATETIME type)
        end: "2025-12-31 23:59:59"  # Optional, loops back to start when exceeded
```

##### ref

Return type: the type of the referenced column, if the referenced column has `format`, the return type is `string`

Reference generator, uses values from another column. Supports two scenarios:

1. **Reference same table column**: Copy the value from another column in the same row, **no `table.` prefix**
2. **Reference other table column**: Randomly uses values from other `table.column`, the `table` must exists and be generated together

Typically used for columns that need the same values, like relational columns `t1 JOIN t2 ON t1.c1 = t2.c1` or `WHERE t1.c1 = t2.c1`:

```yaml
columns:
  # Reference same table column (no table prefix)
  - name: col2
    gen:
      ref: col1  # col2 will have the same value as col1 in each row

  # Reference other table column
  - name: t_int
    gen:
      ref: employees.department_id
      limit: 100  # Randomly select 100 values (default 1000)

  - name: t_struct # struct<dp_id:int, name:text>
    fields:
      - name: dp_id
        format: "1{{%6d}}"
        gen:
          ref: employees.department_id # ref can be used in nested rules
      - name: name
        gen:
          ref: employees.name
```

> [!IMPORTANT]
>
> The references must not have deadlock

##### enum

Return type: same as the return type of enum elements

Enum generator (aka. `enums`), randomly selects from given values, values can be literals or generators (the type will be inferred from parent generator). There is an optional config `weights` (can only be used with `enum`) and the sum of weights must be 1.0:

```yaml
columns:
  - name: t_null_string
    gen:
      enum: [foo, bar, foobar]
      weights: [0.2, 0.6, 0.2]  # Optional, specifies the probability of each value being selected

  - name: t_str
    gen:
      # randomly choose one literal or generators to generate value, each has 20% probability
      enum:
        - "123"
        - length: {min: 5, max: 10}
        - format: "my name is {{%s}}"
        - gen:
            ref: t1.c1
        - gen:
            enum: [1, 2, 3]

  - name: t_json
    gen:
      # randomly choose one structure, each has 50% probability
      enum:
        - structure: struct<foo:int>
        - structure: array<string>
```

##### parts

Return type: [return type of part1, return type of part2, ...]

Must be used together with [`format`](#format). Flexibly combine multiple values ​to produce the final result.

`parts` generates multiple values ​at a time and fills them into `{{%xxx}}` of [`format`](#format) in order. The value of each part can be a literal or a generator(the type will be inferred from parent generator):

```yaml
columns:
  - name: date1 # date
    format: "2025-{{%02d}}-{{%02d}}"
    gen:
      parts:
        - gen: # month
            type: int
            min: 1
            max: 12
        - gen: # day
            ref: table1.column1

  - name: t_null_char # char(10)
    format: "{{%s}}--{{%02d}}" # parts must be used with format
    gen:
      parts:
        - "prefix"
        - gen:
            enum: [2, 4, 6, 8, 10]
```

##### type

Return type: the type specified by `type`

Uses the generator of another Doris type. For example, generating values for a `varchar` column using an `int` type generator:

```yaml
columns:
  - name: t_varchar2
    format: "year: {{%d}}, month: 12"
    gen:
      # do not set min/max out of the range of the Doris int
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
```

##### golang

Return type: the return type of `func gen()`

Uses Go code for a custom generator, supports Go stdlib. By default, `func gen()` is executed in parallel. You can set `parallel: false` to change it to serial execution:

```yaml
columns:
  - name: t_varchar
    gen:
      # parallel: false  # Optional, default true
      golang: |
        import "fmt"

        var i int
        func gen() any {
            i++
            return fmt.Sprintf("Is odd: %v.", i%2 == 1)
        }
```

#### Ghost Columns

Ghost columns are virtual columns that participate in data generation and can be referenced by other columns (same-table or cross-table) via `ref`, but are **excluded from the output** (CSV/INSERT). This solves the problem where a column's value has been modified by `format`, making the raw value unavailable for other columns to reference.

A ghost column is defined with `ghost: true` in the column rules and have a `_ghost_` name prefix. It **must** have a `gen` rule with a custom generator (`inc`, `enum`, `parts`, `ref`, `type`, or `golang`).

```yaml
tables:
  - name: employees
    columns:
      # Ghost column: generates raw values, not written to output
      - name: _ghost_dept_id
        ghost: true
        gen:
          inc: 1
          start: 1
          end: 100

      # DDL column: refs ghost column, applies its own format
      - name: department_id
        format: "D-{{%05d}}"
        gen:
          ref: _ghost_dept_id  # gets raw value 1, 2, 3, ... (not "D-00001")

  - name: sales
    columns:
      - name: dp_id
        format: "1{{%06d}}"
        gen:
          ref: employees._ghost_dept_id  # cross-table ref to ghost column
```

In this example:

- `_ghost_dept_id` generates incremental values `1, 2, 3, ...` but is not written to the CSV
- `employees.department_id` references the ghost column and formats it as `D-00001, D-00002, ...`
- `sales.dp_id` references the same ghost column across tables and formats it as `1000001, 1000002, ...`
- Both columns get the **raw** value from the ghost column, not the formatted value of another DDL column

> [!NOTE]
>
> - Ghost column names must not conflict with DDL column names
> - Ghost columns can reference other columns (including other ghost columns) via `ref`
> - **Ghost tables are not supported**

#### Complex types map/array/struct/json/variant

Complex types have special generation rules:

- For MAP types, you can specify generation rules for `key` and `value` separately:

    ```yaml
      columns:
        - name: t_map_varchar  # map<varchar(255),varchar(255)>
          key:
            format: "key-{{%d}}"
            gen:
              # Auto-increment starting from 0, step is 2
              inc: 2
            # map key is uniqued by default
            # unique: true
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

- For HLL types, you can use `from` to set its value from other column at the same table:

    ```yaml
    columns:
      - name: t_hll
        from: hll_hash(t_str) # The value of t_hll will be `hll_hash(t_str)`
    ```

- For AGG_STATE types, by default it generates according to the column type definition, you can also use `args` to specify the generation rules for each agg function argument:

    ```yaml
    columns:
      - name: t_agg_state # agg_state<avg(decimal(38,18) null)>
        args:
          - name: 0 # Specify the generation rule for arg 0, i.e. decimal(38,18) null
            min: 0
            max: 1000
    ```

#### Generate High-Precision Large Numbers

[By default](#default-generation-rules) numbers larger than `int32` will not be generated. You can configure this using `min/max` in `gendata.yaml`.

Due to limitations in YAML, large numbers will lose precision when generated. In this case, you must use strings to represent large numbers, for example:

```yaml
columns:
  - name: t_largeint
    min: "10000000000000000000000000000000000000"
    max: "90000000000000000000000000000000000000"
  - name: t_decimal
    min: "10000000000000000000000000000000000000"
    max: "90000000000000000000000000000000000000"
  - name: t_decimal256_str
    gen:
      type: decimal(76, 9)
      min: "1000000000000000000000000000000000000000000000000000000000000000000000000000" 
      max: "9000000000000000000000000000000000000000000000000000000000000000000000000000"
```

### AI Generation（via OpenAI/Deepseek）

When generating data using AI, you can pass in a query to ensure that the generated data can be retrieved by that query. **This method is not suitable for generating large amounts of tables or queries.**

You must pass the `--llm` and `--llm-api-key` parameters. The former represents the OpenAI/Deepseek model name (e.g., `deepseek-v4-flash` and `deepseek-v4-pro`), and the latter is the API Key:

```bash
# Generate data from exported t1, t2 tables
dodo gendata --dbs db1 \
    --llm 'deepseek-v4-pro' --llm-api-key 'sk-xxx' \
    --query 'select * from t1 join t2 on t1.a = t2.b where t1.c IN ("a", "b", "c") and t2.d = 1'

# Generate data from any create-table and query
cd example/usercase
dodo gendata -C example.dodo.yaml --ddl 'ddl/*.sql' --query "$(cat sql/*)"

# Use --prompt to add additional hints
dodo gendata ... --prompt 'Generate 1000 rows for each table'
```

> [!TIP]
>
> AI will store the generation conf to `.dodo/gendata.yaml`, you can keep or modiry it for future use.

### Generate and Import External Table Data

1. Before importing, you need to install [`pyspark`](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
2. Both generation and import require specifying the `--catalog` that the external table belongs to

Everything else is the same as internal tables.

```sh
# Generate data for external tables
dodo gendata --catalog my_catalog --dbs db1 --tables ext_table1,ext_table2

# Import data for external tables
dodo import --catalog my_catalog --dbs db1 --tables ext_table1,ext_table2
```

## Replay

`dodo replay --help`

Supports replaying SQL files exported by dodo, as well as replaying any SQL file. For the former, you need to first [Dump Queries](#dump-queries), then replay based on the dumped SQL files.

```sh
# Dump
dodo dump --dump-query --audit-logs fe.audit.log
# Replay, results are placed in the `output/replay` directory by default. Each file represents a client, and each line in the file represents the result of a SQL query.
dodo replay -f output/q0.sql

# Replay any SQL file with 5 concurrent clients, must specify a database
dodo replay -f query.sql --client-count 5 --db db1
```

> [!NOTE]
> Each replay will append to the previous replay result file.

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

## Spark

`dodo spark --help`

Given any `--catalog` and `--table`, generate and run a `pyspark` command to connect to the external table environment. Supports hive/iceberg/hudi/paimon:

```sh
dodo spark --catalog my_catalog --table db1.table1
```

dodo will automatically:

1. Determine if you are in an Alibaba Cloud VPC environment to decide whether to use `-internal.aliyuncs.com` or `.aliyuncs.com` domain names
2. Download the jar packages required by `spark`, such as `org.apache.hadoop:hadoop-client:3.3.2`. Automatically applies the `https_proxy` environment variable
3. Download `kerberos` `krb5.conf` and `keytab` files (requires `--ssh-password/--ssh-private-key` configuration)

> [!TIP]
>
> Specifying `-Ldebug --dry-run` will show the exact `pyspark` command. You can also copy it and modify it to run with `spark-sql`.

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

## Export table data

`dodo export --help`

Encapsulates the [Export](https://doris.apache.org/docs/sql-manual/sql-statements/data-modification/load-and-export/EXPORT) SQL statement, exporting table data to `s3`, `hdfs` or `local` storage.

> [!NOTE]
>
> - The command will wait for the export to complete, and the export will be canceled if the command is terminated.
> - The column separator is `☆` by default in CSV format. You can specify them with `-p column_separator=xxx`

```sh
# The placeholders `{db}` and `{table}` can be used in the export target `--url`, representing the database name and table name respectively
dodo export --dbs db1 --tables t1,t2 --target s3 --url 's3://bucket/export/{db}/{table}_' -p timeout=60 -w s3.endpoint=xxx -w s3.access_key=xxx -w s3.secret_key=xxx
```

## Best Practices

### Command-line Prompts and Autocompletion

`dodo completion --help`

When the installation is complete or when you execute the command above, it will provide instructions on how to enable autocompletion.

---

### Environment Variables and Configuration Files

`dodo --help`

Besides command-line arguments, there are two other ways:

1. Pass parameters through uppercase environment variables prefixed with `DODO_xxx`, e.g., `DODO_HOST=xxx` is equivalent to `--host xxx`.
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
dump DODO_YES=1

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

For example, when writing scripts to dump/replay multiple files, it's inconvenient to manually input `y` for confirmation. You can set the environment variable `DODO_YES=1` or `DODO_YES=0` to automatically confirm or deny.

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

    llm: deepseek-v4-pro  # or deepseek-v4-flash, etc.
    llm-api-key: sk-xxxx    # LLM API key
    EOF
    ```

3. Run `gemini -iyp 'Your task: @example/usercase/prompt.txt, do not ask any questions, just proceed'` at the root of [dodo](https://github.com/Thearas/dodo) repository.

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

If customers cannot access the internet, download the [latest binary](https://github.com/Thearas/dodo/releases/latest) and provide it directly. The Linux version has no dependencies and can run directly on the machine. By default, the tool will not perform any write operations on the cluster.

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
