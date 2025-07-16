# Dodo

Main features:

1. **Dump** schema and query
2. [**Generate fake data**](#generate-data) for tables with *AI powered*
3. **Replay** audit log
4. **Anonymize** database, table, column and comment in SQL

> [!IMPORTANT]
> **See [Introduction & FAQ](./introduction.md) / [中文版](./introduction-zh.md) for more details.**

[![demo](https://asciinema.org/a/706093.svg)](https://asciinema.org/a/706093)

## Install

```sh
curl -sSL https://raw.githubusercontent.com/Thearas/dodo/master/install.sh | bash
```

## Usage

There are two types of workflows, with each step representing a `dodo` command:

- No data generation needed: `Dump -> Replay -> Diff Replay Results`
- Data generation needed: `Dump -> Create Schemas (Optional) -> Generate and Import Data -> Replay -> Diff Replay Results`

> By default, only `SELECT` statments will be dumped. Use `--only-select=false` to dump all.

```sh
# Dump
dodo dump --help

# dump schemas of database db1 and db2
dodo dump --dump-schema --dbs db1,db2 --host <host> --port <port> --user root --password '***' 

# also dump queries from audit logs of db1 and db2
dodo dump --dump-schema --dump-query --dbs db1,db2 --audit-logs 'fe.audit.log,fe.audit.log.20240802-1'

# dump queries from audit log table instead of files, need enable <https://doris.apache.org/docs/admin-manual/audit-plugin>
dodo dump --dump-query --audit-log-table <db.table> --from '2024-11-14 18:45:25' --to '2024-11-14 18:45:26'


# Create dump schemas in another DB server
dodo create --help

# create all tables and views of db1 and db2, it auto finds dump schemas under 'output/' dir
dodo create --dbs db1,db2 --host <host> --port <port> --user root --password '***'

# run any create table/view SQL in db1
dodo create --ddl 'dir/*.sql' --db db1


# Generate data (Totally offline!)
dodo gendata --help

# gen data from any create-table SQL (MySQL, Hive, ...)
dodo gendata --ddl table.sql

# gen data for db1 and db2, it auto finds dump schemas under 'output/' dir
dodo gendata --dbs db1,db2 --host <host> --port <port> --user root --password '***'

# gen data with config
dodo gendata --dbs db1 --genconf example/gendata.yaml

# gen data with AI (Deepseek LLM)
dodo gendata -l 'deepseek-chat' -k '<deepseek-api-key>' --ddl table.sql --query 'select xxx'


# Import data (Require curl command)
dodo import --help

# import data for db1, it auto finds generated data under 'output/' dir
dodo import --dbs db1,db2 --host <host> --http-port <http-port> --user root --password '***'

# import data for t1 and t2 in db1
dodo import --dbs db1 --table t1,t2

# import data from any CSV file
dodo import --tables db1.t1 --data data.csv


# Replay
dodo replay --help

# replay queries in dump sql file (from audit logs)
dodo replay --host <host> --port <port> --user root --password '***' -f output/sql/q0.sql

# replay with args
dodo replay -f output/sql/q0.sql \
    --from '2024-09-20 08:00:00' --to '2024-09-20 09:00:00' \
    --users 'readonly,root' --dbs 'db1,db2' \   # filter sql by users and databases
    --speed 0.5 \                               # increase(< 1.0) or decrease(> 1.0) the time between two serial sqls proportionally, default 1
    --result-dir output/replay \
    --clean                                     # clean 'output/replay' dir before replay


# Diff replay result
dodo diff --help

# diff replay result which is slower more than 200ms than original
dodo diff --min-duration-diff 200ms --original-sqls 'output/sql/*.sql' output/replay

# diff of two replay result directories
dodo diff replay1/ replay2/
```

### Config

You may want to pass parameters by config file or environment, see [Environment Variables and Configuration Files](./introduction.md#environment-variables-and-configuration-files).

### Generate Data

Generate CSV data from create-table SQLs. All databases with similar syntax as Doris are supported, like MySQL, Hive, etc.

Here is an example. See [Custom Generation Rules](./introduction.md#custom-generation-rules) and **[AI Generation](./introduction.md#ai-generationvia-openaideepseek)** for more:

```sh
echo 'create table t1 (
    a varchar(2),
    b struct<foo:tinyint>,
    c date
)' > t1.sql

dodo gendata --ddl t1.sql --rows 5

cat output/gendata/t1/*
sO☆{"foo":-66}☆2020-07-23
lg☆{"foo":-121}☆2021-06-15
4☆{"foo":-117}☆2015-06-17
8h☆{"foo":-83}☆2024-09-06
KW☆{"foo":7}☆2019-02-02
```

### Anonymize

**This feature is experimental, case-insensitive, which means `table1` and `TABLE1` will have the same result.** Two ways:

- Use `dodo anonymize`:

    ```bash
    echo "select * from table1" | dodo anonymize -f -
    ```

- Use `--anonymize` flag while dumping:

    ```bash
    dodo dump ... --anonymize
    ```

> [!NOTE]
> Keep `./dodo_hashdict.yaml` if you want the result to be consistent (put it at current directory, or specify by `--anonymize-minihash-dict`).

## Build

1. Install **optional** dependences:

    - On macOS: [vectorscan](https://github.com/VectorCamp/vectorscan) with Chimera support
    - On Linux: [hyperscan](https://intel.github.io/hyperscan) with Chimera support

2. Run `make` (or `make build-hyper` if the dependences in step 1 are installed)

## Update Doris Parser

```sh
make gen
```
