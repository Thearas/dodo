# Dodo

Main features:

1. [Deploy Doris cluster](#deploy-doris)
2. Dump schema and audit log
3. [Generate fake data](#generate-data) for tables with *AI powered*
4. Replay audit log or any SQL file
5. Top SQL analysis
6. Anonymize SQL

> [!IMPORTANT]
> **See [Introduction & FAQ](./introduction.md) / [中文版](./introduction-zh.md) for more details.**

[![demo](https://asciinema.org/a/706093.svg)](https://asciinema.org/a/706093)

## Install

```sh
curl -sSL https://raw.githubusercontent.com/Thearas/dodo/master/install.sh | bash
```

or download from [Releases](https://github.com/Thearas/dodo/releases/latest).

## Usage

Usually there are two types of workflows.

- No data generation needed: `Dump -> Replay -> Diff Replay Results`
- Data generation needed: `Dump -> Create Schemas -> Generate and Import Data -> Replay -> Diff Replay Results`

> If you only want to replay the most frequent or complex queries, use `dodo topsql` to find them before replay.

```sh
# Dump
dodo dump --help

# dump schemas of database db1 and db2
dodo dump --dump-schema --dbs db1,db2 --host <host> --port <port> --user root --password '***' 

# dump queries from audit logs of db1 and db2
dodo dump --dump-query --dbs db1,db2 --audit-logs 'fe.audit.log,fe.audit.log.20240802-1'

# dump queries from audit log table instead of files, need enable <https://doris.apache.org/docs/admin-manual/audit-plugin>
dodo dump --dump-query --audit-log-table <db.table> --from '2024-11-14 18:45:25' --to '2024-11-14 18:45:26'


# Create dump schemas in another DB server
dodo create --help

# create all tables and views of db1 and db2, it auto finds dump schemas under 'output/ddl/' dir
dodo create --dbs db1,db2 --host <host> --port <port> --user root --password '***'

# create dump schemas of db1 under './ddl/' 
dodo create --dbs db1 --ddl ddl/

# keep creating remaining schemas and summarize failures at the end
dodo create --dbs db1,db2 --continue-on-error


# Generate data
dodo gendata --help

# gen 10k rows of CSV data from any create-table SQL (MySQL, Hive, ...)
dodo gendata --ddl table.sql --rows 10000

# gen insert SQL
dodo gendata --ddl table.sql --output-format insert

# gen CSV data for db1 and db2, it will dump tables if not found locally
dodo gendata --dbs db1,db2 --host <host> --port <port> --user root --password '***'

# gen CSV data with config
dodo gendata --dbs db1 --genconf example/gendata.yaml

# gen CSV data with AI (Deepseek LLM)
dodo gendata -l 'deepseek-v4-pro' -k '<deepseek-api-key>' --ddl table.sql --query 'select xxx'


# Import data (Require curl command)
dodo import --help

# import data for db1, it auto finds generated data under 'output/' dir
dodo import --dbs db1,db2 --host <host> --http-port <http-port> --user root --password '***'

# import data for t1 and t2 in db1
dodo import --dbs db1 --table t1,t2 --http-port <http-port>

# import data from any CSV file
dodo import --tables db1.t1 --data data.csv -s ',' --http-port <http-port>


# Replay
dodo replay --help

# replay dump sql file (from audit logs)
dodo replay --host <host> --port <port> --user root --password '***' -f output/sql/q0.sql

# replay any sql file with 5 parallel clients
dodo replay --db testdb -f query.sql --client-count 5 --db db1

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


# Top SQL - Find most frequent and complex SQLs
dodo topsql --help

# find top 50 sql, sort by count and complexity
dodo topsql -f 'output/sql/*.sql' --top 50

# only analyze specified databases from dodo dump metadata
dodo topsql -f 'output/sql/*.sql' --dbs db1,db2
```

### Config

You may want to pass parameters by config file or environment, see [Environment Variables and Configuration Files](./introduction-zh.md#环境变量和配置文件).

## Deploy Doris

More at [Deploy Doris Cluster](./introduction-zh.md#部署集群).

```sh
# Deploy Doris cluster for testing
dodo cluster deploy --help

# quickly start 1fe + 1be on local machine
dodo cluster deploy ./local-doris-package.tar.gz

# deploy Doris 4.0.1 on single node
dodo cluster deploy https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.0.1-bin-x64.tar.gz \
    --fe 172.20.48.1 --be 172.20.48.1 \
    --ssh-password '***'

# deploy daily build Doris(branch-4.0 from OSS) on 3 nodes (2fe + 3be)
 dodo cluster deploy oss://daily-build/doris/branch-4.0_release_output.tar.gz \
    --fe 172.20.48.1,172.20.48.2 --be 172.20.48.1,172.20.48.2,172.20.48.3 \
    --oss-access-key '***' --oss-secret-key '***' \
    --ssh-password '***'
```

### Generate Data

Generate CSV data from create-table SQLs. All databases with similar syntax as Doris are supported, like MySQL, Hive, etc.

Here is an example. See [Custom Generation Rules](./introduction-zh.md#自定义生成规则) and **[AI Generation](./introduction-zh.md#ai-生成数据使用-openaideepseek)** for more:

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

Run `make` (or `make install` if you want to install to `/usr/local/bin/dodo`), requires Go 1.25+.

## Update Doris Parser

```sh
make gen
```
