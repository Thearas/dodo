# Introduction

- [工作流](#工作流)
- [导出](#导出)
  - [导出表和视图](#导出表和视图)
  - [导出查询](#导出查询)
  - [其他导出参数](#其他导出参数)
- [创建表和视图](#创建表和视图)
- [生成和导入数据](#生成和导入数据)
  - [默认的生成规则](#默认的生成规则)
  - [自定义生成规则](#自定义生成规则)
    - [全局规则与表规则](#全局规则与表规则)
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
    - [复杂类型](#复杂类型-maparraystructjsonvariant)
  - [AI 生成数据](#ai-生成数据使用-openaideepseek)
- [回放](#回放)
  - [回放速度和并发](#回放速度和并发)
  - [其他回放参数](#其他回放参数)
- [对比回放结果](#对比回放结果)
- [最佳实践](#最佳实践)
  - [命令行提示与自动补全](#命令行提示与自动补全)
  - [环境变量和配置文件](#环境变量和配置文件)
  - [监看导出/回放过程](#监看导出回放过程)
  - [多 FE 回放](#多-fe-回放)
  - [大量分批回放](#大量分批回放)
  - [找出回放时长超过 1s 的 SQL](#找出回放时长超过-1s-的-sql)
  - [自动化](#自动化)
  - [AI 自动复现用户 BUG](#ai-自动复现用户-bug)
- [脱敏](#脱敏)
- [FAQ](#faq)
  - [怎么把工具给客户，对生产环境有没有影响](#怎么把工具给客户对生产环境有没有影响)
  - [导出的 SQL 数量比审计日志里的少](#导出的-sql-数量比审计日志里的少)
  - [导出的 SQL 有语法错误](#导出的-sql-有语法错误)
  - [导出的统计信息与实际不符](#导出的统计信息与实际不符)
  - [回放报错连接数超了](#回放报错连接数超了)

## 工作流

分为两种，每一步代表一条 `dodo` 指令：

- 不需要造数据：`导出 -> 回放 -> 对比回放结果`
- 需要造数据：`导出 -> 创建表和视图（可选）-> 生成和导入数据 -> 回放 -> 对比回放结果`

## 导出

`dodo dump --help`

分为两部分，「导出表和视图」和「导出查询」。二者可以合入一条 `dodo` 命令。

### 导出表和视图

`dodo dump --dump-schema`

从 Doris 数据库导出表和视图的 `CREATE` 语句。默认会同时导出表的统计信息，如果统计信息与实际相差较大，推荐指定 `--analyze`。见 [导出的统计信息与实际不符](#导出的统计信息与实际不符)。

```sh
# 导出 db1 和 db2 的所有表和视图
dodo dump --dump-schema --host xxx --port xxx --user xxx --password xxx --dbs db1,db2

# 默认导出到 output/ddl 下：
output
└── ddl
    ├── db1.t1.table.sql
    ├── db1.stats.yaml
    ├── db2.t2.table.sql
    └── db2.stats.yaml
```

### 导出查询

`dodo dump --dump-query`

可以从审计日志表或文件导出，默认只导出 `SELECT` 语句，可以加上 `--only-select=false` 一并导出其他语句。

```sh
# 从审计日志表导出，表名一般是 __internal_schema.audit_log
dodo dump --dump-query --audit-log-table <db.table> --from '2024-11-14 17:00:00' --to '2024-11-14 18:00:00' --host xxx --port xxx --user xxx --password xxx

# 从审计日志文件导出，'*' 代表匹配多个文件（注意要用单引号括起来）
dodo dump --dump-query --audit-logs 'fe.audit.log,fe.audit.log.20240802*'

# 默认导出到 output/sql 下：
output
└── sql
    ├── q0.sql
    └── q1.sql
```

> [!NOTE]
>
> - 从日志文件导出时，`q0.sql` 对应第一个日志文件、`q1.sql` 对应第二个、以此类推；但从日志表导出时，只会写入到 `q0.sql`
> - 导出结果会追加到前一次导出的 SQL 文件中，除非指定 `--clean`，删除之前的 `output/ddl` 和 `output/sql` 目录

### 其他导出参数

- `--analyze` 导出表前自动跑 `ANALYZE TABLE <table> WITH SYNC`，使统计信息更准确，默认关闭
- `--parallel` 控制导出并发量，调大导出更快，调小占用资源更少，默认 `min(机器核数, 10)`
- `--dump-stats` 导出表时也导出统计信息，导出在 `output/ddl/db.stats.yaml` 文件，默认开启
- `--only-select` 是否从只导出 `SELECT` 语句，默认开启
- `--from` 和 `--to` 导出时间范围内的 SQL
- `--query-min-duration` 导出 SQL 的最小执行时长
- `--query-states` 导出 SQL 的状态，可以是 `ok`、`eof` 和 `err`
- `-s, --strict` 从审计日志导出时校验 SQL 语法正确性
- `--audit-log-encoding` 审计日志文件编码，默认自动检测
- `--anonymize` 导出时脱敏，比如 `select * from table1` 变为 `select * from a`
- `--anonymize-xxx` 其他脱敏参数，见 [脱敏](#脱敏)

### 创建表和视图

`dodo create --help`

需要先[导出表和视图](#导出表和视图)到本地，然后去另一个 Doris 创建：

```sh
# 创建 db1 和 db2 的所有已导出的表和视图
dodo create --dbs db1,db2

# 创建已导出的 table1 和 table 表
dodo create --dbs db1 --tables table1,table2

# 在 db1 中跑任意 create table/view SQL
dodo create --ddl 'dir/*.sql' --db db1
```

## 生成和导入数据

`dodo gendata --help`/`dodo import --help`

需要先[导出表和视图](#导出表和视图)到本地，再生成数据和导入：

```sh
# 给 db1 和 db2 的所有已导出的表生成数据
dodo gendata --dbs db1,db2

# 给已导出的 table1 生成数据
dodo gendata --tables db1.table1 # 或 --dbs db1 --tables table1

# 无需事先导出，给任意 create table SQL 也能生成数据
# P.s. 不一定是 Doris，其他数据库比如 Hive 也行
dodo gendata --ddl 'ddl/*.sql'


# 给 db1 和 db2 的所有已生成数据的表导入数据
dodo import --dbs db1,db2

# 给已生成数据的 table1 导入数据
dodo import --tables db1.table1 # 或 --dbs db1 --tables table1

# 导入任意 CSV 数据文件到 table1
dodo import --tables db1.table1 --data 'my_table/*.csv'
```

实现上，工具会按照 `--dbs` 和 `--tables` 参数，在两阶段分别做这些事：

1. 在生成阶段：

    1. 扫描导出目录 `output/ddl/` 下、符合要求的 `<db>.<table>.table.sql` 文件。导出目录（或具体的 `<basename>.sql` 文件）可以用 `--ddl` 指定
    2. 结合对应的统计信息文件 `<db>.stats.yaml` 与自定义生成规则文件（由 `--genconf` 指定），算出最终的生成规则
    3. 根据生成规则，生成 CSV 到数据生成目录 `output/gendata/<db>.<table>/`（或 `output/gendata/<basename>/`）
2. 在导入阶段：

    1. 扫描数据生成目录 `output/gendata/` 下、符合要求的 `<db>.<table>/*` 数据文件。数据生成目录可以用 `--data` 指定
    2. 用 `curl` 命令跑 StreamLoad 导入数据

> [!TIP]
>
> - 导入时指定 `-Ldebug` 可以看到 `curl` 具体命令，方便复现和排查问题

### 默认的生成规则

默认不生成 `NULL`，可以在[自定义生成规则](#自定义生成规则)中指定 `null_frequency` 更改。

各类型的默认生成规则：

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

- 字符串类型是随机生成的，字符集是大小写字母 + 数字 (a-z, A-Z, 0-9)

### 自定义生成规则

生成数据时用 `--genconf gendata.yaml` 指定，完整示例见 [example/gendata.yaml](./example/gendata.yaml)。

#### 全局规则与表规则

生成规则可以分为全局和表级别。表级别会覆盖全局配置。

全局规则示例：

```yaml
# 全局默认 NULL 比例
null_frequency: 0

# 全局类型生成规则
type:
  bigint:
    min: 0
    max: 100
  date:
    min: 1997-02-16
    max: 2025-06-12
```

表级别规则示例：

```yaml
tables:
  - name: employees
    row_count: 100  # 可选，默认 1000（也可通过 --rows 指定）
    columns:
      - name: department_id
        null_frequency: 0.1  # 10% NULL
        min: 1
        max: 10
```

#### null_frequency

指定字段的 NULL 值比例，取值范围 0-1。例如：

```yaml
null_frequency: 0.1  # 10% 的概率生成 NULL
```

#### min/max

指定数值类型字段的取值范围。例如：

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

指定 DECIMAL 类型的精度和小数位数。例如：

```yaml
columns:
  - name: t_decimal
    precision: 10
    scale: 3
    min: 100
    max: 102  # 实际最大值为 102.999
```

#### length

指定字 bitmap、string、array 或 map 类型长度范围。例如：

```yaml
columns:
  - name: t_str
    # or just `length: <int>` if min and max are the same, like `length: 5`
    length:
      min: 1
      max: 5
```

#### format

无论什么生成规则，都能有一个 `format`，它会在该列生成数据后跑，通过自定义模板生成字符串，然后输出到 CSV 文件。`format` 中可以使用两种标签（或叫占位符）：

1. 格式化该列的返回值，如 `{{%s}}` 或 `{{%d}}` 等，语法同 Go 的 `fmt.Sprintf()`。一个 `format` 中只允许有一个此类标签（除非使用 [`parts`](#parts)）
2. 内置标签如 `{{month}}`、`{{year}}` 等，所有内置标签见：[src/generator/README.md](./src/generator/README.md#format-tags)。

例如：

```yaml
columns:
  - name: t_str
    format: 'substr length 1-5: {{%s}}'
    length:
      min: 1
      max: 5
```

注意：如果生成器返回 NULL，format 也会返回 NULL。

#### gen

可选自定义生成器，支持以下几种，必须在 `gen:` 的下面定义：

> [!IMPORTANT]
>
> - 会覆盖列本身层级的生成规则（`null_frequency` 和 `format` 除外）
> - 同一时间只能指定一个生成器，比如指定了 `inc` 生成器，就不能再指定 `enum` 生成器

##### inc

自增生成器，可指定起始值和步长：

```yaml
columns:
  - name: t_string
    format: "string-inc-{{%d}}"
    # `length` won't work, override by `gen`
    # length: 10
    gen:
      inc: 2      # 步长为 2（默认 1）
      start: 100  # 从 100 开始（默认 0）
```

##### enum

枚举生成器，从给定值中随机选择，枚举值可以是字面量或者生成规则：

```yaml
columns:
  - name: t_null_string
    gen:
      enum: [foo, bar, foobar]
      weights: [0.2, 0.6, 0.2]  # 可选，指定各值被选中的概率

  - name: t_str
    gen:
      # 随机选择一个生成规则来生成值，各有 1/4 的概率被选中
      enum:
        - length: 5
        - length: {min: 5, max: 10}
        - format: "my name is {{username}}"
        - gen:
            enum: [1, 2, 3]
```

##### parts

必须与 [`format`](#format) 一起使用。灵活组合多个值来生成最终结果。

`parts` 一次会生成多个值，按顺序填充到 [`format`](#format) 的 `{{%xxx}}` 中，各部分的值可以是字面量或生成规则：

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

引用生成器，随机使用其他表的列的值。
一般在用于关系列之间，比如 `t1 JOIN t2 ON t1.c1 = t2.c1` 或 `WHERE t1.c1 = t2.c1`：

```yaml
columns:
  - name: t_int
    # format: "1{{%6d}}"
    gen:
      ref: employees.department_id
      limit: 100  # 随机选择 100 个值（默认 1000）

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
> - 引用的源表必须一起生成
> - 引用之间不能有死锁

##### type

使用其他类型的生成器，比如 `varchar` 的列用 `int` 类型生成：

```yaml
columns:
  - name: t_varchar2
    format: "year: {{%d}}, month: {{month}}"
    gen:
      type: int
      min: 1997
      max: 2097
```

又比如 `varchar` 类型的列使用 `json`（或 `struct`）格式生成：

```yaml
columns:
  - name: t_varchar2
    gen:
      type: struct<foo:int, bar:text>
      # fields:
      #   - name: foo
      #     gen:
      #       inc:
      #       start: 1000
```

##### golang

使用 Go 代码，支持使用 Go stdlib：

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

#### 复杂类型 map/array/struct/json/variant

复合类型有特殊的生成规则：

- MAP 类型，可分别指定 `key` 和 `value` 的生成规则：

    ```yaml
      columns:
        - name: t_map_varchar  # map<varchar(255),varchar(255)>
          key:
            format: "key-{{%d}}"
            gen:
              # 从 0 开始自增，步长为 1
              inc:
          value:
            length: {min: 20, max: 50}
    ```

- ARRAY 类型，用 `element` 指定元素的生成规则：

    ```yaml
    columns:
      - name: t_array_string  # array<text>
        length: {min: 1, max: 10}
        element:
          gen:
            enum: [foo, bar, foobar]
    ```

- STRUCT 类型，用 `fields` 或 `field` 指定每一个字段的生成规则：

    ```yaml
    columns:
      - name: t_struct_nested  # struct<foo:text, struct_field:array<text>>
        fields:
          - name: foo
            length: 3
          - name: struct_field
            length: 10
            element:
              null_frequency: 0
              length: 2
    ```

- JSON/JSONB/VARIANT 类型，用 `structure` 指定结构：

    ```yaml
    columns:
      - name: json1
        structure: |
          struct<
            c1: varchar(3),
            c2: struct<array_field: array<text>>,  # 支持嵌套类型
            c3: boolean
          >
        fields:
          - name: c1
            length: 1
            null_frequency: 0
          - name: c2
            fields:
              - name: array_field
                length: 1
                element:
                  format: "nested array element: {{%s}}"
                  null_frequency: 0
                  length: 2
    ```

- HLL 类型，默认值为 `hll_empty()`，你也可以指定从同一张表的其他列生成：

    ```yaml
    columns:
      - name: t_hll # t_hll 的值将为 `hll_hash(t_str)`
        from: t_str
    ```

### AI 生成数据（使用 OpenAI/Deepseek）

AI 生成时可以传入查询，令生成的数据能被该查询查出来。

必须传入 `--llm` 和 `--llm-api-key` 两个参数，前者代表 OpenAI/Deepseek 的模型名称（比如 `deepseek-chat` 和 `deepseek-reasoner`），后者代表 API Key：

```bash
# 从导出的 t1,t2 表生成数据
dodo gendata --dbs db1 --tables t1,t2 \
    --llm 'deepseek-chat' --llm-api-key 'sk-xxx' \
    --query 'select * from t1 join t2 on t1.a = t2.b where t1.c IN ("a", "b", "c") and t2.d = 1' \
    --anonymize # 将 SQL 脱敏后再发给 LLM 

# 从任意 create-table 和 query 生成数据
cd example/usercase
dodo gendata -C example.dodo.yaml --ddl 'ddl/*.sql' --query "$(cat sql/*)"

# 使用 `--prompt` 附加提示
dodo gendata ... --prompt '每张表生成 1000 行数据'
```

## 回放

`dodo replay --help`

需要先[导出查询](#导出查询)，然后基于导出的 SQL 文件回放。

```sh
# 导出
dodo dump --dump-query --audit-logs fe.audit.log

# 回放，结果默认放在 `output/replay` 目录下，每个文件代表一个客户端，文件中每行代表一条 SQL 的结果
dodo replay -f output/q0.sql
```

> [!NOTE]
> 执行多次回放命令，结果会追加到前一次的结果中，除非指定 `--clean`，删除之前的 `output/replay` 目录

---

### 回放速度和并发

回放的原理是不同客户端的 SQL 并发，同一客户端的 SQL 串行且有间隔时长，严格按照审计日志计算：

```sh
# sql1 和 sql2 是同一个客户端相邻执行的两条 SQL
间隔时长 = sql1 开始时间 - sql2 开始时间 - sql1 执行时长
```

#### 自定义速度和并发

由以下参数控制：

- `--speed` 控制回放速度，作用于上面提到的「间隔时长」，比如 `--speed 0.5` 代表放慢一倍，而 `--speed 2` 代表加快一倍。原理是按比例增加或减少同一客户端的相邻 SQL 的间隔时长，注意如果 SQL 本身的执行时间过长，则 `--speed` 效果不佳
- `--client-count int` 重新设置客户端数目，并且所有 SQL 都将被均衡地分散到各个客户端并行跑，**设置此值无法预料回放效果！**。默认跟日志里的客户端数一样，以达到跟线上相同的效果

> [!TIP]
> 如果只想以 50 并发无间隔回放，且每条 SQL 都独立无依赖，可以设置 `--speed 999999 --client-count 50`。

---

### 其他回放参数

- `-c, --cluster` 回放的集群，仅在 Cloud 模式下有用
- `--result-dir` 回放结果目录，默认 `output/replay`
- `--users` 只回放这些用户发起的 SQL，默认回放全部用户的
- `--from` 和 `--to` 回放时间范围内的 SQL
- `--max-hash-rows` 回放时记录的最大 hash 结果行数，用于对比两次回放结果是否一致，默认不 hash
- `--max-conn-idle-time` 客户端连接的最大空闲时间，同一客户端的相邻 SQL 的间隔时长超出此值时，连接会被回收，默认 `5s`

## 对比回放结果

`dodo diff --help`

有两种方式：

1. 对比两次回放结果：

    ```sh
    dodo diff output/replay1 output/replay2
    ```

2. 对比导出的 SQL 和它的回放结果：

    ```sh
    dodo diff --min-duration-diff 2s --original-sqls 'output/sql/*.sql' output/replay
    ```

> `--min-duration-diff` 表示打印执行时长差异超过此值的 SQL，默认 `100ms`

## 最佳实践

### 命令行提示与自动补全

`dodo completion --help`

安装完成或执行上面的命令时，会给出启用自动补全的方法。

---

### 环境变量和配置文件

`dodo --help`

除了命令行传参，还有两种方式：

1. 通过前缀为 `DORIS_xxx` 的大写环境变量传参，比如 `DORIS_HOST=xxx` 等价于  `--host xxx`
2. 通过配置文件传参，比如 `dodo --config xxx.yaml`，见 [example](./example/example.dodo.yaml)

参数优先级由高到低：

1. 命令行参数
2. 环境变量
3. `--config` 指定的配置文件
4. 默认配置文件 `~/.dodo.yaml`

---

### 监看导出/回放过程

`--log-level debug/trace`

`debug` 输出简略过程，而 `trace` 可以看到详细过程，比如回放时 SQL 的执行时间和详情等。

---

### 多 FE 回放

每个 FE 的 audit log 是分离的，导出时要分别导出，回放时也要分别、同时回放，比如 2 FE 集群：

```sh
# 分别导出 fe1 和 fe2 的审计日志
dodo dump --dump-query --audit-logs fe1.audlt.log -O fe1
dodo dump --dump-query --audit-logs fe2.audlt.log -O fe2

# 同时回放 fe1 和 fe2 的审计日志
nohup dodo replay -H <fe1.ip> -f fe1/sql/q0.sql -O fe1 &
nohup dodo replay -H <fe2.ip> -f fe2/sql/q0.sql -O fe2 &
```

---

### 大量分批回放

回放的 SQL 量太大时，比如回放一个月 31 天的日志，最好以小时为单位分批回放，在导出时用 `--from` 和 `--to` 分批（或导出后手动分批），示例：

```sh
export YEAR_MONTH="2025-03" # <-- 改这一行
export DORIS_YES=1

for day in {1..31} ; do
  day=$(printf "%02d" $day)

  for hour in {0..23} ; do
      hour=$(printf "%02d" $hour)
      output=output/$day/$hour
      sql=$output/q0.sql

      echo "dumping and replaying at $day-$hour"

      # 导出
      dodo dump --dump-query --from "$YEAR_MONTH-$day $hour:00:00" --to "$YEAR_MONTH-$day $hour:59:59" --audit-log-table __internal_schema.audit_log --output "$output"

      # 回放，并清除前一次回放结果，50 个客户端并发，不间断跑
      dodo replay -f "$sql" --result-dir result --clean --client-count 50 --speed 999999

      # 查看回放结果
      dodo diff --min-duration-diff 1s --original-sqls $sql result -Ldebug 2>&1 | tee -a "result-$day.txt"
  done
done
```

---

### 找出回放时长超过 1s 的 SQL

直接搜索回放结果目录，建议用 [ripgrep](https://github.com/BurntSushi/ripgrep)，用 `grep` 也类似：

```sh
# 找出执行时间超过 1s 的
rg '"durationMs":\d{4}' output/replay

# 找出执行时间超过 6s 的
rg -e '"durationMs":[6-9]\d{3}' -e '"durationMs":\d{5}' output/replay
```

---

### 自动化

比如写脚本导出/回放多个文件时，不方便手动输入 `y` 确认，可以设置环境变量 `DORIS_YES=1` 或 `DORIS_YES=0` 自动确认或否认。

---

### AI 自动复现用户 BUG

首先需要一个目录存放用户的场景，目录结构跟以下一致，本示例在 [example/usercase](./example/usercase)：

```sh
├── ddl                       # 用户的建表语句，格式最好是 <db>.<table>.table.sql
│   ├── example.ob.table.sql
│   └── example.rb.table.sql
├── sql                       # 用户的查询语句
│   └── q0.sql
└── prompt.txt                # Gemini 提示词，可以在里面提需求（比如每张表生成 10w 行），一般复制粘贴示例的即可
```

三步搞定：

1. 安装 [`dodo`](./README.md#install)、[`Gemini CLI`](https://github.com/google-gemini/gemini-cli) 和 `mysql` 命令
2. 克隆 [dodo](https://github.com/Thearas/dodo) 仓库到本地，并在仓库下创建 dodo 的配置文件 `dodo.yaml`：

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

3. 在命令行跑 `gemini -s`，并在 `gemini` 对话框中输入 `dodo --config: @dodo.yaml, prompt: @example/usercase/prompt.txt`，回车

然后看着它自主运行，我们偶尔批准下它的执行计划即可。

---

## 脱敏

`dodo anonymize --help`

基础使用见 [README.md](./README.md#anonymize)。

脱敏使用 Go 版本的 Doris Anltr4 Parser，目前是大小写不敏感的，比如 `table1` 和 `TABLE1` 会有相同的结果。

### 参数

- `-f, --file` 从文件读取 SQL，如果为 '-' 则从标准输入读取
- `--anonymize-reserve-ids` 保留 ID 字段，不做脱敏
- `--anonymize-id-min-length` 长度小于此值的 ID 字段不做脱敏，默认 `3`
- `--anonymize-method` hash 方法，`hash` 或 `minihash`，后者在前者的基础上生成简要字典，让脱敏后的 ID 变短，默认是 `minhash`
- `--anonymize-minihash-dict` 当 hash 方法为 `minihash` 时，指定简要字典文件，默认 `./dodo_hashdict.yaml`

## FAQ

### 怎么把工具给客户，对生产环境有没有影响

客户不能科学上网的话，把[最新二进制](https://github.com/Thearas/dodo/releases)下载下来直接给，Linux 版是无依赖的，放机器上就能跑，默认情况下工具不会对集群有任何写入操作。

导出时担心消耗资源的话，可以设置 `--parallel=1`，内存消耗最多几十兆，一般执行时间在秒级。

回放时请分批执行并保证资源充足。

---

### 导出的 SQL 数量比审计日志里的少

首先检查[导出参数](#导出查询)，看看是不是过滤掉了。

然后打开 `--log-level=debug`，看看是不是以下情况：

- `ignore sql with duplicated query_id`：重复的 `query_id` 会被忽略，这是 Doris 本身的 bug
- `query has been truncated`：SQL 过长会被截断，请检查 Doris 的 [`audit_plugin_max_sql_length`](https://doris.apache.org/docs/admin-manual/audit-plugin#audit-log-configuration) 配置

---

### 导出的 SQL 有语法错误

建议在导出时加上 `-s, --strict` 参数，校验 SQL 语法正确性。然后看工具输出的 `query_id`，去审计日志里找，有可能是以下两种情况：

1. 工具会反转义日志 SQL 中的 `\\r`、`\\n` 和 `\\t`，但如果原始 SQL 本身包含这些字符，就可能导致语法错误
2. 审计日志本身有问题

一般出错的情况不多，手动修改导出后的 SQL 即可。

---

### 导出的统计信息与实际不符

检查导出的 `stats.yaml` 中列的 `method` 字段，如果是 `SAMPLE`（即采样），那么与实际可能偏差较大。

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

推荐导出时指定 `--analyze`，或先手动执行 `ANALYZE DATABASE WITH SYNC`/`ANALYZE TABLE WITH SYNC`，然后再导出。

---

### 回放报错连接数超了

有两种情况：

1. 因为审计日志中没有 `connection id`，所以工具以客户端（`ip:port`）而不是连接为单位进行回放，但一个客户端可能由多个串行的连接组成，这样工具不知道何时断开连接，从而导致连接不能及时释放

    默认回放连接 5s 无活动自动释放，但有时还是会出现连接过多和 session 变量丢失的情况，可以调整 `--max-conn-idle-time`
2. `--speed` 设置过大，过多的 SQL 被挤压到一小段时间执行，减小 `--speed` 值即可解决，参考[自定义速度和并发](#自定义速度和并发)

另外有一个通解：调大用户最大连接数 [`max_user_connections`](https://doris.apache.org/docs/admin-manual/config/user-property#max_user_connections)。
