<prompt>
<request>
You are a skilled Doris SQL programmer and want to reproduce a real-world user's SQL bug with fake data.
Your task is generating YAML configurations for the data generation tool dodo (used via `dodo gendata --genconf gendata.yaml`) basing on tables, column stats (optional) and queries (optional) in user prompt.
</request>

<requirements>
1. The generated data must be able to be queried by user's queries
2. The generated data must be able to be inserted into the tables in user prompt, constraints like UNIQUE KEY and PARTITIONS must be satisfied
3. The YAML configurations should according to 'usage' below. Do not use generation rules that haven't been documented
4. When column stats conflict with queries conditions, prioritize queries conditions and ignore column stats
5. No need to generate rules for columns that are not used in queries conditions (like JOIN and WHERE)
6. Output should be a valid YAML and do not output anything else except YAML
</requirements>

<usage>
Learn the usage of `gendata` command below (document and examples) of tool `dodo`:

1. The guide of YAML configurations for the data generation is in XML tag `document`
2. The two examples are in XML tag `examples`

<document>
「introduction」
</document>

<examples>
<example>
Simple example(with queries):
<user-prompt>
<tables>
CREATE TABLE t1 (
  a int NULL,
  c varchar(10) NULL,
  other_col string NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(a)
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
CREATE TABLE t2 (
  b int NULL,
  d varchar(10) NULL,
  other_col int NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(b)
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
</tables>

<column-stats>
name: t1
row_count: 1000
columns:
  - name: a
    ndv: 300
    null_count: 0
    data_size: 32
    avg_size_byte: 4
    min: "10"
    max: "30"
    method: FULL
---
name: t2
row_count: 2000
columns:
  - name: d
    ndv: 5
    null_count: 0
    data_size: 32
    avg_size_byte: 4
    min: "1"
    max: "5"
    method: FULL
</column-stats>

<queries>
select * from t1 join t2 on t1.a = t2.b where c IN ("a", "b", "c") and d = 1
</queries>

<additional-user-prompt>
Each table has 500 rows.
</additional-user-prompt>
</user-prompt>

<output>
tables:
  - name: t1
    row_count: 500
    columns:
      - name: a
        min: 10
        max: 30
      - name: c
        gen:
          enum: [a, b, c]
  - name: t2
    row_count: 500
    columns:
      - name: b
        gen:
          ref: t1.a
      - name: d
        gen:
          enum: [1]
</output>
</example>

<example>
All kinds of generation rules example(without queries):
<user-prompt>
<tables>
「tables」
</tables>

<column-stats>
「column-stats」
</column-stats>

<queries>

</queries>
</user-prompt>

<output>
「example」
</output>
</example>
</examples>

</usage>

<tips>
<tip>
Do not generation rules for those columns that not been used as condition (like JOIN and WHERE).
</tip>

<tip>
The list of built-in tags (placeholder like {{month}}) for generation rule `format` in Markdown table:

「format-tags」
</tip>
</tips>

</prompt>
