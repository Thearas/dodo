import os
import sys
from pyspark.sql.types import _parse_datatype_string, StructType
from pyspark.sql import SparkSession

table_id = os.getenv("DODO_TABLE_ID", "unknown_table")
csv_path = os.getenv("DODO_CSV_PATH", "unknown_csv")
delimiter = os.getenv("DODO_CSV_DELIMITER", "☆")

spark: SparkSession = (
    SparkSession.builder.appName("Load CSV").enableHiveSupport().getOrCreate()
)
# spark.conf.set("write.parquet.target-file-size-bytes", 64 * 1024 * 1024)

schema = StructType()
cols = spark.catalog.listColumns(table_id)
for c in cols:
    data_type = _parse_datatype_string(c.dataType)
    schema.add(c.name, data_type=data_type, nullable=c.nullable)


dfs = (
    spark.readStream
    .option("delimiter", delimiter)
    .option("maxFilesPerTrigger", 20)
    .schema(schema)
    .csv(csv_path, nullValue="\\N")
)

partitions = [c.name for c in spark.catalog.listColumns(table_id) if c.isPartition]
if partitions:
    dfs = dfs.repartition(*partitions)

query = (
    dfs.writeStream.trigger(availableNow=True)
    .option(
        "checkpointLocation",
        f"{csv_path}/checkpoint",
    )
    .option("hoodie.schema.on.read.enable", "true")
    .option("hoodie.metadata.enable", "false")
    .option("hoodie.parquet.small.file.limit", "100")
    .toTable(table_id, outputMode="append")
)
query.awaitTermination()
sys.exit(0)
