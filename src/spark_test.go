package src

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparkCli_GetCmd(t *testing.T) {
	// Unset proxy env var to make test result consistent
	t.Setenv("https_proxy", "")
	t.Setenv("http_proxy", "")

	testCases := []struct {
		name     string
		cli      *SparkCli
		expected string
	}{
		{
			name: "hudi catalog",
			cli: &SparkCli{
				parallel:     1,
				sparkVersion: "3.2",
				CatalogName:  "hudi_catalog",
				CatalogType:  CatalogTypeHudi,
				CatalogProps: map[string]string{
					"hive.metastore.type": "hms",
				},
				HiveMetastoreUri: "thrift://localhost:9083",
			},
			expected: "pyspark -c spark.sql.defaultCatalog=hudi_catalog --master='local[1]' -c spark.shuffle.file.buffer=32k -c spark.sql.cbo.enabled=true -c spark.sql.cbo.joinReorder.enabled=true -c spark.sql.catalogImplementation=in-memory -c spark.ui.enabled=false -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.legacy.charVarcharAsString=true -c spark.hadoop.hive.exec.dynamic.partition=true -c spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.optimizer.dynamicPartitionPruning.useStats=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.serializer=org.apache.spark.serializer.KryoSerializer -c spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension -c spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog -c spark.sql.catalog.hudi_catalog.type=hive -c spark.hadoop.hive.metastore.uris=thrift://localhost:9083 -c spark.sql.hive.metastore.type=hms --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0",
		},
		{
			name: "iceberg catalog with hms",
			cli: &SparkCli{
				parallel:     1,
				sparkVersion: "3.3",
				CatalogName:  "iceberg_catalog",
				CatalogType:  CatalogTypeIceberg,
				CatalogProps: map[string]string{
					"iceberg.catalog.type": "hms",
				},
				HiveMetastoreUri: "thrift://localhost:9083",
			},
			expected: "pyspark -c spark.sql.defaultCatalog=iceberg_catalog --master='local[1]' -c spark.shuffle.file.buffer=32k -c spark.sql.cbo.enabled=true -c spark.sql.cbo.joinReorder.enabled=true -c spark.sql.catalogImplementation=in-memory -c spark.ui.enabled=false -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.legacy.charVarcharAsString=true -c spark.hadoop.hive.exec.dynamic.partition=true -c spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.optimizer.dynamicPartitionPruning.useStats=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog -c spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions -c spark.sql.catalog.iceberg_catalog.type=hive -c spark.sql.catalog.iceberg_catalog.uri=thrift://localhost:9083 --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.10.0",
		},
		{
			name: "paimon catalog",
			cli: &SparkCli{
				parallel:     1,
				sparkVersion: "3.4",
				CatalogName:  "paimon_catalog",
				CatalogType:  CatalogTypePaimon,
				CatalogProps: map[string]string{
					"paimon.catalog.type": "hms",
				},
			},
			expected: "pyspark -c spark.sql.defaultCatalog=paimon_catalog --master='local[1]' -c spark.shuffle.file.buffer=32k -c spark.sql.cbo.enabled=true -c spark.sql.cbo.joinReorder.enabled=true -c spark.sql.catalogImplementation=in-memory -c spark.ui.enabled=false -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.legacy.charVarcharAsString=true -c spark.hadoop.hive.exec.dynamic.partition=true -c spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.optimizer.dynamicPartitionPruning.useStats=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions --packages org.apache.paimon:paimon-spark3.4:1.0.1,org.apache.paimon:paimon-s3:1.0.1",
		},
		{
			name: "hive catalog with s3",
			cli: &SparkCli{
				parallel:              1,
				sparkVersion:          "3.2",
				CatalogName:           "hive_catalog",
				CatalogType:           CatalogTypeHive,
				CatalogProps:          map[string]string{},
				HiveMetastoreUri:      "thrift://localhost:9083",
				warehouse:             "s3://bucket/warehouse",
				storageProviderSchema: "s3",
				storageEndpoint:       "s3.us-west-2.amazonaws.com",
				storageAk:             "ak",
				storageSk:             "sk",
			},
			expected: "pyspark -c spark.sql.defaultCatalog=hive_catalog --master='local[1]' -c spark.shuffle.file.buffer=32k -c spark.sql.cbo.enabled=true -c spark.sql.cbo.joinReorder.enabled=true -c spark.sql.catalogImplementation=in-memory -c spark.ui.enabled=false -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.legacy.charVarcharAsString=true -c spark.hadoop.hive.exec.dynamic.partition=true -c spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.optimizer.dynamicPartitionPruning.useStats=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.catalog.hive_catalog.type=hive -c spark.hadoop.hive.metastore.uris=thrift://localhost:9083 -c spark.hadoop.fs.s3a.connection.maximum=1000 -c spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem -c spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.catalog.hive_catalog.s3.endpoint=http://s3.us-west-2.amazonaws.com -c spark.hadoop.fs.s3a.endpoint=s3.us-west-2.amazonaws.com -c spark.hadoop.fs.s3a.access.key=ak -c spark.hadoop.fs.s3a.secret.key=sk -c spark.sql.warehouse.dir=s3a://bucket/warehouse -c spark.sql.catalog.hive_catalog.warehouse=s3a://bucket/warehouse -c spark.driver.extraJavaOptions=' -Daws.accessKeyId=ak -Daws.secretAccessKey=sk' -c spark.executor.extraJavaOptions=' -Daws.accessKeyId=ak -Daws.secretAccessKey=sk' --packages org.apache.hadoop:hadoop-client:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := tc.cli.GetCmd()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, cmd)
		})
	}
}

func TestGetSparkVersion(t *testing.T) {
	if p, err := exec.LookPath("pyspark"); err != nil || p == "" {
		t.Skip("pyspark not found, skip")
	}

	v, err := getSparkVersion(context.Background())
	assert.NoError(t, err)
	assert.Regexp(t, `[23]\.\d`, v)
}
