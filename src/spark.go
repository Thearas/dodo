package src

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"

	"github.com/Thearas/dodo/src/parser"
)

type CatalogType string

var (
	CatalogTypeInternal CatalogType = "internal"
	CatalogTypeHive     CatalogType = "hive"
	CatalogTypeHudi     CatalogType = "hudi"
	CatalogTypeIceberg  CatalogType = "iceberg"
	CatalogTypePaimon   CatalogType = "paimon"

	AllCatalogTypes = []CatalogType{
		CatalogTypeInternal,
		CatalogTypeHive,
		CatalogTypeHudi,
		CatalogTypeIceberg,
		CatalogTypePaimon,
	}
)

type SparkCli struct {
	feIP string
	db   *sqlx.DB

	// extract from doris catalog
	CatalogName      string
	CatalogType      CatalogType
	HiveMetastoreUri string
	CatalogProps     map[string]string

	// Cloud storage: https://spark.apache.org/docs/latest/cloud-integration.html
	warehouse                                                                   string
	storageAk, storageSk, storageRegion, storageEndpoint, storageProviderSchema string

	sparkVersion  string
	localKrb5conf string

	feSSHEndpoint string
	feSSHPrivKey  string
	cacheDir      string
	parallel      int
}

func NewSparkCli(
	ctx context.Context,
	db *sqlx.DB,
	feip string,

	createCatalogStmt string,
	createTableStmt string,

	storageSecretKey string,

	feSSHEndpoint string,
	feSSHPrivKey string,

	cacheDir string,
	parallel int,
) (*SparkCli, error) {
	c := &SparkCli{
		feIP:          feip,
		db:            db,
		feSSHEndpoint: feSSHEndpoint,
		feSSHPrivKey:  feSSHPrivKey,
		cacheDir:      cacheDir,
		parallel:      parallel,
	}
	s, ok := parser.NewParser("create_catalog", createCatalogStmt).SupportedCreateStatement().(*parser.CreateCatalogContext)
	if !ok {
		return nil, fmt.Errorf("failed to parse create catalog statement:\n%s", createCatalogStmt)
	}
	c.CatalogName = parser.SqlIdentifier(s.GetCatalogName().GetText())

	props := s.GetProperties().GetFileProperties().AllPropertyItem()
	propKV := lo.SliceToMap(props, func(item parser.IPropertyItemContext) (string, string) {
		return TrimQuote(item.GetKey().GetText()), TrimQuote(item.GetValue().GetText())
	})
	c.CatalogProps = propKV
	logrus.Tracef("Catalog '%s' properties: %+v", c.CatalogName, c.CatalogProps)

	// get catalog type
	ty, ok := propKV["type"]
	if !ok {
		return nil, fmt.Errorf("failed to find 'type' property from catalog: %s", c.CatalogName)
	}
	c.CatalogType = CatalogType(ty)
	if c.CatalogType == "hms" {
		_, cols, err := parser.GetTableNameAndCols(c.CatalogName+".db.table", createTableStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse create table statement:\n%s", createTableStmt)
		}

		c.CatalogType = CatalogTypeHive
		_, isHudiTable := lo.Find(cols, func(col string) bool { return isHudiMetaCol(col) })
		if isHudiTable {
			c.CatalogType = CatalogTypeHudi
		}
	}
	logrus.Debugf("Detected catalog '%s' type: %s", c.CatalogName, c.CatalogType)

	// get warehouse
	if warehouse, ok := propKV["warehouse"]; ok {
		c.warehouse = warehouse
	}

	// get hive metastore uri
	if hiveMetastoreUri, ok := propKV["hive.metastore.uris"]; ok {
		c.HiveMetastoreUri = c.replaceLocalUri(strings.SplitN(hiveMetastoreUri, ",", 2)[0])
	}

	// get storage akkey/sk + endpoint
	akkey, ok := lo.FindKeyBy(propKV, func(k, _ string) bool {
		suf, ok := strings.CutSuffix(k, ".access_key")
		return ok && slices.Contains([]string{"s3", "oss", "cos", "obs", "gs", "client.credentials-provider"}, suf)
	})
	if ok {
		c.storageAk = propKV[akkey]

		akPrefix := strings.TrimSuffix(akkey, ".access_key")
		if sk, ok := propKV[akPrefix+".secret_key"]; ok {
			c.storageSk = sk
		}
		if storageSecretKey != "" || c.storageSk == "" || strings.HasPrefix(c.storageSk, "*XXX") {
			c.storageSk = storageSecretKey
		}
		if c.storageSk == "" {
			return nil, fmt.Errorf("secret key '%s.secret_key' is required, you may want to set '--storage-secret-key'", akPrefix)
		}

		c.storageProviderSchema = strings.TrimPrefix(akPrefix, "client.credentials-provider")
		if endpoint, ok := propKV[c.storageProviderSchema+".endpoint"]; ok {
			c.storageEndpoint = endpoint
		}

		c.storageRegion = propKV[akPrefix+".region"]
		if c.storageRegion == "" {
			c.storageRegion = "fake-region"
		}
	}

	// download keytabs and krb5.conf from fe if specified
	if err := c.fetchKerberosFiles(ctx); err != nil {
		return nil, err
	}

	sparkVersion, err := getSparkVersion(ctx)
	if err != nil {
		return nil, err
	}
	c.sparkVersion = sparkVersion

	return c, nil
}

func (c *SparkCli) RunImportData(ctx context.Context, pySnippet, db, table, csvDirOrFile string, columnSeparator string, additionalParams ...string) error {
	cmd, err := c.GetCmd(additionalParams...)
	if err != nil {
		return err
	}
	subprocess := exec.CommandContext(ctx, "bash", "-ec", cmd)
	subprocess.Stdin = strings.NewReader(pySnippet)
	subprocess.Stdout = os.Stdout
	subprocess.Stderr = os.Stderr

	// pass env vars
	subprocess.Env = append(
		os.Environ(),
		fmt.Sprintf("DODO_TABLE_ID=%s.%s.%s", c.CatalogName, db, table),
		fmt.Sprintf("DODO_CSV_PATH=%s", csvDirOrFile),
		fmt.Sprintf("DODO_CSV_DELIMITER=%s", columnSeparator),
	)

	logrus.Debugf("%s", cmd)

	// run cmd
	return subprocess.Run()
}

func (c *SparkCli) GetCmd(additionalParams ...string) (string, error) {
	// ======================
	// 1. Metastore config
	// ======================

	var (
		cmd                  = fmt.Sprintf("pyspark -c spark.sql.defaultCatalog=%s --master='local[%d]'", c.CatalogName, c.parallel)
		packages             = []string{}
		extraDriverJavaOps   string
		extraExecutorJavaOps string
	)
	cmd += ` -c spark.shuffle.file.buffer=32k -c spark.sql.cbo.enabled=true -c spark.sql.cbo.joinReorder.enabled=true -c spark.sql.catalogImplementation=in-memory -c spark.ui.enabled=false -c spark.hadoop.fs.s3a.fast.upload=true -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.legacy.charVarcharAsString=true`
	cmd += ` -c spark.hadoop.hive.exec.dynamic.partition=true -c spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict -c spark.sql.sources.partitionOverwriteMode=dynamic -c spark.sql.optimizer.dynamicPartitionPruning.useStats=true -c spark.sql.sources.partitionOverwriteMode=dynamic`
	// Partition size: -c spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB -c spark.sql.files.maxPartitionBytes=62914560

	// dlf
	cmd, packages = c.withDLF(cmd, packages)

	switch c.CatalogType {
	case CatalogTypeHudi:
		cmd += " -c spark.serializer=org.apache.spark.serializer.KryoSerializer -c spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension -c spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"
		packages = append(packages, fmt.Sprintf("org.apache.hudi:hudi-spark%s-bundle_2.12:0.15.0", c.sparkVersion))
		fallthrough
	case CatalogTypeHive:
		ty := c.CatalogProps["hive.metastore.type"]
		if ty == "" {
			ty = "hms"
		}
		switch ty {
		case "hms":
			cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.type=%s -c spark.hadoop.hive.metastore.uris=%s",
				c.CatalogName, "hive",
				c.HiveMetastoreUri,
			)
		case "dlf":
			// no-op, already handled in WithDLF
		default:
			return "", fmt.Errorf("unsupported hive catalog type: '%s'", ty)
		}
	case CatalogTypeIceberg:
		cmd += fmt.Sprintf(" -c spark.sql.catalog.%s=org.apache.iceberg.spark.SparkCatalog -c spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			c.CatalogName,
		)
		packages = append(packages, "org.apache.iceberg:iceberg-spark-runtime-"+c.sparkVersion+"_2.12:1.10.0")

		// Doris: https://doris.apache.org/docs/3.0/lakehouse/catalogs/iceberg-catalog?_highlight=iceberg#syntax
		// Spark: https://iceberg.apache.org/docs/1.5.0/spark-configuration
		ty := c.CatalogProps["iceberg.catalog.type"]
		switch ty {
		case "hms":
			cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.type=%s -c spark.sql.catalog.%s.uri=%s",
				c.CatalogName, "hive",
				c.CatalogName, c.HiveMetastoreUri,
			)
		case "rest":
			restUri, ok := c.CatalogProps["iceberg.rest.uri"]
			if !ok {
				return "", fmt.Errorf("uri is required for iceberg rest catalog '%s'", c.CatalogName)
			}
			cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.type=%s -c spark.sql.catalog.%s.uri=%s",
				c.CatalogName, "rest",
				c.CatalogName, restUri,
			)
		case "hadoop":
			cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.type=%s", c.CatalogName, "hadoop")
			packages = append(packages, "org.apache.iceberg:iceberg-aws-bundle:1.5.2")
		case "dlf":
			// no-op, already handled in WithDLF
			// cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.type=%s", s.CatalogName, "hive")
		default:
			return "", fmt.Errorf("unsupported iceberg catalog type: '%s'", ty)
		}
	case CatalogTypePaimon:
		// https://doris.apache.org/zh-CN/docs/dev/lakehouse/catalogs/paimon-catalog
		cmd += " -c spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
		packages = append(packages, fmt.Sprintf("org.apache.paimon:paimon-spark%s:1.0.1,org.apache.paimon:paimon-s3:1.0.1", c.sparkVersion))
		ty := c.CatalogProps["paimon.catalog.type"]
		switch ty {
		case "hms", "dlf":
		default:
			return "", fmt.Errorf("unsupported paimon catalog type: '%s'", ty)
		}
	default:
		return "", fmt.Errorf("unsupported catalog type: %s", c.CatalogType)
	}

	// ======================
	// 2. Storage config
	// ======================

	// s3 config
	// HACK: use s3 protocol for oss/cos/obs/gs/minio
	if slices.Contains([]string{"oss", "cos", "obs", "gs", "minio"}, c.storageProviderSchema) {
		c.storageProviderSchema = "s3"
		if !strings.HasPrefix(c.warehouse, "s3://") {
			c.warehouse = strings.Replace(c.warehouse, fmt.Sprintf("%s://", c.storageProviderSchema), "s3://", 1)
		}
	}
	if c.storageProviderSchema == "s3" {
		if c.storageEndpoint == "" {
			switch c.storageProviderSchema {
			case "s3":
				c.storageEndpoint = "s3." + c.CatalogProps["s3.region"] + ".amazonaws.com"
			case "oss":
				c.storageEndpoint = "oss-" + c.CatalogProps["oss.region"] + ".aliyuncs.com"
			case "cos":
				c.storageEndpoint = "cos." + c.CatalogProps["cos.region"] + ".myqcloud.com"
			case "obs":
				c.storageEndpoint = "obs." + c.CatalogProps["obs.region"] + ".myhuaweicloud.com"
			case "gs":
				c.storageEndpoint = "storage.googleapis.com"
			// case "minio":
			// no-op
			default:
			}
		}

		// https://medium.com/@Shamimw/connect-to-aws-s3-and-read-files-using-apache-spark-186943a5169a
		cmd += " -c spark.hadoop.fs.s3a.connection.maximum=1000 -c spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem -c spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider -c spark.hadoop.fs.s3a.fast.upload=true"
		endpoint := c.storageEndpoint
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			endpoint = "http://" + endpoint
		}
		cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.s3.endpoint=%s", c.CatalogName, c.replaceLocalUri(endpoint))
		cmd += fmt.Sprintf(" -c spark.hadoop.fs.s3a.endpoint=%s", c.replaceLocalUri(c.storageEndpoint))
		cmd += fmt.Sprintf(" -c spark.hadoop.fs.s3a.access.key=%s", c.storageAk)
		cmd += fmt.Sprintf(" -c spark.hadoop.fs.s3a.secret.key=%s", c.storageSk)
		awsOps := fmt.Sprintf(" -Daws.accessKeyId=%s -Daws.secretAccessKey=%s", c.storageAk, c.storageSk)
		extraDriverJavaOps += awsOps
		extraExecutorJavaOps += awsOps
		if c.storageRegion != "" {
			extraDriverJavaOps += fmt.Sprintf(" -Daws.region=%s ", c.storageRegion)
			extraExecutorJavaOps += fmt.Sprintf(" -Daws.region=%s ", c.storageRegion)
		}
		if c.CatalogType == CatalogTypeIceberg {
			cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.io-impl=org.apache.iceberg.aws.s3.S3FileIO", c.CatalogName)
		}
		packages = append(
			packages,
			"org.apache.hadoop:hadoop-client:3.3.2",
			"org.apache.hadoop:hadoop-aws:3.3.2",
			"com.amazonaws:aws-java-sdk-bundle:1.12.262",
		)
	}
	// set warehouse
	if c.warehouse != "" {
		cmd += fmt.Sprintf(" -c spark.sql.warehouse.dir=%s", c.replaceLocalUri(c.warehouse))
		cmd += fmt.Sprintf(" -c spark.sql.catalog.%s.warehouse=%s", c.CatalogName, c.replaceLocalUri(c.warehouse))
	}

	// ======================
	// 3. Other configs
	// ======================

	// others hive/hadoop properties
	cmd = c.withInheritProps(cmd)

	// set proxy for to java opts
	httpsProxy := os.Getenv("https_proxy")
	if httpsProxy != "" {
		proxyUrl, err := url.Parse(httpsProxy)
		if err != nil {
			return "", fmt.Errorf("failed to parse https_proxy %s: %v", httpsProxy, err)
		}
		extraDriverJavaOps += fmt.Sprintf("-Dhttps.proxyHost=%s -Dhttps.proxyPort=%s ", proxyUrl.Hostname(), proxyUrl.Port())
	}
	// set kerberos to java opts
	if c.localKrb5conf != "" {
		krbconf := fmt.Sprintf(" -Djava.security.krb5.conf=%s -Dsun.security.krb5.debug=true", c.localKrb5conf)
		extraDriverJavaOps += krbconf
		extraExecutorJavaOps += krbconf
	}
	// set extra java opts
	if extraDriverJavaOps != "" {
		cmd += fmt.Sprintf(" -c spark.driver.extraJavaOptions='%s'", extraDriverJavaOps)
	}
	if extraExecutorJavaOps != "" {
		cmd += fmt.Sprintf(" -c spark.executor.extraJavaOptions='%s'", extraExecutorJavaOps)
	}

	// set packages
	cmd += fmt.Sprintf(" --packages %s", strings.Join(packages, ","))

	// add additionalParams
	cmd += " " + strings.Join(additionalParams, " ")

	return strings.TrimSpace(cmd), nil
}

func (c *SparkCli) withInheritProps(cmd string) string {
	newcmd := cmd
	// inherit other catalog properties as spark.hadoop.*
	// e.g. hadoop.fs.s3a.connection.ssl.enabled=false
	for k, v := range c.CatalogProps {
		if after, ok := strings.CutPrefix(k, "hadoop."); ok {
			newcmd += fmt.Sprintf(" -c spark.hadoop.%s=%s", after, v)
		} else if after, ok := strings.CutPrefix(k, "hive."); ok {
			newcmd += fmt.Sprintf(" -c spark.sql.hive.%s=%s", after, v)
		}
	}
	if defaultFS, ok := c.CatalogProps["fs.defaultFS"]; ok {
		newcmd += fmt.Sprintf(" -c spark.hadoop.fs.defaultFS=%s", c.replaceLocalUri(defaultFS))
	}
	return newcmd
}

func (c *SparkCli) withDLF(cmd string, packages []string) (string, []string) {
	if c.CatalogProps["dlf.catalog_id"] == "" {
		return cmd, packages
	}

	// https://www.alibabacloud.com/help/zh/emr/emr-on-ecs/user-guide/build-a-debugging-environment-for-spark-on-an-on-premises-machine#p-cdx-1rk-yih
	if c.storageEndpoint == "" {
		// construct from region
		region := c.CatalogProps["dlf.region"]
		c.storageEndpoint = fmt.Sprintf("dlf.%s.aliyuncs.com", region)
	}
	newcmd := cmd + fmt.Sprintf(" -c spark.hadoop.hive.imetastoreclient.factory.class=com.aliyun.datalake.metastore.hive2.DlfMetaStoreClientFactory -c spark.hadoop.dlf.catalog.accessKeyId=%s -c spark.hadoop.dlf.catalog.accessKeySecret=%s -c spark.hadoop.dlf.catalog.endpoint=%s -c spark.hadoop.dlf.catalog.id=%s -c spark.hadoop.hive.metastore.warehouse.dir=%s",
		c.storageAk, c.storageSk,
		c.storageEndpoint,
		c.CatalogProps["dlf.catalog.id"],
		c.warehouse,
	)
	// add dlf client package
	newpkgs := append(packages, "com.aliyun.datalake:metastore-client-hive3:0.2.14")

	return newcmd, newpkgs
}

func (c *SparkCli) fetchKerberosFiles(ctx context.Context) error {
	var hasKeytab bool
	for _, k := range []string{"hive.metastore.kerberos.keytab", "hadoop.kerberos.keytab"} {
		if keytab, ok := c.CatalogProps[k]; ok {
			hasKeytab = true

			p := filepath.Join(c.cacheDir, "kerberos", c.CatalogName+"."+strings.Split(k, ".")[0]+".keytab")
			if _, err := os.Stat(p); err == nil {
				logrus.Infof("Keytab '%s' of catalog '%s' already exists, skip downloading again", p, c.CatalogName)
				c.CatalogProps[k] = p
				continue
			}
			err := ScpFromRemote(ctx, c.feSSHPrivKey, filepath.Join(c.feSSHEndpoint, keytab), p)
			if err != nil {
				logrus.Warnf("Failed to download '%s' from FE %s: %v", k, keytab, err)
				continue
			}
			c.CatalogProps[k] = p
			logrus.Infof("Downloaded '%s' from FE %s to %s", k, keytab, p)
		}
	}
	if !hasKeytab {
		return nil
	}

	fedir, err := ShowFronendsDisksDir(ctx, c.db, "deploy")
	if err != nil {
		return fmt.Errorf("failed to show frontend disks dir: %v", err)
	}

	// find the value of -D java.security.krb5.conf = xxx in fe.conf
	// TODO: also check fe_custom.conf?
	remoteFeconf := filepath.Join(fedir, "conf", "fe.conf")
	out, stderr, err := SshExec(c.feSSHPrivKey, c.feSSHEndpoint, `grep "java.security.krb5.conf" `+remoteFeconf+` | awk -F'-D' '{for(i=1;i<=NF;i++){if($i ~ /java.security.krb5.conf/){print $i}}}' | awk -F'=' '{print $2}' | awk -F'=' '{print $2}' | awk '{print $1}' | head -n1`)
	if err != nil || stderr != "" {
		out = "<unknown>"
	}

	// download krb5.conf
	remoteKrb5conf := strings.TrimSpace(out)
	c.localKrb5conf = filepath.Join(c.cacheDir, "kerberos", c.CatalogName+".krb5.conf")
	if _, err := os.Stat(c.localKrb5conf); err == nil {
		logrus.Infof("krb5.conf '%s' of catalog '%s' already exists, skip downloading again", c.localKrb5conf, c.CatalogName)
		return nil
	}
	// let user confirm before overwrite
	if out == "<unknown>" || !Confirm(fmt.Sprintf("Is %s the correct path for krb5.conf on FE", remoteKrb5conf)) {
		c.localKrb5conf, err = UserInput("Please enter the correct path for krb5.conf on FE:")
		if err != nil {
			return err
		}
	}
	err = ScpFromRemote(ctx, c.feSSHPrivKey, filepath.Join(c.feSSHEndpoint, remoteKrb5conf), c.localKrb5conf)
	if err != nil {
		logrus.Warnf("Failed to download 'krb5.conf' from FE %s: %v", remoteKrb5conf, err)
		c.localKrb5conf = remoteKrb5conf
	}
	return nil
}

func (c *SparkCli) replaceLocalUri(uri string) string {
	uri = ReplaceOSSUri(uri)

	u, err := url.Parse(uri)
	if err != nil {
		logrus.Fatalf("Invalid URI: %s", uri)
	}

	// 's3://' -> 's3a://'
	if u.Scheme == "s3" {
		u.Scheme = "s3a"
	}

	if u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" {
		u.Host = fmt.Sprintf("%s:%s", c.feIP, u.Port())
	}
	return u.String()
}

func getSparkVersion(ctx context.Context) (string, error) {
	out, err := RunCmd(ctx, "pyspark --version", false)
	if err != nil {
		return "", err
	}
	parts := strings.Fields(string(out))
	for i, part := range parts {
		if i+1 < len(parts) && strings.EqualFold(part, "version") {
			version := strings.TrimSpace(parts[i+1])
			// only keep `major.minor`
			versionParts := strings.Split(version, ".")
			if len(versionParts) > 2 {
				version = fmt.Sprintf("%s.%s", versionParts[0], versionParts[1])
			}

			return version, nil
		}
	}
	return "", fmt.Errorf("failed to get pyspark version: %s", string(out))
}
