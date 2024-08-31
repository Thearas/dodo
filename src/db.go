package src

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var (
	InternalSqlComment = "/*dodo*/"

	sqlLikeReplacer = strings.NewReplacer(
		`"`, `\"`,
		`_`, `\_`,
		`%`, `\%`,
	)
)

type SchemaType string

var (
	SchemaTypeTable            SchemaType = "TABLE"
	SchemaTypeView             SchemaType = "VIEW"
	SchemaTypeMaterializedView SchemaType = "MATERIALIZED_VIEW"

	AllSchemaTypes = []SchemaType{
		SchemaTypeTable,
		SchemaTypeView,
		SchemaTypeMaterializedView,
	}
)

func (s SchemaType) sanitize() SchemaType {
	switch s {
	case "BASE TABLE":
		return SchemaTypeTable
	case "VIEW":
		return SchemaTypeView
	default:
		logrus.Warnf("unknown schema type: %s", s)
		return SchemaType(strings.ReplaceAll(string(s), " ", "_"))
	}
}

func (s SchemaType) Lower() string {
	return strings.ToLower(string(s))
}

type DBSchema struct {
	Name    string        `yaml:"db"`
	Schemas []*Schema     `yaml:"-"`
	Stats   []*TableStats `yaml:"tables,omitempty"`
}

type Schema struct {
	Name       string     `db:"TABLE_NAME"`
	Type       SchemaType `db:"TABLE_TYPE"`
	DB         string     `db:"TABLE_SCHEMA"`
	CreateStmt string     `db:"-"`
}

func (s *Schema) String() string {
	return fmt.Sprintf("%s.%s", s.DB, s.Name)
}

type TableStats struct {
	Name     string         `yaml:"name"`
	RowCount int64          `yaml:"row_count"`
	Columns  []*ColumnStats `yaml:"columns,omitempty"`
}

func (ts *TableStats) ColumnStats() map[string]*ColumnStats {
	return lo.SliceToMap(ts.Columns, func(s *ColumnStats) (string, *ColumnStats) {
		s.Count = ts.RowCount
		return s.Name, s
	})
}

type ColumnStats struct {
	Name        string `yaml:"name"`
	Count       int64  `yaml:"-"`
	Ndv         int64  `yaml:"ndv"`
	NullCount   int64  `yaml:"null_count"`
	DataSize    int64  `yaml:"data_size"`
	AvgSizeByte int64  `yaml:"avg_size_byte"`
	Min         string `yaml:"min"`
	Max         string `yaml:"max"`
	Method      string `yaml:"method"`
}

func NewDB(host string, port uint16, user, password, catalog, db string, ping ...bool) (*sqlx.DB, error) {
	if catalog != "" && catalog != "internal" {
		db = catalog + "." + db
	}

	cfg := &mysql.Config{
		User:                 user,
		Passwd:               password,
		Addr:                 net.JoinHostPort(host, strconv.Itoa(int(port))),
		Net:                  "tcp",
		DBName:               db,
		AllowNativePasswords: true,
		Timeout:              3 * time.Second,
		InterpolateParams:    true, // some doris does not enable prepare stmt
		ParseTime:            false,
		ReadTimeout:          600 * time.Second,
		WriteTimeout:         600 * time.Second,
		CheckConnLiveness:    false,
	}
	dsn := cfg.FormatDSN()
	logrus.Traceln("Connecting:", logrus.Fields{
		"Host": host,
		"Port": port,
		"User": user,
		"DB":   db,
	})
	if len(ping) > 0 && ping[0] {
		return sqlx.Connect("mysql", dsn)
	}
	return sqlx.Open("mysql", dsn)
}

func ShowCreateCatalog(ctx context.Context, conn *sqlx.DB, catalog string) (string, error) {
	if catalog == "" {
		catalog = "internal"
	}

	r, err := conn.QueryxContext(ctx, fmt.Sprintf(InternalSqlComment+"SHOW CREATE CATALOG `%s`", catalog))
	if err != nil {
		return "", fmt.Errorf("SHOW CREATE CATALOG %s failed: %v", catalog, err)
	}
	defer r.Close()

	if !r.Next() {
		return "", fmt.Errorf("no rows returned from SHOW CREATE CATALOG for catalog: %s", catalog)
	}

	result := map[string]any{}
	if err := r.MapScan(result); err != nil {
		return "", err
	}

	createStmt := cast.ToString(result["CreateCatalog"])
	return createStmt, r.Err()
}

//nolint:revive
func ShowCreateTables(ctx context.Context, conn *sqlx.DB, ignoreView bool, db string, tables ...string) (schemas []*Schema, err error) {
	schemas_, err := ShowTables(ctx, conn, ignoreView, db)
	if err != nil {
		return nil, err
	}
	tables_ := lo.Map(schemas_, func(s *Schema, _ int) string { return s.Name })
	logrus.Debugln("found tables:", tables_)

	schemas = schemas_

	// filter tables
	if len(tables) > 0 {
		schemas = make([]*Schema, 0, len(tables))
		for _, t := range tables {
			if !strings.Contains(t, ".") {
				t = db + "." + t
			}
			schema, find := lo.Find(schemas_, func(s *Schema) bool { return s.String() == t })
			if !find {
				return nil, fmt.Errorf("table %s not found in %s", t, db)
			}
			schemas = append(schemas, schema)
		}
	}

	for _, s := range schemas {
		createStmt, isMaterializedView, err := showCreateTable(ctx, conn, db, s.Name, ignoreView)
		if err != nil {
			return nil, err
		}

		s.CreateStmt = createStmt
		if isMaterializedView {
			s.Type = SchemaTypeMaterializedView
		}
	}

	return
}

//nolint:revive
func showCreateTable(ctx context.Context, conn *sqlx.DB, db, table string, ignoreView bool) (schema string, isMaterializedView bool, err error) {
	r, err := conn.QueryxContext(ctx, fmt.Sprintf(InternalSqlComment+"SHOW CREATE TABLE `%s`.`%s`", db, table))
	if err != nil {
		if ignoreView {
			return "", false, nil
		}
		// may be a materialized view
		var err_ error
		r, err_ = conn.QueryxContext(ctx, fmt.Sprintf(InternalSqlComment+"SHOW CREATE MATERIALIZED VIEW `%s`.`%s`", db, table))
		if err_ != nil {
			return "", false, err
		}
		isMaterializedView = true
	}
	defer r.Close()

	logrus.Debugln("show create table:", table)

	schema, err = getStmtfromShowCreate(r)
	if err != nil {
		return "", false, err
	}

	// logrus.Traceln("create table:", schema)

	return
}

func getStmtfromShowCreate(r *sqlx.Rows) (schema string, err error) {
	cols, err := r.Columns()
	if err != nil {
		return "", err
	}
	vals := lo.ToAnySlice(lo.ToSlicePtr(make([]string, len(cols))))

	for r.Next() {
		err := r.Scan(vals...)
		if err != nil {
			return "", err
		}
		// the second column is the create statement
		schema = *vals[1].(*string)
	}
	if err := r.Err(); err != nil {
		return schema, err
	}

	return
}

func ShowCatalogs(ctx context.Context, conn *sqlx.DB, namePrefix string) ([]string, error) {
	r, err := conn.QueryxContext(ctx, InternalSqlComment+`SHOW CATALOGS LIKE ?`, SanitizeLike(namePrefix)+"%")
	if err != nil {
		return nil, err
	}
	defer r.Close()

	catalogs := []string{}
	for r.Next() {
		catalog := map[string]any{}
		if err := r.MapScan(catalog); err != nil {
			return nil, err
		}
		// cobra.CompDebug(fmt.Sprintln("asdadad", catalog), true)
		catalogs = append(catalogs, cast.ToString(catalog["CatalogName"]))
	}

	return catalogs, r.Err()
}

func ShowDatabases(ctx context.Context, conn *sqlx.DB, dbnamePrefix string) ([]string, error) {
	dbs := []string{}
	err := conn.SelectContext(ctx, &dbs, InternalSqlComment+`SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME not in ('__internal_schema', 'information_schema', 'mysql') AND SCHEMA_NAME like ? ORDER BY SCHEMA_NAME`, SanitizeLike(dbnamePrefix)+"%")
	if err != nil {
		return nil, err
	}
	return dbs, nil
}

//nolint:revive
func ShowTables(ctx context.Context, conn *sqlx.DB, ignoreView bool, dbname string, tablenamePrefix ...string) (tables []*Schema, err error) {
	tables = []*Schema{}
	conds := ""
	if ignoreView {
		conds += " AND TABLE_TYPE = 'BASE TABLE'"
	}
	if len(tablenamePrefix) > 0 {
		err = conn.SelectContext(ctx, &tables, InternalSqlComment+`SELECT TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME like ? `+conds+` ORDER BY TABLE_NAME`, dbname, SanitizeLike(tablenamePrefix[0])+"%")
	} else {
		err = conn.SelectContext(ctx, &tables, InternalSqlComment+`SELECT TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? `+conds+` ORDER BY TABLE_NAME`, dbname)
	}
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		t.Type = t.Type.sanitize()
	}
	return
}

// ShowColumns via DESCRIBE
func ShowColumns(ctx context.Context, conn *sqlx.DB, dbname, tablename string) (cols []*Column, err error) {
	logrus.Debugf("show columns: `%s`.`%s`", dbname, tablename)
	r, err := conn.QueryxContext(ctx, InternalSqlComment+fmt.Sprintf("DESCRIBE `%s`.`%s`", dbname, tablename))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for r.Next() {
		vals, err := r.SliceScan()
		if err != nil {
			return nil, err
		}

		col := new(Column)
		col.Name = cast.ToString(vals[0])
		p := parser.NewParser(fmt.Sprintf("%s.%s", tablename, col.Name), cast.ToString(vals[1]))
		col.Type = p.DataType()
		if p.ErrListener.LastErr != nil {
			return nil, fmt.Errorf("parse type generator failed for column '%s', err: %v", col.Name, p.ErrListener.LastErr)
		}
		col.Nullable = strings.EqualFold(cast.ToString(vals[2]), "Yes")
		// col.Default = cast.ToString(vals[4])
		cols = append(cols, col)
	}

	return cols, r.Err()
}

func ShowBackendCount(ctx context.Context, conn *sqlx.DB) (count int, err error) {
	r, err := conn.QueryxContext(ctx, InternalSqlComment+"SHOW BACKENDS")
	if err != nil {
		return 0, err
	}
	defer r.Close()

	for r.Next() {
		_, err := r.SliceScan()
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, r.Err()
}

func ShowFronendsDisksDir(ctx context.Context, conn *sqlx.DB, dirType string) (dir string, err error) {
	r, err := conn.QueryxContext(ctx, InternalSqlComment+"show frontends DISKS")
	if err != nil {
		return "", err
	}
	defer r.Close()

	cols, err := r.Columns()
	if err != nil {
		return "", err
	}
	colDirTypeIdx := lo.IndexOf(cols, "DirType")
	colDirIdx := lo.IndexOf(cols, "Dir")
	vals := lo.ToAnySlice(lo.ToSlicePtr(make([]string, len(cols))))

	for r.Next() {
		err := r.Scan(vals...)
		if err != nil {
			return "", err
		}

		if *vals[colDirTypeIdx].(*string) == dirType {
			dir = *vals[colDirIdx].(*string)
			break
		}
	}

	return dir, r.Err()
}

func exportTable(ctx context.Context, conn *sqlx.DB, dbname, table, target, toURL string, with, props map[string]string) error {
	strKV := func(k string, v string) string {
		if !strings.HasPrefix(k, `"`) && !strings.HasSuffix(k, `'`) {
			k = string(MustJsonMarshal(strings.TrimSpace(k)))
		}
		if !strings.HasPrefix(v, `"`) && !strings.HasSuffix(v, `'`) {
			v = string(MustJsonMarshal(strings.TrimSpace(v)))
		}
		return fmt.Sprintf("  %s = %s", k, v)
	}

	stmt := fmt.Sprintf("EXPORT TABLE `%s`.`%s` TO '%s'\nPROPERTIES (\n%s\n)\nWITH %s (\n%s\n);",
		dbname, table, toURL,
		strings.Join(lo.MapToSlice(props, strKV), ",\n"),
		strings.ToUpper(target),
		strings.Join(lo.MapToSlice(with, strKV), ",\n"),
	)

	_, err := conn.ExecContext(ctx, InternalSqlComment+stmt)
	return err
}

func showExportTable(ctx context.Context, conn *sqlx.DB, dbname string, label string) (completed bool, progress string, err error) {
	r, err := conn.QueryxContext(ctx, InternalSqlComment+fmt.Sprintf("SHOW EXPORT FROM `%s` WHERE Label = '%s' ORDER BY CreateTime desc LIMIT 1", dbname, label))
	if err != nil {
		return false, "", err
	}
	defer r.Close()
	if !r.Next() {
		return false, "", fmt.Errorf("no rows returned from SHOW EXPORT, db: %s, label: %s", dbname, label)
	}

	vals := map[string]any{}
	if err := r.MapScan(vals); err != nil {
		return false, "", err
	}

	// https://doris.apache.org/docs/sql-manual/sql-statements/data-modification/load-and-export/SHOW-EXPORT#return-value
	state := cast.ToString(vals["State"])
	progress = cast.ToString(vals["Progress"])
	errMsg := cast.ToString(vals["ErrorMsg"])
	if state == "CANCELLED" || errMsg != "" {
		return false, "", fmt.Errorf("export failed: %s", errMsg)
	}
	return state == "FINISHED", progress, nil
}

func cancelExportTable(ctx context.Context, conn *sqlx.DB, dbname string, label string) error {
	_, err := conn.ExecContext(ctx, InternalSqlComment+fmt.Sprintf("CANCEL EXPORT FROM `%s` WHERE Label = '%s'", dbname, label))
	return err
}

//nolint:revive
func GetTablesStats(ctx context.Context, conn *sqlx.DB, analyze bool, dbname string, tables ...string) ([]*TableStats, error) {
	if len(tables) == 0 {
		return []*TableStats{}, nil
	}

	stats := make([]*TableStats, 0, len(tables))
	for _, table := range tables {
		if analyze {
			if err := analyzeTableSync(ctx, conn, dbname, table); err != nil {
				return nil, err
			}
		}

		s, err := getTableStats(ctx, conn, dbname, table)
		if err != nil {
			logrus.Warnf("get table stats failed: db: %s, table: %s, err: %v", dbname, table, err)
			return nil, err
		}
		if s == nil {
			continue
		}
		stats = append(stats, s)
	}

	return stats, nil
}

func analyzeTableSync(ctx context.Context, conn *sqlx.DB, dbname, table string) error {
	logrus.Debugf("analyzing table `%s`.`%s` with sync", dbname, table)

	r, err := conn.QueryxContext(ctx, InternalSqlComment+fmt.Sprintf("ANALYZE TABLE `%s`.`%s` WITH SYNC", dbname, table))
	if err != nil {
		return fmt.Errorf("analyze table `%s`.`%s` failed, err: %v", dbname, table, err)
	}
	defer r.Close()
	return nil
}

// describeColumnBaseTypes returns a map of column name -> base type (e.g. "VARCHAR", "INT", "DATETIME")
// by running DESCRIBE on the table. The base type is extracted by uppercasing and trimming
// anything after '(' (e.g. "varchar(255)" -> "VARCHAR").
func describeColumnBaseTypes(ctx context.Context, conn *sqlx.DB, dbname, table string) (map[string]string, error) {
	r, err := conn.QueryxContext(ctx, InternalSqlComment+fmt.Sprintf("DESCRIBE `%s`.`%s`", dbname, table))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	colTypes := map[string]string{}
	for r.Next() {
		vals, err := r.SliceScan()
		if err != nil {
			return nil, err
		}
		colName := cast.ToString(vals[0])
		rawType := strings.ToUpper(cast.ToString(vals[1]))
		if idx := strings.IndexByte(rawType, '('); idx != -1 {
			rawType = rawType[:idx]
		}
		colTypes[colName] = rawType
	}
	return colTypes, r.Err()
}

// isMinMaxSafeType returns true if the column type is numeric or datetime,
// whose min/max values are safe to output as-is (no garbled characters or sensitive data).
func isMinMaxSafeType(baseType string) bool {
	return baseType == "BOOLEAN" || (IsIntegerType(baseType) || IsFloatType(baseType) || IsTimeType(baseType))
}

// maskMinMax replaces a stats value with '*' of the same length.
func maskMinMax(val string) string {
	return strings.Repeat("*", len(val))
}

func getTableStats(ctx context.Context, conn *sqlx.DB, dbname, table string) (*TableStats, error) {
	logrus.Debugln("get table stats:", table)

	// get column types via DESCRIBE to determine which min/max values need masking
	colTypes, err := describeColumnBaseTypes(ctx, conn, dbname, table)
	if err != nil {
		logrus.Debugf("describe table `%s`.`%s` failed, skip min/max masking: %v", dbname, table, err)
	}

	// show all column stats of table.
	r, err := conn.QueryxContext(ctx, InternalSqlComment+fmt.Sprintf("SHOW COLUMN STATS `%s`.`%s`", dbname, table))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	cols := []*ColumnStats{}
	for r.Next() {
		vals := map[string]any{}
		if err := r.MapScan(vals); err != nil {
			return nil, err
		}

		minVal, maxVal := vals["min"].([]byte), vals["max"].([]byte)
		if bytes.HasPrefix(minVal, []byte(`'`)) {
			minVal = bytes.ReplaceAll(minVal[1:len(minVal)-1], []byte(`''`), []byte(`'`))
		}
		if bytes.HasPrefix(maxVal, []byte(`'`)) {
			maxVal = bytes.ReplaceAll(maxVal[1:len(maxVal)-1], []byte(`''`), []byte(`'`))
		}

		colName := cast.ToString(vals["column_name"])
		minStr, maxStr := string(minVal), string(maxVal)

		// mask min/max for non-numeric/non-datetime columns to avoid
		// garbled characters (e.g. emoji) and provide data masking
		if baseType, ok := colTypes[colName]; ok && !isMinMaxSafeType(baseType) {
			minStr = maskMinMax(minStr)
			maxStr = maskMinMax(maxStr)
		}

		method, ok := vals["method"]
		if !ok {
			method = ""
		}
		cols = append(cols, &ColumnStats{
			Name:        colName,
			Count:       int64(cast.ToFloat64((string(vals["count"].([]byte))))),
			Ndv:         int64(cast.ToFloat64((string(vals["ndv"].([]byte))))),
			NullCount:   int64(cast.ToFloat64((string(vals["num_null"].([]byte))))),
			AvgSizeByte: int64(cast.ToFloat64((string(vals["avg_size_byte"].([]byte))))),
			DataSize:    int64(cast.ToFloat64((string(vals["data_size"].([]byte))))),
			Min:         minStr,
			Max:         maxStr,
			Method:      cast.ToString(method),
		})
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		logrus.Warnf("no column stats found for %s.%s", dbname, table)
		return nil, nil
	}

	tbl := &TableStats{
		Name:     table,
		RowCount: cols[0].Count,
		Columns:  cols,
	}
	return tbl, nil
}

func CountAuditlogs(
	ctx context.Context,
	db *sqlx.DB,
	dbname, table string,
	opts AuditLogScanOpts,
	hasCatalogColumn bool,
) (int, error) {
	query := fmt.Sprintf("SELECT count(*) FROM `%s`.`%s` WHERE %s", dbname, table, opts.sqlConditions(hasCatalogColumn))
	logrus.Traceln("query from audit log table:", query)

	var total int
	err := db.GetContext(ctx, &total, InternalSqlComment+query)
	if err != nil {
		logrus.Errorln("query audit log count failed, err:", err)
	}
	return total, err
}

func auditLogHasColumn(ctx context.Context, db *sqlx.DB, dbname, table, column string) (bool, error) {
	cols, err := ShowColumns(ctx, db, dbname, table)
	if err != nil {
		return false, err
	}

	for _, col := range cols {
		if strings.EqualFold(col.Name, column) {
			return true, nil
		}
	}

	return false, nil
}

func GetDBAuditLogs(
	ctx context.Context,
	w SqlWriter,
	db *sqlx.DB,
	dbname, table string,
	opts AuditLogScanOpts,
	parallel int,
) (int, error) {
	hasCatalogColumn := false
	var err error
	if opts.Catalog != "" {
		hasCatalogColumn, err = auditLogHasColumn(ctx, db, dbname, table, "catalog")
		if err != nil {
			return 0, err
		}
		if !hasCatalogColumn {
			logrus.Warnf("audit log table `%s`.`%s` has no catalog column, skip catalog filter", dbname, table)
		}
	}

	total, err := CountAuditlogs(ctx, db, dbname, table, opts, hasCatalogColumn)
	if err != nil {
		return 0, err
	}
	if total <= 0 {
		logrus.Warnln("no audit log found")
		return 0, nil
	}
	if total > 1_000_000 {
		if !Confirm(fmt.Sprintf("Audit log count(%d) may be bigger than 1 million, continue", total)) {
			return 0, nil
		}
	}

	logrus.Debugf("need to scan %d audit log row(s)", total)

	if parallel > total {
		parallel = total
	}

	logScans := make([]*AuditLogScanner, parallel)
	for i := range logScans {
		s := NewAuditLogScanner(opts)
		logScans[i] = s
	}

	var (
		g              = ParallelGroup(parallel)
		perThreadCount = total / parallel
		conditions     = opts.sqlConditions(hasCatalogColumn)
		count          = 0

		outputThread = &atomic.Int32{}
		outputLock   = new(sync.Mutex)
		outputCond   = sync.NewCond(outputLock)
	)
	for i, logScan := range logScans {
		start, end := i*perThreadCount, (i+1)*perThreadCount
		if i == len(logScans)-1 {
			end = total
		}

		g.Go(func() error {
			const limitPerSelect = 100

			pageConds := ""
			for offset := start; offset < end; offset += limitPerSelect {
				limit := limitPerSelect

				overflow := offset + limit - end
				if overflow > 0 {
					limit -= overflow
				}

				offset_ := offset
				if pageConds != "" {
					offset_ = 0
				}

				time, queryId, err := getDBAuditLogsWithConds(ctx, logScan, db, dbname, table, conditions+pageConds, limit, offset_)
				if err != nil {
					return err
				}
				// next page with bigger `time` or with same `time` and bigger query_id
				pageConds = fmt.Sprintf(" AND (`time` > '%s' OR (`time` = '%s' AND query_id > '%s'))", time, time, queryId)

				if int(outputThread.Load()) == i {
					count_, err := logScan.Consume(w)
					if err != nil {
						return err
					}
					count += count_
				}
			}

			// write to file immediately to avoid using too much memory
			outputLock.Lock()
			defer outputLock.Unlock()
			for int(outputThread.Load()) != i {
				outputCond.Wait()
			}
			count_, err := logScan.Consume(w)
			if err != nil {
				return err
			}
			count += count_
			outputThread.Add(1)
			outputCond.Broadcast()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}

	return count, nil
}

func getDBAuditLogsWithConds(
	ctx context.Context,
	logScan *AuditLogScanner,
	db *sqlx.DB,
	dbname, table string,
	conditions string,
	limit, offset int,
) (lastTime string, lastQueryId string, err error) {
	const maxRetry = 5
	for retry := range maxRetry {
		stmt := fmt.Sprintf("SELECT `time`, client_ip, user, db, query_time, query_id, stmt, is_query, state FROM `%s`.`%s` WHERE %s ORDER BY `time`, query_id LIMIT %d OFFSET %d",
			dbname,
			table,
			conditions,
			limit,
			offset,
		)
		logrus.Traceln("query audit log:", stmt)

		var r *sqlx.Rows
		r, err = db.QueryxContext(ctx, InternalSqlComment+stmt)
		if err != nil {
			logrus.Errorf("query audit log table failed: retry: %d, db: %s, table: %s, err: %v", retry, dbname, table, err)
			continue
		}
		defer r.Close() //nolint:revive

		var i int
		for ; r.Next(); i++ {
			vals_, err := r.SliceScan()
			if err != nil {
				break
			}

			vals, err := cast.ToStringSliceE(vals_)
			if err != nil {
				logrus.Errorf("read audit log table failed: db: %s, table: %s, err: %v", dbname, table, err)
				break
			}
			lastTime, lastQueryId = vals[0], vals[5]

			logScan.onMatch([9]string(vals), "", true)
		}

		// prepare limit/offset for next retry
		limit -= i
		offset += i

		_ = r.Close()
		if err != nil {
			continue
		} else if err = r.Err(); err == nil {
			break
		}
	}

	return
}

func SanitizeLike(s string) string {
	return sqlLikeReplacer.Replace(s)
}
