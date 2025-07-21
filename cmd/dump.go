/*
Copyright © 2024 Thearas thearas850@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Thearas/dodo/src"
)

var DumpConfig = Dump{}

type Dump struct {
	AuditLogPaths []string
	AuditLogTable string

	OutputDDLDir          string
	OutputQueryDir        string
	LocalAuditLogCacheDir string
	AuditLogEncoding      string

	SSHAddress    string
	SSHPassword   string
	SSHPrivateKey string

	DumpSchema         bool
	DumpStats          bool
	DumpQuery          bool
	QueryMinDuration_  time.Duration
	QueryMinDurationMs int64
	QueryStates        []string
	OnlySelect         bool
	Strict             bool
	From, To           string
	Analyze            bool

	Clean bool
}

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump schema and query for Doris",
	Long: `
Dump schema from DB and query from audit-log.

You may want to pass config by '$HOME/.dodo.yaml',
or environment variables with prefix 'DORIS_', e.g.
    DORIS_HOST=xxx
    DORIS_PORT=9030
	`,
	Aliases:          []string{"d"},
	Example:          "dodo dump --dump-schema --dump-query -dbs db1 --audit-logs /path/to/audit.log",
	TraverseChildren: true,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := completeDumpConfig(); err != nil {
			return err
		}

		if DumpConfig.Clean {
			if err := cleanFile(DumpConfig.OutputDDLDir, true); err != nil {
				return err
			}
			if err := cleanFile(DumpConfig.OutputQueryDir, true); err != nil {
				return err
			}
		}

		if AnonymizeConfig.Enabled {
			SetupAnonymizer()
		}

		// dump schemas
		if DumpConfig.DumpSchema {
			schemas, err := dumpSchemas(ctx)
			if err != nil {
				return err
			}

			logrus.Infof("Found %d schema(s)\n", lo.SumBy(schemas, func(s *src.DBSchema) int { return len(s.Schemas) }))

			if err := outputSchemas(schemas); err != nil {
				return err
			}
		}

		// dump queries
		if DumpConfig.DumpQuery {
			count, err := dumpQueries(ctx)
			if err != nil {
				return err
			}

			logrus.Infof("Found %d query(s)\n", count)
		}

		// store anonymize hash dict
		if AnonymizeConfig.Enabled {
			src.StoreMiniHashDict(AnonymizeConfig.Method, AnonymizeConfig.HashDictPath)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)
	dumpCmd.PersistentFlags().SortFlags = false
	dumpCmd.Flags().SortFlags = false

	pFlags := dumpCmd.PersistentFlags()
	pFlags.BoolVar(&DumpConfig.DumpSchema, "dump-schema", false, "Dump schema")
	pFlags.BoolVar(&DumpConfig.DumpStats, "dump-stats", true, "Dump schema stats, only take effect when '--dump-schema=true'")
	pFlags.BoolVar(&DumpConfig.DumpQuery, "dump-query", false, "Dump query from audit log")
	pFlags.DurationVar(&DumpConfig.QueryMinDuration_, "query-min-duration", 0, "Dump queries which execution duration is greater than or equal to")
	pFlags.StringSliceVar(&DumpConfig.QueryStates, "query-states", []string{}, "Dump queries with states, like 'ok', 'eof' and 'err'")
	pFlags.BoolVar(&DumpConfig.OnlySelect, "only-select", true, "Only dump SELECT queries")
	pFlags.BoolVarP(&DumpConfig.Strict, "strict", "s", false, "Filter out sqls that can't be parsed")
	pFlags.StringVar(&DumpConfig.From, "from", "", "Dump queries from this time, like '2006-01-02 15:04:05'")
	pFlags.StringVar(&DumpConfig.To, "to", "", "Dump queries to this time, like '2006-01-02 16:04:05'")
	pFlags.StringSliceVar(&DumpConfig.AuditLogPaths, "audit-logs", nil, "Scan query from audit log files, either local path or 'ssh://xxx'")
	pFlags.StringVar(&DumpConfig.AuditLogTable, "audit-log-table", "", "Scan query from audit log table, like 'audit_db.audit_tbl'")
	pFlags.StringVar(&DumpConfig.AuditLogEncoding, "audit-log-encoding", "auto", "Audit log encoding, like utf8, gbk, ...")
	pFlags.BoolVar(&DumpConfig.Analyze, "analyze", false, "Run 'ANALYZE TABLE' before dump stats, only take effect when '--dump-stats=true'")
	pFlags.StringVar(&DumpConfig.SSHAddress, "ssh-address", "", "SSH address for downloading audit log, default is 'root@{db_host}:22'")
	pFlags.StringVar(&DumpConfig.SSHPassword, "ssh-password", "", "SSH password for '--ssh-address'")
	pFlags.StringVar(&DumpConfig.SSHPrivateKey, "ssh-private-key", "~/.ssh/id_rsa", "File path of SSH private key for '--ssh-address'")
	addAnonymizeBaseFlags(pFlags, false)

	dumpCmd.RegisterFlagCompletionFunc("query-states", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"ok", "eof", "err"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveDefault
	})

	flags := dumpCmd.Flags()
	flags.BoolVar(&DumpConfig.Clean, "clean", false, "Clean previous data and output directory")
}

func completeDumpConfig() error {
	if !DumpConfig.DumpSchema && !DumpConfig.DumpQuery {
		return errors.New("expected at least one of --dump-schema or --dump-query")
	}

	DumpConfig.OutputDDLDir = filepath.Join(GlobalConfig.OutputDir, "ddl")
	DumpConfig.OutputQueryDir = filepath.Join(GlobalConfig.OutputDir, "sql")
	DumpConfig.LocalAuditLogCacheDir = filepath.Join(GlobalConfig.DodoDataDir, "auditlog")

	if DumpConfig.AuditLogTable != "" && !strings.Contains(DumpConfig.AuditLogTable, ".") {
		return errors.New("need to specific database in '--audit-log-table', like 'audit_db.audit_tbl'")
	}

	if DumpConfig.QueryMinDuration_ > 0 {
		DumpConfig.QueryMinDurationMs = DumpConfig.QueryMinDuration_.Milliseconds()
	}

	if DumpConfig.From != "" {
		if _, err := time.Parse(time.DateTime, DumpConfig.From); err != nil {
			return err
		}
	}
	if DumpConfig.To != "" {
		if _, err := time.Parse(time.DateTime, DumpConfig.To); err != nil {
			return err
		}
	}

	GlobalConfig.DBs, GlobalConfig.Tables = lo.Uniq(GlobalConfig.DBs), lo.Uniq(GlobalConfig.Tables)
	dbs, tables := GlobalConfig.DBs, GlobalConfig.Tables
	if DumpConfig.DumpSchema && len(dbs) == 0 {
		return errors.New("expected at least one database, please use --dbs flag")
	} else if len(dbs) == 1 {
		// prepend default database if only one database specified
		prefix := dbs[0] + "."
		for i, t := range tables {
			if !strings.Contains(t, ".") {
				tables[i] = prefix + t
			}
		}
	} else {
		for _, t := range tables {
			if !strings.Contains(t, ".") {
				return errors.New("expected database in table name when multiple databases specified, e.g. --tables db1.table1,db2.table2")
			}
		}
	}

	DumpConfig.QueryStates = lo.Map(DumpConfig.QueryStates, func(s string, _ int) string {
		return strings.ToUpper(s)
	})

	DumpConfig.SSHPrivateKey = src.ExpandHome(DumpConfig.SSHPrivateKey)
	if DumpConfig.SSHAddress == "" {
		DumpConfig.SSHAddress = fmt.Sprintf("ssh://root@%s:22", GlobalConfig.DBHost)
	}
	if !strings.HasPrefix(DumpConfig.SSHAddress, "ssh://") {
		DumpConfig.SSHAddress = "ssh://" + DumpConfig.SSHAddress
	}

	return nil
}

func dumpSchemas(ctx context.Context) ([]*src.DBSchema, error) {
	dbs, tables := GlobalConfig.DBs, GlobalConfig.Tables
	g := src.ParallelGroup(GlobalConfig.Parallel)

	schemas := make([]*src.DBSchema, len(dbs))
	for i, db := range dbs {
		g.Go(func() error {
			logrus.Infof("Dumping schemas from %s...\n", db)
			conn, err := connectDB(db)
			if err != nil {
				return err
			}
			defer conn.Close()

			// dump schema
			createTables, err := src.ShowCreateTables(ctx, conn, db, tables...)
			if err != nil {
				return err
			}

			// dump stats
			if !DumpConfig.DumpStats {
				return nil
			}
			tbls := lo.Map(createTables, func(s *src.Schema, _ int) string { return s.Name })
			stats, err := src.GetTablesStats(ctx, conn, DumpConfig.Analyze, db, tbls...)
			if err != nil {
				return err
			}

			schemas[i] = &src.DBSchema{
				Name:    db,
				Schemas: createTables,
				Stats:   stats,
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return schemas, nil
}

func outputSchemas(schemas []*src.DBSchema) error {
	if len(schemas) == 0 {
		return nil
	}

	if !GlobalConfig.DryRun {
		if err := os.MkdirAll(DumpConfig.OutputDDLDir, 0755); err != nil {
			logrus.Errorln("Create output ddl directory failed, ", err)
			return err
		}
	}

	g := src.ParallelGroup(GlobalConfig.Parallel)
	for _, s := range schemas {
		g.Go(func() error {
			// 1. write each schema into split file
			for _, s := range s.Schemas {
				var filename string
				if AnonymizeConfig.Enabled {
					s.DB = src.Anonymize(AnonymizeConfig.Method, s.DB)
					s.Name = src.Anonymize(AnonymizeConfig.Method, s.Name)
				}

				filename = fmt.Sprintf("%s.%s.%s.sql", s.DB, s.Name, s.Type.Lower())
				if AnonymizeConfig.Enabled {
					s.CreateStmt = AnonymizeSQL(filename, s.CreateStmt)
				}

				path := filepath.Join(DumpConfig.OutputDDLDir, filename)
				if GlobalConfig.DryRun {
					return nil
				}
				if err := src.WriteFile(path, s.CreateStmt); err != nil {
					return err
				}

			}

			// 2. write all stats into one file
			if len(s.Stats) == 0 {
				return nil
			}
			if AnonymizeConfig.Enabled {
				s.Name = src.Anonymize(AnonymizeConfig.Method, s.Name)
				s.Stats = AnonymizeStats(s.Stats)
			}
			yml_, err := yaml.Marshal(s)
			if err != nil {
				return err
			}
			yml := string(yml_)

			if GlobalConfig.DryRun {
				return nil
			}

			filename := fmt.Sprintf("%s.stats.yaml", s.Name)
			path := filepath.Join(DumpConfig.OutputDDLDir, filename)
			return src.WriteFile(path, yml)
		})
	}

	return g.Wait()
}

func dumpQueries(ctx context.Context) (int, error) {
	if !GlobalConfig.DryRun {
		if err := os.MkdirAll(DumpConfig.OutputQueryDir, 0755); err != nil {
			logrus.Errorln("Create output query directory failed, ", err)
			return 0, err
		}
	}

	opts := src.AuditLogScanOpts{
		DBs:                GlobalConfig.DBs,
		QueryMinDurationMs: DumpConfig.QueryMinDurationMs,
		QueryStates:        DumpConfig.QueryStates,
		OnlySelect:         DumpConfig.OnlySelect,
		Strict:             DumpConfig.Strict,
		From:               DumpConfig.From,
		To:                 DumpConfig.To,
	}

	if DumpConfig.AuditLogTable != "" {
		return dumpQueriesFromTable(ctx, opts)
	}
	return dumpQueriesFromFile(ctx, opts)
}

func dumpQueriesFromTable(ctx context.Context, opts src.AuditLogScanOpts) (int, error) {
	if opts.From == "" || opts.To == "" {
		return 0, errors.New("must specific both '--from' and '--to' when dumping from audit log table")
	}

	dbTable := strings.SplitN(DumpConfig.AuditLogTable, ".", 2)
	dbname, table := dbTable[0], dbTable[1]

	db, err := connectDB(dbname)
	if err != nil {
		return 0, err
	}

	logrus.Infof("Dumping queries from audit log table '%s'...\n", DumpConfig.AuditLogTable)

	w := NewQueryWriter(1, 0)
	defer w.Close()

	count, err := src.GetDBAuditLogs(ctx, w, db, dbname, table, opts, GlobalConfig.Parallel)
	if err != nil {
		logrus.Errorf("Extract queries from audit logs table failed, %v\n", err)
		return 0, err
	}

	return count, nil
}

func dumpQueriesFromFile(ctx context.Context, opts src.AuditLogScanOpts) (int, error) {
	auditLogs := DumpConfig.AuditLogPaths
	if len(auditLogs) == 0 {
		sshUrl, err := chooseRemoteAuditLog(ctx)
		if err != nil {
			return 0, fmt.Errorf("please specific audit log files by '--audit-logs' or table by '--audit-log-table', err: %v", err)
		}
		auditLogs = []string{sshUrl}
	}

	logrus.Debugf("audit log paths: %+v", auditLogs)

	auditLogFiles := []string{}
	for _, auditLog := range auditLogs {
		var localPath string

		// 1. Remote audit log. scp remote path to local path
		if strings.HasPrefix(auditLog, "ssh://") {
			localPath = filepath.Join(DumpConfig.LocalAuditLogCacheDir, path.Base(auditLog))
			if err := copyAuditLog(ctx, auditLog, localPath); err != nil {
				logrus.Errorln("Copy remote audit log failed:", err)
				return 0, err
			}
			auditLogFiles = append(auditLogFiles, localPath)
			continue
		}

		// 2. Local audit log.
		localPath = strings.TrimPrefix(auditLog, "file://")
		localPaths, err := filepath.Glob(localPath)
		if err != nil {
			return 0, fmt.Errorf("invalid audit log path: %s, err: %v", localPath, err)
		}

		auditLogFiles = append(auditLogFiles, localPaths...)
	}

	// 3. Start dumping.
	logrus.Infoln("Dumping queries from audit log files...")

	writers := make([]src.SqlWriter, len(auditLogFiles))
	for i := range auditLogFiles {
		writers[i] = NewQueryWriter(len(auditLogFiles), i)
		//nolint:revive
		defer writers[i].Close()
	}

	count, err := src.ExtractQueriesFromAuditLogs(
		writers,
		auditLogFiles,
		DumpConfig.AuditLogEncoding,
		opts,
		GlobalConfig.Parallel,
	)
	if err != nil {
		logrus.Errorf("Extract queries from audit logs file failed, %v\n", err)
		return 0, err
	}

	return count, nil
}

type queryWriter struct {
	filename string
	f        *os.File
	w        *bufio.Writer
	count    int
}

func NewQueryWriter(filecount, fileidx int) src.SqlWriter {
	format := outputQueryFileNameFormat(filecount)
	name := fmt.Sprintf(format, fileidx)
	path := filepath.Join(DumpConfig.OutputQueryDir, name)

	w := &queryWriter{filename: name}
	if GlobalConfig.DryRun {
		return w
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		logrus.Fatalln("Can not open output sql file:", path, ", err:", err)
	}

	return &queryWriter{
		filename: name,
		f:        f,
		w:        bufio.NewWriterSize(f, 256*1024),
	}
}

func (w *queryWriter) WriteSql(s string) error {
	w.count++

	if AnonymizeConfig.Enabled {
		// anonymizer will strip leading '/*dodo...*/ ' comment,
		// we need restoring it after anonymize
		var leadComment string
		if strings.HasPrefix(s, src.ReplaySqlPrefix) {
			leadComment = s[:strings.Index(s, src.ReplaySqlSuffix)+len(src.ReplaySqlSuffix)+1]
		}
		b := strings.Builder{}
		b.Grow(len(s))
		b.WriteString(leadComment)
		b.WriteString(AnonymizeSQL(w.filename+"#"+strconv.Itoa(w.count), s))
		s = b.String()
	}
	if w.w == nil {
		return nil
	}
	if _, err := w.w.WriteString(s); err != nil {
		return err
	}
	_, err := w.w.WriteRune('\n')
	return err
}

func (w *queryWriter) Close() error {
	if w.w != nil {
		if err := w.w.Flush(); err != nil {
			return err
		}
	}
	if w.f != nil {
		return w.f.Close()
	}
	return nil
}

func outputQueryFileNameFormat(total int) string {
	count := 0
	for total != 0 {
		total /= 10
		count++
	}

	return fmt.Sprintf("q%%0%dd.sql", count)
}

func chooseRemoteAuditLog(ctx context.Context) (string, error) {
	conn, err := connectDB("information_schema")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	dir, err := src.ShowFronendsDisksDir(ctx, conn, "audit-log")
	if err != nil {
		return "", err
	}

	sshUrl, err := expandSSHPath(fmt.Sprintf("%s%s", DumpConfig.SSHAddress, dir))
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(sshUrl, "/") {
		sshUrl += "/"
	}
	sshUrl += "fe.audit.log*"

	auditLogs, err := src.SshLs(ctx, DumpConfig.SSHPrivateKey, sshUrl)
	if err != nil {
		return "", fmt.Errorf("SSH list remote audit log failed: %v", err)
	}
	if len(auditLogs) == 0 {
		return "", errors.New("no audit log found on remote server")
	}

	choosed, err := src.Choose("Choose audit log on remote server to dump", auditLogs)
	if err != nil {
		return "", err
	}

	return expandSSHPath(fmt.Sprintf("%s%s", DumpConfig.SSHAddress, choosed))
}

func copyAuditLog(ctx context.Context, remotePath, localPath string) error {
	remotePath, err := expandSSHPath(remotePath)
	if err != nil {
		return err
	}

	err = src.ScpFromRemote(ctx, DumpConfig.SSHPrivateKey, remotePath, localPath)
	if err != nil {
		err = fmt.Errorf("scp failed, please check --ssh-password or --ssh-private-key: %v", err)
	}
	return err
}

func expandSSHPath(remotePath string) (string, error) {
	// default
	u, err := url.Parse(DumpConfig.SSHAddress)
	if err != nil {
		return "", err
	}
	defaultUser := u.User.Username()
	defaultHost := u.Host
	defaultPass, passInAddr := u.User.Password()
	if !passInAddr {
		defaultPass = DumpConfig.SSHPassword
	}

	// remotePath custom
	u, err = u.Parse(remotePath)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		u.Host = defaultHost
	}
	user := u.User.Username()
	pass, passOk := u.User.Password()
	if user == "" {
		user = defaultUser
	}
	if !passOk {
		pass = defaultPass
	}
	u.User = url.UserPassword(user, pass)

	return u.String(), nil
}

func connectDB(db string) (*sqlx.DB, error) {
	if db == "" {
		return nil, errors.New("database name is required")
	}
	return src.NewDB(GlobalConfig.DBHost, GlobalConfig.DBPort, GlobalConfig.DBUser, GlobalConfig.DBPassword, GlobalConfig.Catalog, db)
}

func connectDBWithoutDBName() (*sqlx.DB, error) {
	return src.NewDB(GlobalConfig.DBHost, GlobalConfig.DBPort, GlobalConfig.DBUser, GlobalConfig.DBPassword, GlobalConfig.Catalog, "information_schema")
}
