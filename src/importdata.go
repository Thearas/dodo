package src

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"

	gen "github.com/Thearas/dodo/src/generator"
	"github.com/Thearas/dodo/src/pyspark"
)

const (
	StreamLoadMaxRetries = 3
)

//nolint:revive
func StreamLoad(ctx context.Context, host, httpPort, user, password, db, table, file, fileProgress, columnSeparator, lineDelimiter string, maxFilterRatio float64, dryrun bool) error {
	f, err := os.Open(file)
	if err != nil {
		logrus.Errorf("Open data file '%s' failed", file)
		return err
	}
	r := bufio.NewReader(f)
	columns, err := r.ReadString('\n')
	if err != nil {
		columns = ""
	}
	_ = f.Close()
	columns = strings.TrimSpace(columns)

	skipLines := 1
	if !strings.HasPrefix(columns, GenDataFileFirstLinePrefix) {
		skipLines = 0
		columns = ""
	}

	if columnSeparator == "" {
		columnSeparator = string(gen.ColumnSeparator)
	}
	if lineDelimiter == "" {
		lineDelimiter = "\n"
	}

	var contentLength int64
	if stat, err := os.Stat(file); err == nil {
		contentLength = stat.Size()
	}

	// use curl to perform stream load
	userpass := fmt.Sprintf("%s:%s", user, password)
	curl := fmt.Sprintf(`curl -sS --location-trusted --connect-timeout 5 -u '%s' -H 'Expect:100-continue' -H 'Proxy-Connection:Close' -H 'Content-Length: %d' -H 'format:csv' -H 'column_separator:%s' -H 'line_delimiter:%s' -H 'skip_lines:%d' -H 'max_filter_ratio:%v' -XPUT 'http://%s:%s/api/%s/%s/_stream_load'`, userpass, contentLength, columnSeparator, lineDelimiter, skipLines, maxFilterRatio, host, httpPort, db, table)
	if columns != "" {
		curl += fmt.Sprintf(" -H '%s'", columns)
	}
	curl += fmt.Sprintf(" -T '%s'", file)

	sanitizedCurl := strings.Replace(curl, userpass, fmt.Sprintf("%s:****", user), 1)
	logrus.Infof("Stream load %s.%s (%s)", db, table, fileProgress)
	logrus.Debugln(sanitizedCurl)

	if dryrun {
		return nil
	}

	var stdout []byte
	for range StreamLoadMaxRetries {
		cmd := exec.CommandContext(ctx, "sh", "-ec", curl)
		stdout, err = cmd.Output()
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	result := make(map[string]any)
	if err_ := json.Unmarshal(stdout, &result); err_ != nil {
		logrus.Errorf("Stream load get result failed for '%s.%s' at data file '%s', please check network and '--http-port'", db, table, file)
		return errors.New("stream load failed")
	}
	if status, ok := result["Status"]; !ok || status.(string) != "Success" {
		msg := result["Message"]
		if msg == nil {
			msg = result["msg"]
		}
		if msg == nil {
			msg = result["data"]
		}
		details := result["ErrorURL"]
		logrus.Errorf("Stream load failed for '%s.%s' at data file '%s', message: %v, details: %v", db, table, file, msg, details)
		return errors.New("stream load failed")
	}

	return nil
}

func PysparkImportCSV(ctx context.Context, spark *SparkCli, db, table, csvDirOrFile, columnSeparator string, additionalParams ...string) error {
	return spark.RunImportData(ctx, pyspark.PysparkImportData, db, table, csvDirOrFile, columnSeparator, additionalParams...)
}
