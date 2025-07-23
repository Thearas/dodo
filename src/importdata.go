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
)

const (
	StreamLoadMaxRetries = 3
)

//nolint:revive
func StreamLoad(ctx context.Context, host, httpPort, user, password, db, table, file, fileProgress string, dryrun bool) error {
	f, err := os.Open(file)
	if err != nil {
		logrus.Errorf("Open data file '%s' failed\n", file)
		return err
	}
	r := bufio.NewReader(f)
	columns, err := r.ReadString('\n')
	if err != nil || len(columns) == 0 {
		_ = f.Close()
		return fmt.Errorf("data file '%s' is unreadable or empty", file)
	}

	skipLines := 1
	if !strings.HasPrefix(columns, GenDataFileFirstLinePrefix) {
		skipLines = 0
		columns = ""
	}
	_ = f.Close()

	// use curl to perform stream load
	userpass := fmt.Sprintf("%s:%s", user, password)
	curl := fmt.Sprintf(`curl -sS --location-trusted -u '%s' -H 'Expect:100-continue' -H 'Proxy-Connection:Close' -H 'format:csv' -H 'column_separator:%s' -H 'skip_lines:%d' -XPUT 'http://%s:%s/api/%s/%s/_stream_load'`, userpass, string(ColumnSeparator), skipLines, host, httpPort, db, table)
	if columns != "" {
		curl += fmt.Sprintf(" -H '%s'", columns)
	}
	curl += fmt.Sprintf(" -T '%s'", file)

	sanitizedCurl := strings.Replace(curl, userpass, fmt.Sprintf("%s:****", user), 1)
	logrus.Infof("Stream load %s.%s (%s)\n", db, table, fileProgress)
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
		logrus.Errorf("Stream load get result failed for '%s.%s' at data file '%s'\n", db, table, file)
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
		logrus.Errorf("Stream load failed for '%s.%s' at data file '%s', message: %v, details: %v\n", db, table, file, msg, details)
		return errors.New("stream load failed")
	}

	return nil
}
