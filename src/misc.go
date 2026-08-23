package src

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/fatih/color"
	"github.com/goccy/go-json"
	"github.com/gogs/chardet"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/klauspost/pgzip"
	"github.com/manifoldco/promptui"
	"github.com/projectdiscovery/mapcidr"
	probing "github.com/prometheus-community/pro-bing"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/zeebo/blake3"
	"go.yaml.in/yaml/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/encoding/simplifiedchinese"
)

var (
	NumberRe = regexp.MustCompile(`\d+`)

	mProgress     *mpb.Progress
	mProgressLock = &sync.Mutex{}
)

func ExpandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		dirname, _ := os.UserHomeDir()
		path = filepath.Join(dirname, path[2:])
	}
	return path
}

func GetCIDR(ips ...string) ([]string, error) {
	if len(ips) == 0 {
		return []string{GetLocalIP() + "/24"}, nil
	} else if len(ips) == 1 {
		_, cidr, err := net.ParseCIDR(ips[0] + "/24")
		if err != nil {
			return nil, fmt.Errorf("invalid ip address: %s", ips[0])
		}
		return []string{cidr.String()}, nil
	}

	cidrs, err := mapcidr.AggregateApproxIPv4To24(lo.Map(ips, func(ip string, _ int) *net.IPNet {
		_, cidr, err := net.ParseCIDR(ip + "/24")
		if err != nil {
			logrus.Fatalf("invalid ip address: %s", ip)
		}
		return cidr
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate cidr for ips %v: %v", ips, err)
	}
	return lo.Map(cidrs, func(cidr *net.IPNet, _ int) string { return cidr.String() }), nil
}

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logrus.Errorln("local ip not found, get net interface failed")
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip := ipnet.IP.String()
				return ip
			}
		}
	}
	logrus.Errorln("local ip not found")
	return ""
}

func IsLocalhost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == GetLocalIP()
}

func WriteFile(path string, content string) error {
	if content == "" {
		return nil
	}

	// append newline if not exists
	b := []byte(content)
	if b[len(b)-1] != '\n' {
		b = append(b, '\n')
	}
	return os.WriteFile(path, b, 0600)
}

// RemoveBlankLines removes lines that contain only spaces/tabs/newline.
// This prevents YAML parsing errors caused by whitespace-only lines.
func RemoveBlankLines(b []byte) []byte {
	var result []byte
	for len(b) > 0 {
		// Find end of current line
		idx := bytes.IndexByte(b, '\n')
		var line []byte
		if idx == -1 {
			line = b
			b = nil
		} else {
			line = b[:idx+1]
			b = b[idx+1:]
		}
		// Skip blank lines (empty or containing only spaces/tabs)
		trimmed := bytes.TrimRight(line, " \t\r\n")
		if len(trimmed) == 0 {
			continue
		}
		result = append(result, line...)
	}
	return result
}

func ReadFileOrStdin(path string) (string, error) {
	var (
		input []byte
		err   error
	)
	switch path {
	case "-":
		// read from stdin
		input, err = io.ReadAll(os.Stdin)
	default:
		input, err = os.ReadFile(path)
	}
	return string(input), err
}

func ParallelGroup(parallel int) *errgroup.Group {
	g := errgroup.Group{}
	if parallel >= 1 {
		g.SetLimit(parallel)
	}
	return &g
}

func Confirm(msg string) bool {
	prompt := promptui.Prompt{
		Label:     msg,
		IsConfirm: true,
	}
	defaultYes := os.Getenv("DODO_YES")
	if defaultYes == "0" {
		return false
	} else if defaultYes != "" {
		return true
	}
	result, _ := prompt.Run()
	return result == "y"
}

func Choose(msg string, items []string) (string, error) {
	prompt := promptui.Select{
		Label:             msg,
		Items:             items,
		Size:              20,
		StartInSearchMode: true,
		Searcher: func(input string, index int) bool {
			item := items[index]
			return strings.Contains(item, input)
		},
	}
	_, result, err := prompt.Run()
	return result, err
}

func UserInput(msg string) (string, error) {
	prompt := promptui.Prompt{
		Label: msg,
	}
	return prompt.Run()
}

func GenRowProgressBar(totalRows int64, title string) *mpb.Bar {
	mProgressLock.Lock()
	defer mProgressLock.Unlock()
	initMultiProgressBar()
	return mProgress.AddBar(totalRows,
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(title, decor.WC{C: decor.DindentRight | decor.DextraSpace}),
			decor.OnCompleteMeta(
				decor.OnComplete(
					decor.Meta(decor.Name("generating", decor.WCSyncSpaceR), colorPrintF(color.FgBlue)),
					"done!",
				),
				colorPrintF(color.FgGreen),
			),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), ""),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.NewPercentage("%d", decor.WCSyncSpaceR), ""),
			decor.Name(fmt.Sprintf("(total %d)", totalRows)),
		),
	)
}

func BytesProgressBar(totalBytes int64, title, doing string) *mpb.Bar {
	mProgressLock.Lock()
	defer mProgressLock.Unlock()
	initMultiProgressBar()
	return mProgress.AddBar(totalBytes,
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(title, decor.WC{C: decor.DindentRight | decor.DextraSpace}),
			decor.OnCompleteMeta(
				decor.OnComplete(
					decor.Meta(decor.Name(doing, decor.WCSyncSpaceR), colorPrintF(color.FgBlue)),
					"done!",
				),
				colorPrintF(color.FgGreen),
			),
			decor.Counters(decor.SizeB1024(0), "% .2f / % .2f", decor.WC{C: decor.DindentRight | decor.DextraSpace}),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), ""),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.NewPercentage("%d", decor.WCSyncSpaceR), ""),
		),
	)
}

func ShutdownMultiProgressBar() {
	mProgressLock.Lock()
	defer mProgressLock.Unlock()

	if mProgress != nil {
		mProgress.Shutdown()
		mProgress = nil
	}
}

func initMultiProgressBar() {
	if mProgress == nil {
		mProgress = mpb.New(mpb.WithOutput(os.Stderr), mpb.WithWidth(30), mpb.WithAutoRefresh())
	}
}

func colorPrintF(a color.Attribute) func(string) string {
	c := color.New(a)
	return func(s string) string {
		return c.Sprint(s)
	}
}

func hashstr(h *blake3.Hasher, s string) [32]byte {
	_, _ = h.WriteString(s)
	result := h.Sum(nil)
	h.Reset()
	return [32]byte(result)
}

func DetectCharset(r *bufio.Reader) (string, error) {
	hdr, err := r.Peek(4096)
	if len(hdr) == 0 {
		return "", fmt.Errorf("cannot read file: %v", err)
	}
	ress, err := chardet.NewTextDetector().DetectAll(hdr)
	if err != nil {
		return "", fmt.Errorf("cannot detect encoding: %v", err)
	}
	if _, utf8 := lo.Find(ress, func(r chardet.Result) bool { return r.Charset == "UTF-8" }); utf8 {
		return "UTF-8", nil
	}

	return ress[0].Charset, nil
}

func FileGlob(paths []string) ([]string, error) {
	files := []string{}
	for _, s := range paths {
		// '-' represents stdin
		if s == "-" {
			files = append(files, "-")
			continue
		}
		localPaths, err := filepath.Glob(s)
		if err != nil {
			return nil, fmt.Errorf("invalid file path: %s, err: %v", s, err)
		}

		files = append(files, localPaths...)
	}

	return lo.Uniq(files), nil
}

//nolint:revive
func RunCmd(ctx context.Context, cmd string, pipe bool) (string, error) {
	subprocess := exec.CommandContext(ctx, "bash", "-ec", cmd)

	var (
		out = []byte{}
		err error
	)
	if pipe {
		subprocess.Stdin = os.Stdin
		subprocess.Stdout = os.Stdout
		subprocess.Stderr = os.Stderr
		err = subprocess.Run()
	} else {
		out, err = subprocess.CombinedOutput()
	}

	return string(out), err
}

func CompressTarGz(srcDir, destPath string, parallel int) error {
	// Create temporary file for incomplete tar.gz
	compTarTemp := destPath + ".uncomplete"
	f, err := os.Create(compTarTemp)
	if err != nil {
		return err
	}
	defer f.Close()

	gw := pgzip.NewWriter(f)
	defer gw.Close()
	gw.SetConcurrency(2*1024*1024, parallel)
	tw := tar.NewWriter(gw)
	defer tw.Close()
	srcRoot, err := os.OpenRoot(srcDir)
	if err != nil {
		return err
	}
	defer srcRoot.Close()

	// Walk through the source directory
	baseDir := filepath.Base(srcDir)
	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Set the correct name for the header (relative to the source directory)
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}
		// if relPath == "." {
		// 	return nil
		// }

		// Create a tar header from the file info
		var (
			isSymlink     = info.Mode()&os.ModeSymlink != 0
			symlinkTarget string
		)
		if isSymlink {
			symlinkTarget, err = srcRoot.Readlink(relPath)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, symlinkTarget)
		if err != nil {
			return fmt.Errorf("failed to create tar header: %w", err)
		}
		header.Name = filepath.ToSlash(filepath.Join(baseDir, relPath))

		// Write the header
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		// if not a symlink or dir, write file content
		if isSymlink || info.IsDir() {
			return nil
		}
		srcFile, err := srcRoot.Open(relPath)
		if err != nil {
			return fmt.Errorf("failed to open source file: %w", err)
		}
		defer srcFile.Close()

		if _, err := io.Copy(tw, srcFile); err != nil {
			return fmt.Errorf("failed to copy file content: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return os.Rename(compTarTemp, destPath)
}

func DetectFileEncoding(encoding string, f *os.File) (encoding.Encoding, error) {
	// detect encoding
	var err error
	if encoding == "auto" || encoding == "" {
		// detect encoding
		encoding, err = DetectCharset(bufio.NewReader(f))
		if err != nil {
			return nil, fmt.Errorf("cannot detect charset of file %s: %v", f.Name(), err)
		}
		if _, err = f.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("cannot seek file %s: %v", f.Name(), err)
		}
	}
	enc, err := GetEncoding(encoding)
	if err != nil {
		return nil, err
	}

	return enc, nil
}

func GetEncoding(name string) (encoding.Encoding, error) {
	enc, err := htmlindex.Get(name)
	if err != nil {
		return nil, fmt.Errorf("invalid encoding: %s", name)
	}
	//nolint:revive
	switch enc {
	case simplifiedchinese.GBK:
		enc = simplifiedchinese.GB18030
	}

	return enc, nil
}

func MustJsonMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func MustYamlMarshal(v any) []byte {
	data, err := yaml.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func TrimQuote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func IsStringType(colType string) bool {
	switch colType {
	case "VARCHAR", "CHAR", "TEXT", "STRING":
		return true
	}
	return false
}

func IsIntegerType(colType string) bool {
	switch colType {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "LARGEINT":
		return true
	}
	return false
}

func IsFloatType(colType string) bool {
	switch colType {
	case "FLOAT", "DOUBLE", "DECIMAL", "DECIMALV2", "DECIMALV3":
		return true
	}
	return false
}

func IsTimeType(colType string) bool {
	return IsDateType(colType) || IsDateTimeType(colType)
}

func IsDateType(colType string) bool {
	switch colType {
	case "DATE", "DATEV1", "DATEV2":
		return true
	}
	return false
}

func IsDateTimeType(colType string) bool {
	switch colType {
	case "DATETIME", "DATETIMEV1", "DATETIMEV2", "TIMESTAMP":
		return true
	}
	return false
}

func IsVaildNumber(i string) bool {
	if len(i) == 0 {
		return false
	}
	if i[0] == '-' || i[0] == '+' {
		i = i[1:]
	}
	if len(i) == 0 {
		return false
	}

	f := big.Float{}
	_, ok := f.SetString(i)
	return ok
}

func IsVaildDate(i string) bool {
	if len(i) == 0 {
		return false
	}

	t, err := cast.StringToDate(i)
	if err != nil {
		return false
	}
	return t.Year() > 1997 && t.Year() <= time.Now().Year()+1
}

func IsVaildStatsMinMax(minVal, maxVal string) bool {
	return (IsVaildNumber(minVal) && IsVaildNumber(maxVal)) || (IsVaildDate(minVal) && IsVaildDate(maxVal))
}

func IsPortOpen(ctx context.Context, host string, port int) bool {
	address := net.JoinHostPort(host, strconv.Itoa(port))

	// Create a dialer with timeout
	dialer := &net.Dialer{
		Timeout: 3 * time.Second,
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func Ping(host string, timeout time.Duration) error {
	pinger, err := probing.NewPinger(host)
	if err != nil {
		return fmt.Errorf("failed to ping: %v", err)
	}
	pinger.Count = 1
	pinger.Timeout = timeout
	pinger.SetPrivileged(false)
	err = pinger.Run() // Blocks until finished.
	if err != nil {
		return fmt.Errorf("failed to run ping: %v", err)
	}
	if pinger.Statistics().PacketsRecv == 0 {
		return fmt.Errorf("host %s is unreachable", host)
	}
	// stats := pinger.Statistics() // get send/receive/duplicate/rtt stats
	return nil
}

func ReplaceOSSUri(uri string) string {
	uri = strings.TrimSpace(uri)
	if strings.Contains(uri, "oss-accelerate.aliyuncs.com") {
		// do nothing for oss accelerate endpoint
		return uri
	}

	if strings.Contains(uri, ".aliyuncs.com") {
		// ping oss public endpoint
		ossEndpoint := "oss-cn-beijing-internal.aliyuncs.com"
		if err := Ping(ossEndpoint, 100*time.Millisecond); err != nil {
			logrus.Tracef("failed to ping internal oss endpoint %s: %v", ossEndpoint, err)
			uri = strings.Replace(uri, "-internal.aliyuncs.com", ".aliyuncs.com", 1)
		} else if !strings.Contains(uri, "-internal.aliyuncs.com") {
			uri = strings.Replace(uri, ".aliyuncs.com", "-internal.aliyuncs.com", 1)
		}
	}

	return uri
}

func IsInternalCatalog(catalog string) bool {
	return slices.Contains([]string{"", "internal"}, catalog)
}

// Must call ShutdownMultiProgressBar() after using this function.
func DownloadHTTPFile(ctx context.Context, localpath, url string, progressTitle string) error {
	tempFile, err := os.Create(localpath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	// Create HTTP request
	req_, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Execute request
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp_, err := client.Do(req_)
	if err != nil {
		return fmt.Errorf("failed to download from %s: %w", url, err)
	}
	_ = resp_.Body.Close()
	if resp_.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status %d: %s", resp_.StatusCode, url)
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 5
	retryClient.RetryWaitMax = 5 * time.Second
	retryClient.Logger = nil

	cli := grab.NewClient()
	cli.HTTPClient = retryClient.StandardClient()
	req, err := grab.NewRequest(localpath, url)
	if err != nil {
		return fmt.Errorf("failed to construct download request for %s: %w", url, err)
	}
	// start download
	resp := cli.Do(req.WithContext(ctx))

	// start UI progress loop
	t := time.NewTicker(400 * time.Millisecond)
	defer t.Stop()
	progress := BytesProgressBar(resp_.ContentLength, progressTitle, "downloading")

Loop:
	for {
		select {
		case <-t.C:
			progress.SetCurrent(resp.BytesComplete())
		case <-resp.Done:
			// download is complete
			break Loop
		}
	}

	if err := resp.Err(); err != nil {
		return fmt.Errorf("failed to download from %s: %w", url, err)
	}

	logrus.Infof("Downloaded %.2f MB successfully", float64(resp_.ContentLength)/1024/1024)
	return nil
}
