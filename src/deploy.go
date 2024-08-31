/*
Copyright © 2025 Thearas thearas850@gmail.com

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
package src

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/hairyhenderson/go-which"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

type DeployConfig struct {
	// Doris package source (local path, URL, or object storage path)
	PackageSource string
	// FE nodes (IP addresses)
	FENodes []string
	// BE nodes (IP addresses)
	BENodes []string
	// FE configuration file or key=value pairs
	FEConf string
	// BE configuration file or key=value pairs
	BEConf string
	// SSH credentials
	SSHUser     string
	SSHPassword string
	SSHKeyPath  string
	SSHPort     int
	// Deployment paths
	DorisHome string // e.g., /opt/doris
	// Java home path
	JavaHome string
	// OSS credentials (optional, will use environment variables if not set)
	OSSEndpoint        string
	OSSAccessKeyID     string
	OSSAccessKeySecret string

	TempDir  string
	Parallel int

	priorityNetworks string
	sqlPort          uint16
	dorisHomeTempDir string
}

// DeployManager manages the Doris cluster deployment
type DeployManager struct {
	config *DeployConfig
}

// NewDeployManager creates a new deployment manager
func NewDeployManager(config *DeployConfig) *DeployManager {
	if which.Which("mysql") == "" {
		logrus.Fatalln("Command mysql not found")
	}
	if which.Which("tar") == "" {
		logrus.Fatalln("Command tar not found")
	}
	if len(config.FENodes) == 0 {
		logrus.Fatalln("At least one FE node is required")
	}
	if len(config.BENodes) == 0 {
		logrus.Fatalln("At least one BE node is required")
	}
	if !filepath.IsAbs(config.DorisHome) {
		logrus.Fatalf("Doris home path must be absolute: %s", config.DorisHome)
	}

	config.sqlPort = 9030
	config.dorisHomeTempDir = "dodo"
	return &DeployManager{
		config: config,
	}
}

// Deploy deploys the Doris cluster
func (dm *DeployManager) Deploy(ctx context.Context) error {
	// prepend priority_networks to FE and BE conf
	allnodes := lo.Union(append(dm.config.FENodes, dm.config.BENodes...))
	cidr, err := GetCIDR(allnodes...)
	if err != nil {
		return fmt.Errorf("failed to get CIDR: %w", err)
	}
	dm.config.priorityNetworks = strings.Join(cidr, ",")

	// Check JAVA_HOME on each machine
	if err := dm.checkJavaHome(ctx, allnodes); err != nil {
		return err
	}

	// Step 1: Extract/Download package
	if err := dm.preparePackage(ctx, false); err != nil {
		return fmt.Errorf("failed to prepare package: %w", err)
	}
	ShutdownMultiProgressBar()

	logrus.Infof("Deploy Doris cluster:")
	logrus.Infof("  FE nodes: %v", dm.config.FENodes)
	logrus.Infof("  BE nodes: %v", dm.config.BENodes)

	_ = os.Unsetenv("http_proxy")
	_ = os.Unsetenv("https_proxy")

	// Step 2: Deploy FE Master
	masterFENode := dm.config.FENodes[0]
	logrus.Infof("Deploying FE Master on %s", masterFENode)

	feConf := dm.generateFEConf()
	if err := dm.deployFEMaster(ctx, masterFENode, feConf); err != nil {
		return fmt.Errorf("failed to deploy FE Master: %w", err)
	}
	ShutdownMultiProgressBar()

	// Wait for FE Master to be ready
	if err := dm.waitForFEReady(ctx, masterFENode); err != nil {
		return fmt.Errorf("FE Master failed to start: %w", err)
	}

	// Step 3: Deploy additional FE nodes (Followers)
	feFollowers := dm.config.FENodes[1:]
	g := ParallelGroup(-1)
	for _, feNode := range feFollowers {
		g.Go(func() error {
			logrus.Infof("Deploying FE Follower on %s", feNode)

			if err := dm.deployFEFollower(ctx, feNode, feConf, masterFENode); err != nil {
				logrus.Errorf("failed to deploy FE Follower on %s: %v", feNode, err)
				// Continue with other nodes
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	ShutdownMultiProgressBar()

	// Step 4: Deploy BE nodes
	g = ParallelGroup(-1)
	beConf := dm.generateBEConf()
	for _, beNode := range dm.config.BENodes {
		g.Go(func() error {
			logrus.Infof("Deploying BE on %s", beNode)

			if err := dm.deployBE(ctx, beNode, beConf); err != nil {
				logrus.Errorf("failed to deploy BE on %s: %v", beNode, err)
				// Continue with other nodes
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	ShutdownMultiProgressBar()

	logrus.Info("Doris cluster deployment completed successfully!")
	logrus.Infof("Connect: mysql -h %s -P %d -uroot", masterFENode, dm.config.sqlPort)

	dm.teardown(ctx, allnodes)
	return nil
}

func (dm *DeployManager) teardown(ctx context.Context, allnodes []string) {
	// clean temp dir
	g := ParallelGroup(-1)
	for _, node := range allnodes {
		g.Go(func() error {
			_, err := dm.executeRemoteCommands(ctx, node,
				"rm -rf "+filepath.Join(dm.config.DorisHome, dm.config.dorisHomeTempDir),
			)
			return err
		})
	}
	_ = g.Wait()
}

// checkJavaHome verifies JAVA_HOME is set and valid on each node
func (dm *DeployManager) checkJavaHome(ctx context.Context, nodes []string) error {
	logrus.Debugln("Checking JAVA_HOME on all nodes...")

	var errs []error
	var mu sync.Mutex

	g := ParallelGroup(-1)
	for _, node := range nodes {
		g.Go(func() error {
			javaHome := dm.config.JavaHome
			var checkCmd string
			if javaHome != "" {
				// Check if configured JAVA_HOME exists and has java binary
				checkCmd = fmt.Sprintf("test -d '%s' && test -x '%s/bin/java' && '%s/bin/java' -version 2>&1 | head -1", javaHome, javaHome, javaHome)
			} else {
				// Check if JAVA_HOME env var is set and valid
				checkCmd = "test -n \"$JAVA_HOME\" && test -d \"$JAVA_HOME\" && test -x \"$JAVA_HOME/bin/java\" && \"$JAVA_HOME/bin/java\" -version 2>&1 | head -1"
			}

			output, err := dm.executeRemoteCommands(ctx, node, checkCmd)
			if err != nil {
				mu.Lock()
				if javaHome != "" {
					errs = append(errs, fmt.Errorf("node %s: JAVA_HOME '%s' is invalid or java not found, err: %v", node, javaHome, err))
				} else {
					errs = append(errs, fmt.Errorf("node %s: JAVA_HOME environment variable is not set or invalid, err: %v", node, err))
				}
				mu.Unlock()
				return nil // Don't fail the group, collect all errors
			}

			logrus.Debugf("JAVA_HOME check passed on %s: %s", node, strings.TrimSpace(output))
			return nil
		})
	}
	_ = g.Wait()

	if len(errs) > 0 {
		for _, e := range errs {
			logrus.Error(e)
		}
		return fmt.Errorf("JAVA_HOME check failed on %d node(s)", len(errs))
	}

	logrus.Debugln("JAVA_HOME check passed on all nodes")
	return nil
}

func (dm *DeployManager) preparePackage(ctx context.Context, cloud bool) error {
	tempPath, err := dm.downloadPackage(ctx)
	if err != nil {
		return err
	}

	return dm.repackComponents(ctx, tempPath, cloud)
}

func (dm *DeployManager) downloadPackage(ctx context.Context) (string, error) {
	source := dm.config.PackageSource
	if !strings.HasSuffix(source, ".tar.gz") {
		return "", errors.New("only .tar.gz packages are supported")
	}

	pkgPath := filepath.Join(dm.config.TempDir, filepath.Base(source))
	if _, err := os.Stat(pkgPath); err == nil {
		if !Confirm(fmt.Sprintf("Local package %s already exists, re-download", pkgPath)) {
			logrus.Infoln("Using existing package")
			return pkgPath, nil
		}
	}

	// Remove old paths
	if err := dm.removeOlds(ctx); err != nil {
		return "", err
	}

	tempPath := pkgPath + ".downloading"
	if err := os.MkdirAll(dm.config.TempDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temp dir %s: %w", dm.config.TempDir, err)
	}
	f, err := os.Create(tempPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local package file: %w", err)
	}
	_ = f.Close()

	logrus.Infof("Downloading from '%s' to '%s'", source, tempPath)

	// Handle remote sources
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		err = dm.downloadHTTP(ctx, tempPath, source)
	} else if strings.HasPrefix(source, "oss://") {
		err = dm.downloadOSS(ctx, tempPath, source)
	} else if strings.HasPrefix(source, "file://") || !strings.Contains(source, "://") {
		// local file, just use it
		tempPath = strings.TrimPrefix(source, "file://")
	} else {
		return "", fmt.Errorf("%s download not yet implemented, please use OSS or HTTP(S) sources", strings.SplitN(source, "://", 2)[0])
	}
	if err != nil {
		return "", err
	}

	return pkgPath, os.Rename(tempPath, pkgPath) // mv to final path
}

func (dm *DeployManager) removeOlds(ctx context.Context) error {
	// 1. Local: Remove old tag.gz
	downloadingPkg := filepath.Join(dm.config.TempDir, "*.downloading")
	tagGzPkg := filepath.Join(dm.config.TempDir, dm.compTarName("*"))
	_, err := dm.executeLocalCommands(ctx,
		fmt.Sprintf("rm %s || true", downloadingPkg),
		fmt.Sprintf("rm %s || true", tagGzPkg),
	)
	if err != nil {
		return fmt.Errorf("failed to remove old doris package: %w", err)
	}

	// 2. Remote: Remove old remote file if exists
	allnodes := append(dm.config.FENodes, dm.config.BENodes...)
	g := ParallelGroup(-1)
	for _, node := range allnodes {
		g.Go(func() error {
			_, err := dm.executeRemoteCommands(ctx,
				node,
				// match all component packages
				fmt.Sprintf("rm %s || true", filepath.Join(dm.config.DorisHome, dm.compTarName("*"))),
			)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to remove old remote package: %w", err)
	}
	return nil
}

//nolint:revive
func (dm *DeployManager) repackComponents(ctx context.Context, pkgPath string, cloud bool) error {
	// Extract and pack fe/be separately
	logrus.Infoln("Extracting and repacking Doris package for each component...")
	pkgPath, err := filepath.Abs(pkgPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path of package: %w", err)
	}
	unpackDir := dm.TarBase(pkgPath)
	unpackDirTemp := "dodo-unpack"
	if s, err := os.Stat(unpackDir); err == nil && s.IsDir() {
		logrus.Debugf("Directory %s already exists, skipping extraction", unpackDir)
	} else {
		_, err = dm.executeLocalCommands(ctx,
			fmt.Sprintf("mkdir -p %s", filepath.Join(dm.config.TempDir, unpackDirTemp)),
			fmt.Sprintf("cd '%s' && tar xzf '%s' --directory=%s && cp -r %s/* . && (mv output %s || true)",
				dm.config.TempDir,
				pkgPath,
				unpackDirTemp,
				unpackDirTemp,
				unpackDir,
			),
			fmt.Sprintf("rm -rf %s", unpackDirTemp),
		)
		if err != nil {
			return fmt.Errorf("failed to extract Doris package: %w", err)
		}
	}

	components := []string{"fe", "be"}
	if cloud {
		components = append(components, "ms", "tools")
	}
	for _, component := range components {
		compTar := dm.compTarName(component)
		if s, err := os.Stat(compTar); err == nil && !s.IsDir() {
			logrus.Debugf("File %s already exists, skipping compression", compTar)
			return nil
		}

		unpackCompDir := filepath.Join(dm.config.TempDir, unpackDir, component)
		compTarGzFile := filepath.Join(dm.config.TempDir, compTar)
		err = CompressTarGz(unpackCompDir, compTarGzFile, dm.config.Parallel)
		if err != nil {
			return fmt.Errorf("failed to repack %s package: %w", component, err)
		}
	}

	return nil
}

// downloadHTTP downloads a package from HTTP(S) URL
func (*DeployManager) downloadHTTP(ctx context.Context, localpath, url string) error {
	defer ShutdownMultiProgressBar()
	return DownloadHTTPFile(ctx, localpath, url, "Doris package")
}

// downloadOSS downloads a package from Alibaba OSS
func (dm *DeployManager) downloadOSS(ctx context.Context, localPath, ossPath string) error {
	// Parse OSS path: oss://bucket/path/to/file or oss://bucket/key with credentials in environment
	parts := strings.TrimPrefix(ossPath, "oss://")
	parts = strings.TrimPrefix(parts, "//")

	// Split bucket and key
	separatorIdx := strings.Index(parts, "/")
	if separatorIdx == -1 {
		return fmt.Errorf("invalid OSS path format, expected oss://bucket/key: %s", ossPath)
	}

	bucket := parts[:separatorIdx]
	key := parts[separatorIdx+1:]

	// Get OSS credentials from config first, then environment variables
	endpoint := dm.config.OSSEndpoint
	if endpoint == "" {
		endpoint = os.Getenv("OSS_ENDPOINT")
	}
	accessKeyID := dm.config.OSSAccessKeyID
	if accessKeyID == "" {
		accessKeyID = os.Getenv("OSS_ACCESS_KEY_ID")
	}
	accessKeySecret := dm.config.OSSAccessKeySecret
	if accessKeySecret == "" {
		accessKeySecret = os.Getenv("OSS_ACCESS_KEY_SECRET")
	}

	if accessKeyID == "" || accessKeySecret == "" {
		return errors.New("OSS ak/sk not found in config or environment: --oss-access-key/--oss-secret-key or OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET")
	}

	// extract region from endpoint: oss-cn-beijing.aliyuncs.com -> cn-beijing
	// remove prefix
	region_ := strings.SplitN(endpoint, "oss-", 2)
	if len(region_) != 2 {
		return fmt.Errorf("invalid OSS endpoint: %s", endpoint)
	}
	region := region_[1]
	// remote suffix
	region = strings.TrimSuffix(strings.TrimSuffix(region, ".aliyuncs.com"), "-internal.aliyuncs.com")

	// Create OSS client
	cred := credentials.NewStaticCredentialsProvider(accessKeyID, accessKeySecret)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(cred).
		WithEndpoint(ReplaceOSSUri(endpoint)).
		WithRegion(region).
		WithRetryMaxAttempts(3)

	client := oss.NewClient(cfg)

	// get file size
	r, err := client.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object info from OSS: %w", err)
	}
	_ = r.Body.Close()
	progress := BytesProgressBar(r.ContentLength, "Doris package", "downloading")
	defer ShutdownMultiProgressBar()

	// Download file from OSS
	downloader := oss.NewDownloader(client, func(do *oss.DownloaderOptions) {
		do.CheckpointDir = filepath.Dir(localPath)
		do.EnableCheckpoint = true
		do.ParallelNum = dm.config.Parallel
		do.PartSize = 3 * 1024 * 1024 // 3 MB
	})
	result, err := downloader.DownloadFile(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
		ProgressFn: func(increment, _, _ int64) {
			progress.IncrInt64(increment)
		},
	}, localPath)
	if err != nil {
		return fmt.Errorf("failed to download from OSS: %w", err)
	}

	logrus.Infof("Downloaded %.2f MB from OSS successfully", float64(result.Written)/1024/1024)
	return nil
}

func (dm *DeployManager) deployFEMaster(ctx context.Context, feNode, feConf string) error {
	return dm.deployFE(ctx, feNode, feConf)
}

func (dm *DeployManager) deployFEFollower(ctx context.Context, feNode, feConf, masterFE string) error {
	// Start FE with helper
	err := dm.deployFE(ctx, feNode, feConf, fmt.Sprintf("--helper %s:9010", masterFE))
	if err != nil {
		return err
	}

	// Register FE Follower in cluster
	if err := dm.addFollowerNode(ctx, masterFE, feNode); err != nil {
		return fmt.Errorf("failed to register FE follower %s: %w", feNode, err)
	}

	// Wait FE Follower to be ready
	if err := dm.waitForFEReady(ctx, feNode); err != nil {
		return fmt.Errorf("failed to start FE follower %s: %w", feNode, err)
	}
	return nil
}

func (dm *DeployManager) deployFE(ctx context.Context, feNode, feConf string, extraArgs ...string) error {
	var (
		feHome         = filepath.Join(dm.config.DorisHome, "fe")
		feStopCmdPath  = filepath.Join(feHome, "bin", "stop_fe.sh")
		feStartCmdPath = filepath.Join(feHome, "bin", "start_fe.sh")
		feConfPath     = filepath.Join(feHome, "conf", "fe.conf")
		feOrigConfPath = filepath.Join(feHome, "conf", "fe.conf.dodo.orig")
		feMeta         = filepath.Join(feHome, "doris-meta")
		feOldMeta      = filepath.Join(feHome, "doris-meta-bak")
	)

	_, err := dm.executeRemoteCommands(ctx, feNode,
		"set +e",
		// stop be
		fmt.Sprintf("sh '%s'", feStopCmdPath),
		// backup old storage
		fmt.Sprintf("rm -rf '%s' && mv '%s' '%s' && mkdir -p '%s'", feOldMeta, feMeta, feOldMeta, feMeta),
		"true",
	)
	if err != nil {
		return fmt.Errorf("failed to prepare FE node %s: %w", feNode, err)
	}

	if err := dm.pushPackageToNode(ctx, feNode, "fe"); err != nil {
		return fmt.Errorf("failed to push package to %s: %w", feNode, err)
	}

	// Backup original fe.conf -> fe.conf.dodo.orig,
	// then modify fe.conf
	commands := []string{
		// stop fe
		fmt.Sprintf("sh '%s' || true", feStopCmdPath),

		// backup old meta
		fmt.Sprintf("(rm -rf '%s' || true) && (mv '%s' '%s' || true) && mkdir -p '%s'", feOldMeta, feMeta, feOldMeta, feMeta),

		// backup conf
		fmt.Sprintf("[ -f '%s' ] || cp '%s' '%s'", feOrigConfPath, feConfPath, feOrigConfPath),
		fmt.Sprintf("mv '%s' '%s.bak' && cp '%s' '%s'", feConfPath, feConfPath, feOrigConfPath, feConfPath),
		fmt.Sprintf("cat >> '%s' << 'EOF'\n%s\nEOF", feConfPath, feConf),
	}

	// Start FE
	commands = append(commands, fmt.Sprintf("export JAVA_HOME=%s && sh %s --daemon %s", dm.config.JavaHome, feStartCmdPath, strings.Join(extraArgs, " ")))

	_, err = dm.executeRemoteCommands(ctx, feNode, commands...)
	return err
}

func (dm *DeployManager) deployBE(ctx context.Context, beNode, beConf string) error {
	var (
		beHome         = filepath.Join(dm.config.DorisHome, "be")
		beStartCmdPath = filepath.Join(beHome, "bin", "start_be.sh")
		beStopCmdPath  = filepath.Join(beHome, "bin", "stop_be.sh")
		beConfPath     = filepath.Join(beHome, "conf", "be.conf")
		beOrigConfPath = filepath.Join(beHome, "conf", "be.conf.dodo.orig")
		beStorage      = filepath.Join(beHome, "storage")
		beOldStorage   = filepath.Join(beHome, "storage-bak")
	)

	_, err := dm.executeRemoteCommands(ctx, beNode,
		"set +e",
		// stop be
		fmt.Sprintf("sh '%s'", beStopCmdPath),
		// backup old storage
		fmt.Sprintf("rm -rf '%s' && mv '%s' '%s' && mkdir -p '%s'", beOldStorage, beStorage, beOldStorage, beStorage),
		"true",
	)
	if err != nil {
		return fmt.Errorf("failed to prepare BE node %s: %w", beNode, err)
	}

	if err := dm.pushPackageToNode(ctx, beNode, "be"); err != nil {
		return fmt.Errorf("failed to push package to %s: %w", beNode, err)
	}

	// Backup original be.conf -> be.conf.dodo.orig,
	// then modify be.conf
	commands := []string{
		fmt.Sprintf("[ -f '%s' ] || cp '%s' '%s'", beOrigConfPath, beConfPath, beOrigConfPath),
		fmt.Sprintf("mv '%s' '%s.bak' && cp '%s' '%s'", beConfPath, beConfPath, beOrigConfPath, beConfPath),
		fmt.Sprintf("cat >> '%s' << 'EOF'\n%s\nEOF", beConfPath, beConf),
	}

	// Start BE
	commands = append(commands, fmt.Sprintf("sysctl -w vm.max_map_count=2000000 && ulimit -n 65535 && export JAVA_HOME=%s && sh %s --daemon", dm.config.JavaHome, beStartCmdPath))

	_, err = dm.executeRemoteCommands(ctx, beNode, commands...)
	if err != nil {
		return err
	}

	// Register BE in cluster
	if err := dm.addBackendNode(ctx, dm.config.FENodes[0], beNode); err != nil {
		return fmt.Errorf("failed to register BE %s: %w", beNode, err)
	}

	// Wait BE to be ready
	if err := dm.waitForBEReady(ctx, beNode); err != nil {
		return fmt.Errorf("failed to start BE %s: %w", beNode, err)
	}

	return nil
}

func (dm *DeployManager) pushPackageToNode(ctx context.Context, node, component string) error {
	_, err := dm.executeRemoteCommands(ctx, node, fmt.Sprintf("mkdir -p %s", dm.config.DorisHome))
	if err != nil {
		return err
	}

	localPkgPath := dm.compTarLocalPath(component)
	remotePkgPath := filepath.Join(dm.config.DorisHome, dm.compTarName(component))
	if err := dm.scpPackage(ctx, localPkgPath, remotePkgPath, node); err != nil {
		return fmt.Errorf("failed to execute SCP: %w", err)
	}

	// Extract package
	tempDir := dm.config.dorisHomeTempDir
	unpackDir := filepath.Join(tempDir, component)
	_, err = dm.executeRemoteCommands(ctx, node,
		fmt.Sprintf("cd %s", dm.config.DorisHome),
		fmt.Sprintf("rm -rf %s || true", unpackDir),
		"mkdir -p "+tempDir,
		fmt.Sprintf(
			"tar xzf %s --directory=%s && (rm -rf '%s-bak' || true) && (mv '%s' '%s-bak' || true) && cp -r '%s' .",
			remotePkgPath,
			tempDir,
			component,
			component,
			component,
			unpackDir,
		),
	)
	return err
}

func (dm *DeployManager) scpPackage(ctx context.Context, packagePath, remotePkgPath, nodeip string) error {
	if IsLocalhost(nodeip) {
		// just copy locally
		_, err := dm.executeLocalCommands(ctx, fmt.Sprintf("cp %s %s", packagePath, remotePkgPath))
		return err
	}

	// skip if already exists
	output, err := dm.executeRemoteCommands(ctx, nodeip, fmt.Sprintf("test -f '%s' && echo exists || echo notexists", remotePkgPath))
	if err == nil && strings.TrimSpace(output) == "exists" {
		logrus.Infof("Package %s already exists on %s, skipping upload", remotePkgPath, nodeip)
		return nil
	}

	// upload to temp path first
	remotePkgPathTemp := remotePkgPath + ".uploading"
	err = ScpToRemote(ctx, dm.config.SSHKeyPath, packagePath, fmt.Sprintf("ssh://%s:%s@%s:%d%s", dm.config.SSHUser, dm.config.SSHPassword, nodeip, dm.config.SSHPort, remotePkgPathTemp))
	if err != nil {
		return err
	}

	// rename
	_, err = dm.executeRemoteCommands(ctx, nodeip, fmt.Sprintf("mv '%s' '%s'", remotePkgPathTemp, remotePkgPath))
	return err
}

func (dm *DeployManager) generateFEConf() string {
	conf := `
# Generated by dodo deployment tool
`

	// Add Java configuration
	if dm.config.JavaHome != "" {
		conf += fmt.Sprintf("JAVA_HOME = %s\n", dm.config.JavaHome)
	}
	conf += fmt.Sprintf("priority_networks = %s\n", dm.config.priorityNetworks)

	conf += getConf(dm.config.FEConf)
	return conf
}

func (dm *DeployManager) generateBEConf() string {
	conf := `
# Generated by dodo deployment tool
`

	// Add Java configuration
	if dm.config.JavaHome != "" {
		conf += fmt.Sprintf("JAVA_HOME = %s\n", dm.config.JavaHome)
	}
	conf += fmt.Sprintf("priority_networks = %s\n", dm.config.priorityNetworks)

	conf += getConf(dm.config.BEConf)
	return conf
}

func getConf(conf string) string {
	var result string
	if conf == "" {
		return ""
	}

	// Check if it's a file or key=value pairs
	if _, err := os.Stat(conf); err == nil {
		// It's a file, read it
		if content, err := os.ReadFile(conf); err == nil {
			result += string(content) + "\n"
		}
	} else {
		// It's key=value pairs
		result += conf + "\n"
	}

	return result
}

func (dm *DeployManager) executeRemoteCommands(ctx context.Context, host string, commands ...string) (string, error) {
	// If host is localhost, execute locally
	if IsLocalhost(host) {
		return dm.executeLocalCommands(ctx, commands...)
	}

	// Execute remotely via SSH
	return dm.executeSSHCommands(ctx, host, commands...)
}

func (*DeployManager) executeLocalCommands(ctx context.Context, commands ...string) (string, error) {
	cmd := strings.Join(commands, "\n")
	logrus.Debugf("Executing: %s", cmd)
	output, err := RunCmd(ctx, cmd, false)
	if err != nil {
		logrus.Errorf("Output:\n%s", output)
		return output, fmt.Errorf("local command execution failed: %w", err)
	}
	if len(output) > 0 {
		logrus.Debugf("Output:\n%s", output)
	}
	return output, nil
}

func (dm *DeployManager) executeSSHCommands(_ context.Context, host string, commands ...string) (string, error) {
	sshUrl := fmt.Sprintf("ssh://%s:%s@%s:%d", dm.config.SSHUser, dm.config.SSHPassword, host, dm.config.SSHPort)

	cmd := strings.Join(commands, "\n")
	logrus.Debugf("Executing on %s: %s", host, cmd)

	// Execute commands
	output, stderr, err := SshExec(dm.config.SSHKeyPath, sshUrl, cmd)
	if err != nil {
		logrus.Errorf("Output from %s:\n%s", host, string(stderr))
		return string(stderr), fmt.Errorf("command failed on %s: %w", host, err)
	}

	if len(output) > 0 {
		logrus.Debugf("Output from %s:\n%s", host, string(output))
	}

	return string(output), nil
}

func (dm *DeployManager) waitForFEReady(ctx context.Context, feNode string) error {
	maxRetries := 30
	for i := range maxRetries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}

		// Try to connect to FE
		err := dm.checkFEHealth(ctx, feNode)
		if err == nil {
			logrus.Infof("FE at %s is ready", feNode)
			return nil
		}
		logrus.Infof("FE at %s not ready yet: %v (%d/%d)", feNode, err, i+1, maxRetries)

	}

	return fmt.Errorf("FE at %s did not become ready in time", feNode)
}

func (dm *DeployManager) waitForBEReady(ctx context.Context, beNode string) error {
	maxRetries := 30
	for i := range maxRetries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}

		// Try to connect to BE
		err := dm.checkBEHealth(ctx, beNode)
		if err == nil {
			logrus.Infof("BE at %s is ready", beNode)
			return nil
		}
		logrus.Infof("BE at %s not ready yet: %v (%d/%d)", beNode, err, i+1, maxRetries)
	}

	return fmt.Errorf("BE at %s did not become ready in time", beNode)
}

func (dm *DeployManager) checkFEHealth(ctx context.Context, feNode string) error {
	return dm.checkComponentAlive(ctx, "fe", feNode)
}

func (dm *DeployManager) checkBEHealth(ctx context.Context, beNode string) error {
	return dm.checkComponentAlive(ctx, "be", beNode)
}

func (dm *DeployManager) checkComponentAlive(ctx context.Context, comp, node string) error {
	var component string
	switch comp {
	case "fe":
		component = "frontends"
	case "be":
		component = "backends"
	default:
		return fmt.Errorf("unknown component: %s", comp)
	}

	output, err := RunCmd(ctx, fmt.Sprintf(`mysql -h '%s' -P%d -uroot -e 'show %s\G'`,
		dm.config.FENodes[0],
		dm.config.sqlPort,
		component,
	), false)
	if err != nil {
		return fmt.Errorf("execute SHOW %s failed: %s", component, output)
	}

	// HACK: use go-mysql will cause error: no backends found, so we use mysql cli
	_, after, ok := strings.Cut(output, fmt.Sprintf("Host: %s", node))
	if !ok {
		return fmt.Errorf("%s not found", comp)
	}
	for line := range strings.Lines(strings.SplitN(after, "********", 2)[0]) {
		line = strings.TrimSpace(line)
		if after0, ok0 := strings.CutPrefix(line, "Alive: "); ok0 {
			aliveStr := strings.TrimSpace(after0)
			if aliveStr == "true" {
				return nil
			}
			return fmt.Errorf("%s is not alive", comp)
		}
	}
	return errors.New("alive not found for")
}

// addFollowerNode registers a new FE follower node
func (dm *DeployManager) addFollowerNode(ctx context.Context, masterNode string, followerNode string) error {
	output, err := RunCmd(ctx, fmt.Sprintf(`mysql -h '%s' -P%d -uroot -e 'ALTER SYSTEM ADD FOLLOWER "%s:9010"\G'`,
		masterNode,
		dm.config.sqlPort,
		followerNode,
	), false)
	if err != nil {
		return fmt.Errorf("failed to add follower node %s: %w, %s", followerNode, err, output)
	}

	logrus.Infof("Successfully registered FE follower %s", followerNode)
	return nil
}

// addBackendNode registers a new BE node
func (dm *DeployManager) addBackendNode(ctx context.Context, masterNode string, backendNode string) error {
	output, err := RunCmd(ctx, fmt.Sprintf(`mysql -h '%s' -P%d -uroot -e 'ALTER SYSTEM ADD BACKEND "%s:9050"\G'`,
		masterNode,
		dm.config.sqlPort,
		backendNode,
	), false)
	if err != nil {
		return fmt.Errorf("failed to add backend node %s: %w, %s", backendNode, err, output)
	}

	logrus.Infof("Successfully registered BE node %s", backendNode)
	return nil
}

func (dm *DeployManager) compTarLocalPath(comp string) string {
	return filepath.Join(dm.config.TempDir, dm.compTarName(comp))
}

func (dm *DeployManager) compTarName(comp string) string {
	name := dm.TarBase(dm.config.PackageSource)
	return fmt.Sprintf("dodo-%s-%s.tar.gz", name, comp)
}

func (*DeployManager) TarBase(tarName string) string {
	base := filepath.Base(tarName)
	return strings.TrimSuffix(base, ".tar.gz")
}
