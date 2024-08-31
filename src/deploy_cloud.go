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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

// CloudDeployConfig contains configuration for separating storage-compute Doris cluster deployment
type CloudDeployConfig struct {
	DeployConfig

	// FoundationDB (FDB) configuration
	FDBNodes         []string // FDB cluster node IPs
	FDBHome          string   // FDB home directory
	FDBDataDirs      []string // Data directories for FDB (comma-separated paths on each node)
	FDBClusterID     string   // Cluster ID for FDB (e.g., SAQESzbh)
	FDBClusterDesc   string   // Description of the FDB cluster
	FDBMemoryLimitGB int      // Memory limit for FDB in GB

	// Meta Service configuration
	MsNodes          []string // Meta Service node IPs
	MsPort           int      // Meta Service listening port
	FDBClusterString string   // FDB cluster connection string (e.g., xxx:yyy@127.0.0.1:4500)

	// FE cluster configuration for cloud mode
	ClusterID string // Unique cluster ID for the Doris cluster
}

// CloudDeployManager manages the Doris cluster deployment in cloud (storage-compute separation) mode
type CloudDeployManager struct {
	DeployManager
	config *CloudDeployConfig
}

// NewCloudDeployManager creates a new cloud deployment manager
func NewCloudDeployManager(config *CloudDeployConfig) *CloudDeployManager {
	if len(config.MsNodes) == 0 {
		logrus.Fatalln("At least one Meta Service node is required")
	}

	if config.FDBHome == "" {
		config.FDBHome = filepath.Join(config.DorisHome, "fdb")
	}
	if len(config.FDBDataDirs) == 0 {
		config.FDBDataDirs = []string{filepath.Join(config.FDBHome, "data")}
	}

	if config.MsPort == 0 {
		config.MsPort = 5000
	}
	return &CloudDeployManager{
		DeployManager: *NewDeployManager(&config.DeployConfig),
		config:        config,
	}
}

// Deploy deploys a separating storage-compute Doris cluster
func (cdm *CloudDeployManager) Deploy(ctx context.Context) error {
	// Generate cluster ID if not provided
	if cdm.config.ClusterID == "" {
		cdm.config.ClusterID = fmt.Sprintf("%d", os.Getpid()%1000000)
		logrus.Infof("Using generated cluster ID: %s", cdm.config.ClusterID)
	}

	// Prepare CIDR for priority_networks
	allnodes := append(append(cdm.config.FENodes, cdm.config.BENodes...), cdm.config.MsNodes...)
	if len(cdm.config.FDBNodes) > 0 {
		allnodes = append(allnodes, cdm.config.FDBNodes...)
	}
	allnodes = lo.Uniq(allnodes)
	cidr, err := GetCIDR(allnodes...)
	if err != nil {
		return fmt.Errorf("failed to get CIDR: %w", err)
	}
	cdm.config.priorityNetworks = strings.Join(cidr, ",")

	// Check JAVA_HOME on each machine
	if err := cdm.checkJavaHome(ctx, allnodes); err != nil {
		return err
	}

	// Step 1: Extract/Download package
	if err := cdm.preparePackage(ctx, true); err != nil {
		return fmt.Errorf("failed to prepare package: %w", err)
	}
	ShutdownMultiProgressBar()

	logrus.Infof("Deploy Doris cloud cluster:")
	if len(cdm.config.FDBNodes) > 0 {
		logrus.Infof("  FDB nodes: %v", cdm.config.FDBNodes)
	}
	logrus.Infof("  Meta Service nodes: %v", cdm.config.MsNodes)
	logrus.Infof("  FE nodes: %v", cdm.config.FENodes)
	logrus.Infof("  BE nodes: %v", cdm.config.BENodes)

	// Step 2: Deploy FoundationDB (if FDB nodes are provided)
	if len(cdm.config.FDBNodes) > 0 && cdm.config.FDBClusterString == "" {
		if cdm.config.FDBClusterID == "" {
			return errors.New("FDB cluster ID is required when deploying FDB")
		}
		logrus.Infof("Deploying FoundationDB cluster")
		if err := cdm.deployFDB(ctx); err != nil {
			return fmt.Errorf("failed to deploy FoundationDB: %w", err)
		}
		ShutdownMultiProgressBar()

		// Get FDB cluster string
		fdbClusterString, err := cdm.getFDBClusterString(ctx)
		if err != nil {
			return fmt.Errorf("failed to get FDB cluster string: %w", err)
		}
		cdm.config.FDBClusterString = fdbClusterString
		logrus.Infof("FDB cluster string: %s", fdbClusterString)
	} else {
		logrus.Infof("Skipping FDB deployment (using external FDB cluster string)")
	}

	_ = os.Unsetenv("http_proxy")
	_ = os.Unsetenv("https_proxy")

	// Step 3: Deploy Meta Service
	logrus.Infof("Deploying Meta Service")
	if err := cdm.deployMs(ctx); err != nil {
		return fmt.Errorf("failed to deploy Meta Service: %w", err)
	}
	ShutdownMultiProgressBar()

	// Wait for Meta Service to be ready
	if err := cdm.waitForMsReady(ctx); err != nil {
		logrus.Warnf("Meta Service readiness check failed: %v (continuing anyway)", err)
	}

	// Step 4: Deploy FE Master
	masterFENode := cdm.config.FENodes[0]
	logrus.Infof("Deploying FE Master on %s", masterFENode)

	feConf := cdm.generateFEConf()
	if err := cdm.deployFEMaster(ctx, masterFENode, feConf); err != nil {
		return fmt.Errorf("failed to deploy FE Master: %w", err)
	}
	ShutdownMultiProgressBar()

	// Wait for FE Master to be ready
	if err := cdm.waitForFEReady(ctx, masterFENode); err != nil {
		return fmt.Errorf("FE Master failed to start: %w", err)
	}

	// Step 5: Deploy additional FE nodes (Followers)
	feFollowers := cdm.config.FENodes[1:]
	g := ParallelGroup(-1)
	for _, feNode := range feFollowers {
		g.Go(func() error {
			logrus.Infof("Deploying FE Follower on %s", feNode)

			if err := cdm.deployFEFollower(ctx, feNode, feConf, masterFENode); err != nil {
				logrus.Errorf("failed to deploy FE Follower on %s: %v", feNode, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	ShutdownMultiProgressBar()

	// Step 6: Deploy BE nodes
	g = ParallelGroup(-1)
	beConf := cdm.generateBEConf()
	for _, beNode := range cdm.config.BENodes {
		g.Go(func() error {
			logrus.Infof("Deploying BE on %s", beNode)

			if err := cdm.deployBE(ctx, beNode, beConf); err != nil {
				logrus.Errorf("failed to deploy BE on %s: %v", beNode, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	ShutdownMultiProgressBar()

	logrus.Infoln("Doris cloud cluster deployment completed successfully!")
	logrus.Infof("Connect: mysql -h %s -P %d -uroot", masterFENode, cdm.config.sqlPort)

	cdm.teardown(ctx, allnodes)
	return nil
}

// deployFDB deploys the FoundationDB cluster
func (cdm *CloudDeployManager) deployFDB(ctx context.Context) error {
	if len(cdm.config.FDBNodes) == 0 {
		return errors.New("no FDB nodes specified")
	}

	// Step 1: Push tools package to FDB nodes
	// First, we need to extract tools from the package
	logrus.Infof("Preparing FDB tools")

	g := ParallelGroup(-1)
	for _, fdbNode := range cdm.config.FDBNodes {
		g.Go(func() error {
			if err := cdm.pushPackageToNode(ctx, fdbNode, "tools"); err != nil {
				return fmt.Errorf("failed to push tools package to %s: %w", fdbNode, err)
			}

			// Configure and deploy FDB
			if err := cdm.setupFDBNode(ctx, fdbNode); err != nil {
				return fmt.Errorf("failed to setup FDB on %s: %w", fdbNode, err)
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Step 2: Deploy FDB cluster
	firstNode := cdm.config.FDBNodes[0]
	logrus.Infof("Download and deploy FDB cluster to %s", firstNode)

	_, err := cdm.executeRemoteCommands(ctx, firstNode,
		fmt.Sprintf("cd %s && bash fdb_ctl.sh deploy", cdm.getFDBToolsDir()),
	)
	if err != nil {
		return fmt.Errorf("failed to deploy FDB cluster: %w", err)
	}

	// Step 3: Start FDB cluster
	logrus.Infof("Starting FDB cluster to %s", firstNode)
	_, err = cdm.executeRemoteCommands(ctx, firstNode,
		fmt.Sprintf("cd %s && bash fdb_ctl.sh start", cdm.getFDBToolsDir()),
	)
	if err != nil {
		return fmt.Errorf("failed to start FDB cluster: %w", err)
	}

	return nil
}

// setupFDBNode configures FDB on a single node
func (cdm *CloudDeployManager) setupFDBNode(ctx context.Context, fdbNode string) error {
	fdbToolsDir := cdm.getFDBToolsDir()
	fdbCtlPath := filepath.Join(fdbToolsDir, "fdb_ctl.sh")
	fdbVarsPath := filepath.Join(fdbToolsDir, "fdb_vars.sh")
	fdbOrigVarsPath := filepath.Join(fdbToolsDir, "fdb_vars.sh.dodo.orig")

	// Generate fdb_vars.sh script
	fdbVarsContent := cdm.generateFDBVars()

	// Create fdb_vars.sh on the node
	_, err := cdm.executeRemoteCommands(ctx, fdbNode,
		// stop and clean fdb
		fmt.Sprintf("bash %s stop all || true", fdbCtlPath),
		fmt.Sprintf("bash %s clean all || true", fdbCtlPath),

		// backup vars
		fmt.Sprintf("[ -f '%s' ] || cp '%s' '%s'", fdbOrigVarsPath, fdbVarsPath, fdbOrigVarsPath),
		fmt.Sprintf("mv '%s' '%s.bak' && cp '%s' '%s'", fdbVarsPath, fdbVarsPath, fdbOrigVarsPath, fdbVarsPath),
		fmt.Sprintf("cat >> '%s' << 'EOF'\n%s\nEOF", fdbVarsPath, fdbVarsContent),
		fmt.Sprintf("chmod +x '%s'", fdbVarsPath),
	)
	return err
}

// generateFDBVars generates the fdb_vars.sh configuration script
func (cdm *CloudDeployManager) generateFDBVars() string {
	clusterIPs := strings.Join(cdm.config.FDBNodes, ",")
	dataDirs := strings.Join(cdm.config.FDBDataDirs, ",")

	memoryLimit := cdm.config.FDBMemoryLimitGB
	if memoryLimit == 0 {
		memoryLimit = 2
	}

	return fmt.Sprintf(`#!/bin/bash
# Generated by dodo deployment tool

export FDB_HOME=%s
export FDB_CLUSTER_IPS=%s
export FDB_CLUSTER_ID=%s
export FDB_CLUSTER_DESC=%s
export DATA_DIRS=%s
export CPU_CORES_LIMIT=$(nproc)
export MEMORY_LIMIT_GB=%d

# Create data directories if they don't exist
for dir in $(echo "$DATA_DIRS" | tr ',' ' '); do
  mkdir -p "$dir"
done
`,
		cdm.config.FDBHome,
		clusterIPs,
		cdm.config.FDBClusterID,
		cdm.config.FDBClusterDesc,
		dataDirs,
		memoryLimit,
	)
}

// getFDBClusterString retrieves the FDB cluster connection string from the deployed cluster
func (cdm *CloudDeployManager) getFDBClusterString(ctx context.Context) (string, error) {
	firstNode := cdm.config.FDBNodes[0]
	fdbConfPath := filepath.Join(cdm.config.FDBHome, "conf", "fdb.cluster")

	output, err := cdm.executeRemoteCommands(ctx, firstNode,
		fmt.Sprintf("tail -1 '%s'", fdbConfPath),
	)
	if err != nil {
		return "", fmt.Errorf("failed to get FDB cluster string: %w", err)
	}

	clusterString := strings.TrimSpace(output)
	if clusterString == "" {
		return "", errors.New("FDB cluster string is empty")
	}

	return clusterString, nil
}

// deployMs deploys the Meta Service component
func (cdm *CloudDeployManager) deployMs(ctx context.Context) error {
	if len(cdm.config.MsNodes) == 0 {
		return errors.New("no Meta Service nodes specified")
	}

	if cdm.config.FDBClusterString == "" {
		return errors.New("FDB cluster string is required for Meta Service")
	}

	g := ParallelGroup(-1)
	for _, msNode := range cdm.config.MsNodes {
		g.Go(func() error {
			logrus.Infof("Deploying Meta Service on %s", msNode)

			if err := cdm.deployMsNode(ctx, msNode); err != nil {
				return fmt.Errorf("failed to deploy Meta Service on %s: %w", msNode, err)
			}

			return nil
		})
	}
	return g.Wait()
}

// deployMsNode deploys Meta Service on a single node
func (cdm *CloudDeployManager) deployMsNode(ctx context.Context, msNode string) error {
	msHome := filepath.Join(cdm.config.DorisHome, "ms")
	msConfPath := filepath.Join(msHome, "conf", "doris_cloud.conf")
	msOrigConfPath := filepath.Join(msHome, "conf", "doris_cloud.conf.dodo.orig")
	msStartCmd := filepath.Join(msHome, "bin", "start.sh")
	msStopCmd := filepath.Join(msHome, "bin", "stop.sh")

	if err := cdm.pushPackageToNode(ctx, msNode, "ms"); err != nil {
		return fmt.Errorf("failed to push ms package to %s: %w", msNode, err)
	}

	// Generate Meta Service configuration
	msConf := cdm.generateMsConf()

	_, err := cdm.executeRemoteCommands(ctx, msNode,
		"set +e",
		fmt.Sprintf("bash '%s' || true", msStopCmd),
		// Backup conf
		fmt.Sprintf("[ -f '%s' ] || cp '%s' '%s'", msOrigConfPath, msConfPath, msOrigConfPath),
		fmt.Sprintf("mv '%s' '%s.bak' && cp '%s' '%s'", msConfPath, msConfPath, msOrigConfPath, msConfPath),
		fmt.Sprintf("cat >> '%s' << 'EOF'\n%s\nEOF", msConfPath, msConf),
		// Start Meta Service
		fmt.Sprintf("export JAVA_HOME=%s && bash '%s' --daemon", cdm.config.JavaHome, msStartCmd),
		"true",
	)
	return err
}

// generateMsConf generates Meta Service configuration
func (cdm *CloudDeployManager) generateMsConf() string {
	return fmt.Sprintf(`
# Generated by dodo deployment tool
brpc_listen_port = %d
fdb_cluster = %s
`, cdm.config.MsPort, cdm.config.FDBClusterString)
}

// generateFEConf generates FE configuration for cloud mode
func (cdm *CloudDeployManager) generateFEConf() string {
	conf := `
# Generated by dodo deployment tool
deploy_mode = cloud
`

	conf += fmt.Sprintf("meta_service_endpoint = %s\n", net.JoinHostPort(cdm.config.MsNodes[0], strconv.Itoa(cdm.config.MsPort)))
	if cdm.config.ClusterID != "" {
		conf += fmt.Sprintf("cluster_id = %s\n", cdm.config.ClusterID)
	}

	if cdm.config.JavaHome != "" {
		conf += fmt.Sprintf("JAVA_HOME = %s\n", cdm.config.JavaHome)
	}
	conf += fmt.Sprintf("priority_networks = %s\n", cdm.config.priorityNetworks)

	conf += getConf(cdm.config.FEConf)
	return conf
}

// generateBEConf generates BE configuration for cloud mode
func (cdm *CloudDeployManager) generateBEConf() string {
	conf := `
# Generated by dodo deployment tool
deploy_mode = cloud
`

	if cdm.config.JavaHome != "" {
		conf += fmt.Sprintf("JAVA_HOME = %s\n", cdm.config.JavaHome)
	}
	conf += fmt.Sprintf("priority_networks = %s\n", cdm.config.priorityNetworks)

	conf += getConf(cdm.config.BEConf)
	return conf
}

func (cdm *CloudDeployManager) waitForMsReady(ctx context.Context) error {
	maxRetries := 15
	for i := range maxRetries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}

		// Check if Meta Service is responding on at least one node
		for _, msNode := range cdm.config.MsNodes {
			if IsPortOpen(ctx, msNode, cdm.config.MsPort) {
				logrus.Infof("Meta Service %s is ready", msNode)
				return nil
			}
		}

		logrus.Infof("Meta Service not ready yet (%d/%d)", i+1, maxRetries)
	}

	return errors.New("meta service did not become ready in time")
}

func (cdm *CloudDeployManager) getFDBToolsDir() string {
	return filepath.Join(cdm.config.DorisHome, "tools", "fdb")
}
