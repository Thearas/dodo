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
package cmd

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	// FDB nodes and configuration
	cloudDeployFDBNodes       []string
	cloudDeployFDBHome        string
	cloudDeployFDBDataDirs    []string
	cloudDeployFDBClusterID   string
	cloudDeployFDBClusterDesc string

	// Meta Service configuration
	cloudDeployMsNodes []string
	cloudDeployMsPort  int

	cloudDeployClusterID string

	// OSS credentials
	cloudDeployOSSEndpoint        string
	cloudDeployOSSAccessKeyID     string
	cloudDeployOSSAccessKeySecret string
)

// clusterCloudDeployCmd represents the cluster deploy-cloud command
var clusterCloudDeployCmd = &cobra.Command{
	Use:   "deploy-cloud",
	Short: "(Re)Deploy Doris cluster in separating storage-compute mode",
	Long: `(Re)Deploy a new Doris cluster in separating storage-compute (cloud) mode with Meta Service and optional FoundationDB.

Example:

  # Deploy 1FDB, 1 MS, 1 FE and 1 BE
  dodo cluster deploy-cloud ./doris-package.tar.gz

  # Deploy 1 MS, 1 FE and 1 BE, with external FDB cluster
  dodo cluster deploy-cloud ./doris-package.tar.gz \
    --fdb-cluster-string "xxx:yyy@127.0.0.1:4500"
`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("exactly one Doris package is required")
		}

		packageSource := src.ReplaceOSSUri(args[0])
		localip := src.GetLocalIP()
		if localip == "" {
			localip = "127.0.0.1"
		}

		// Parse FE and BE nodes
		feNodes := []string{localip}
		beNodes := []string{localip}
		msNodes := []string{localip}
		fdbNodes := []string{}

		if len(clusterFENodes) > 0 {
			feNodes = nil
			for _, node := range clusterFENodes {
				feNodes = append(feNodes, strings.Split(node, ",")...)
			}
		}
		if len(clusterBENodes) > 0 {
			beNodes = nil
			for _, node := range clusterBENodes {
				beNodes = append(beNodes, strings.Split(node, ",")...)
			}
		}
		if len(cloudDeployMsNodes) > 0 {
			msNodes = nil
			for _, node := range cloudDeployMsNodes {
				msNodes = append(msNodes, strings.Split(node, ",")...)
			}
		}
		if len(cloudDeployFDBNodes) > 0 {
			logrus.Fatalln("Not yet support multiple FDB deployment")
			for _, node := range cloudDeployFDBNodes {
				fdbNodes = append(fdbNodes, strings.Split(node, ",")...)
			}
		} else {
			fdbNodes = []string{msNodes[0]}
		}

		replaceLocalIP := func(nodes []string) []string {
			return lo.Map(nodes, func(node string, _ int) string {
				if src.IsLocalhost(node) {
					return localip
				}
				return node
			})
		}

		fdbDataDirs := []string{}
		if len(cloudDeployFDBDataDirs) > 0 {
			for _, dir := range cloudDeployFDBDataDirs {
				fdbDataDirs = append(fdbDataDirs, strings.Split(dir, ",")...)
			}
		}

		// Create cloud deployment configuration
		config := &src.CloudDeployConfig{
			DeployConfig: src.DeployConfig{
				PackageSource:      packageSource,
				FENodes:            lo.Uniq(replaceLocalIP(feNodes)),
				BENodes:            lo.Uniq(replaceLocalIP(beNodes)),
				FEConf:             clusterFEConf,
				BEConf:             clusterBEConf,
				SSHUser:            clusterSSHUser,
				SSHPassword:        clusterSSHPassword,
				SSHKeyPath:         clusterSSHKeyPath,
				SSHPort:            clusterSSHPort,
				DorisHome:          clusterDorisHome,
				JavaHome:           clusterJavaHome,
				OSSEndpoint:        cloudDeployOSSEndpoint,
				OSSAccessKeyID:     cloudDeployOSSAccessKeyID,
				OSSAccessKeySecret: cloudDeployOSSAccessKeySecret,
				TempDir:            filepath.Join(GlobalConfig.DodoDataDir, "deploy"),
				Parallel:           GlobalConfig.Parallel,
			},
			ClusterID:      cloudDeployClusterID,
			FDBNodes:       lo.Uniq(replaceLocalIP(fdbNodes)),
			FDBHome:        cloudDeployFDBHome,
			FDBDataDirs:    fdbDataDirs,
			FDBClusterID:   cloudDeployFDBClusterID,
			FDBClusterDesc: cloudDeployFDBClusterDesc,
			MsNodes:        lo.Uniq(replaceLocalIP(msNodes)),
			MsPort:         cloudDeployMsPort,
		}

		// Create cloud deployment manager and deploy
		manager := src.NewCloudDeployManager(config)
		return manager.Deploy(cmd.Context())
	},
}

func init() {
	clusterCmd.AddCommand(clusterCloudDeployCmd)
	clusterCloudDeployCmd.PersistentFlags().SortFlags = false
	clusterCloudDeployCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(clusterCloudDeployCmd, "dodo-data-dir", "parallel")

	localip := src.GetLocalIP()
	if localip == "" {
		localip = "127.0.0.1"
	}

	pFlags := clusterCloudDeployCmd.PersistentFlags()

	// FoundationDB configuration
	pFlags.StringSliceVar(&cloudDeployFDBNodes, "fdb", []string{}, "FoundationDB cluster IPs, by default equal to the first MS node")
	pFlags.StringVar(&cloudDeployFDBHome, "fdb-home", "", "FoundationDB home directory (default '<doris-home>/fdb')")
	pFlags.StringSliceVar(&cloudDeployFDBDataDirs, "fdb-data-dirs", []string{}, "FoundationDB data directories (default '<doris-home>/fdb/data')")
	pFlags.StringVar(&cloudDeployFDBClusterID, "fdb-cluster-id", "dodoFDBClusterID", "FoundationDB cluster ID (e.g., SAQESzbh)")
	pFlags.StringVar(&cloudDeployFDBClusterDesc, "fdb-cluster-desc", "dodoDorisfdb", "FoundationDB cluster description")

	// Meta Service configuration
	pFlags.StringSliceVar(&cloudDeployMsNodes, "ms", []string{localip}, "Meta Service node IP addresses (comma-separated for multiple nodes)")
	pFlags.IntVar(&cloudDeployMsPort, "ms-port", 5000, "Meta Service listening port")

	// FE and BE configuration
	pFlags.StringVar(&cloudDeployClusterID, "cluster-id", "", "Doris cluster ID (auto-generated if not specified)")

	// OSS credentials
	pFlags.StringVar(&cloudDeployOSSEndpoint, "oss-endpoint", "https://oss-cn-beijing.aliyuncs.com", "OSS endpoint")
	pFlags.StringVar(&cloudDeployOSSAccessKeyID, "oss-access-key", "", "OSS access key ID")
	pFlags.StringVar(&cloudDeployOSSAccessKeySecret, "oss-secret-key", "", "OSS access key secret")
}
