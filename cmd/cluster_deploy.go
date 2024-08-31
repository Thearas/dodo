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
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	deployOSSEndpoint        string
	deployOSSAccessKeyID     string
	deployOSSAccessKeySecret string
)

// clusterDeployCmd represents the cluster deploy command
var clusterDeployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "(Re)Deploy Doris cluster",
	Long: `(Re)Deploy a new Doris cluster with specified package.
Example:

  # Deploy 1 fe and 1 be locally
  dodo cluster deploy ./doris-package.tar.gz

  dodo cluster deploy https://example.com/doris-package.tar.gz

  # Specify configuration for FE and BE, both literal and via file are OK
  dodo cluster deploy oss://my-bucket/doris-package.tar.gz --fe-conf 'k=v' --be-conf be.conf

  # Deploy 2 fe and 3 be to remote hosts with SSH password authentication
  dodo cluster deploy oss://my-bucket/doris-package.tar.gz --ssh-password xxx --fe 10.0.0.1,10.0.0.2 --be 10.0.0.3,10.0.0.4,10.0.0.5
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

		replaceLocalIP := func(nodes []string) []string {
			return lo.Map(nodes, func(node string, _ int) string {
				if src.IsLocalhost(node) {
					return localip
				}
				return node
			})
		}

		// Create deployment configuration
		config := &src.DeployConfig{
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
			OSSEndpoint:        deployOSSEndpoint,
			OSSAccessKeyID:     deployOSSAccessKeyID,
			OSSAccessKeySecret: deployOSSAccessKeySecret,
			TempDir:            filepath.Join(GlobalConfig.DodoDataDir, "deploy"),
			Parallel:           GlobalConfig.Parallel,
		}

		// Create deployment manager and deploy
		manager := src.NewDeployManager(config)
		return manager.Deploy(cmd.Context())
	},
}

func init() {
	clusterCmd.AddCommand(clusterDeployCmd)
	clusterDeployCmd.PersistentFlags().SortFlags = false
	clusterDeployCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(clusterDeployCmd, "dodo-data-dir", "parallel")

	pFlags := clusterDeployCmd.PersistentFlags()
	pFlags.StringVar(&deployOSSEndpoint, "oss-endpoint", "https://oss-cn-beijing.aliyuncs.com", "OSS endpoint")
	pFlags.StringVar(&deployOSSAccessKeyID, "oss-access-key", "", "OSS access key ID")
	pFlags.StringVar(&deployOSSAccessKeySecret, "oss-secret-key", "", "OSS access key secret")
}
