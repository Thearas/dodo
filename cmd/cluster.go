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
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var (
	clusterFENodes []string
	clusterBENodes []string
	clusterFEConf  string
	clusterBEConf  string

	clusterSSHUser     string
	clusterSSHPassword string
	clusterSSHKeyPath  string
	clusterSSHPort     int

	clusterDorisHome string
	clusterJavaHome  string
)

// clusterCmd represents the cluster command
var clusterCmd = &cobra.Command{
	Use:        "cluster",
	Short:      "Deploy and Manage Doris cluster",
	Long:       `Deploy and manage Doris clusters with ease.`,
	SuggestFor: []string{"deploy"},
}

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.PersistentFlags().SortFlags = false
	clusterCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(clusterCmd, "dodo-data-dir", "parallel")

	localip := src.GetLocalIP()
	if localip == "" {
		localip = "127.0.0.1"
	}

	pFlags := clusterCmd.PersistentFlags()

	// FE and BE nodes
	pFlags.StringSliceVar(&clusterFENodes, "fe", []string{localip}, "FE node IP addresses (comma-separated for multiple nodes)")
	pFlags.StringSliceVar(&clusterBENodes, "be", []string{localip}, "BE node IP addresses (comma-separated for multiple nodes)")
	pFlags.StringVar(&clusterFEConf, "fe-conf", "", "FE configuration file or key=value pairs")
	pFlags.StringVar(&clusterBEConf, "be-conf", "", "BE configuration file or key=value pairs")

	// SSH credentials
	pFlags.StringVar(&clusterSSHUser, "ssh-user", "root", "SSH user for remote deployment")
	pFlags.StringVar(&clusterSSHPassword, "ssh-password", "", "SSH password for remote deployment")
	pFlags.StringVar(&clusterSSHKeyPath, "ssh-private-key", src.ExpandHome("~/.ssh/id_rsa"), "SSH private key path for remote deployment")
	pFlags.IntVar(&clusterSSHPort, "ssh-port", 22, "SSH port")

	// Deployment paths
	pFlags.StringVar(&clusterDorisHome, "doris-home", "/root/doris", "Doris installation directory")
	pFlags.StringVar(&clusterJavaHome, "java-home", "/usr/lib/jvm/java-17-openjdk-amd64", "Java home path on FE, BE and Meta Service nodes")
}
