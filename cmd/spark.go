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
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Thearas/dodo/src"
)

var SparkConfig = Spark{}

type Spark struct {
	AdditionalParams []string
	StorageSecretKey string
	SSHPassword      string
	SSHPrivateKey    string
}

// sparkCmd represents the spark command
var sparkCmd = &cobra.Command{
	Use:              "spark",
	Short:            "Run pyspark cli for specific Doris external table",
	TraverseChildren: true,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if slices.Contains([]string{"", "internal"}, GlobalConfig.Catalog) {
			return errors.New("please specify a non-internal catalog via '--catalog' flag")
		}
		if err := completeDBTables("please specify exactly one table via '--tables/--table' flag"); err != nil {
			return err
		}
		if len(GlobalConfig.Tables) != 1 {
			return errors.New("please specify exactly one table via '--tables/--table' flag")
		}
		table := GlobalConfig.Tables[0]
		GlobalConfig.DBs = []string{strings.Split(table, ".")[0]}

		conn, err := connectDBWithoutDBName()
		if err != nil {
			return err
		}
		createCatalogStmt, err := src.ShowCreateCatalog(ctx, conn, GlobalConfig.Catalog)
		if err != nil {
			return err
		}
		createTableStmt, err := src.ShowCreateTables(ctx, conn, false, GlobalConfig.DBs[0], table)
		if err != nil {
			return err
		}
		if len(createTableStmt) == 0 {
			return fmt.Errorf("table %s does not exist", table)
		}

		sshUrl, err := url.Parse(fmt.Sprintf("ssh://root@%s:22", GlobalConfig.DBHost))
		if err != nil {
			return err
		}
		user := sshUrl.User
		sshUrl.User = url.UserPassword(user.Username(), SparkConfig.SSHPassword)

		sparkCli, err := src.NewSparkCli(
			ctx, conn, GlobalConfig.DBHost,
			createCatalogStmt, createTableStmt[0].CreateStmt,
			SparkConfig.StorageSecretKey,
			sshUrl.String(), SparkConfig.SSHPrivateKey,
			GlobalConfig.DodoDataDir, GlobalConfig.Parallel,
		)
		if err != nil {
			return err
		}
		sparkCmd, err := sparkCli.GetCmd(SparkConfig.AdditionalParams...)
		if err != nil {
			return err
		}
		logrus.Debugln(sparkCmd)

		if GlobalConfig.DryRun {
			return nil
		}

		_, err = src.RunCmd(ctx, sparkCmd, true)

		return err
	},
}

func init() {
	rootCmd.AddCommand(sparkCmd)
	sparkCmd.PersistentFlags().SortFlags = false
	sparkCmd.Flags().SortFlags = false
	setVisibleRootHelpFlags(sparkCmd, "dodo-data-dir", "dry-run", "parallel", "host", "port", "user", "password", "catalog", "dbs", "tables")

	pFlags := sparkCmd.PersistentFlags()
	pFlags.StringSliceVarP(&SparkConfig.AdditionalParams, "params", "p", []string{}, "Additional parameters for pyspark cli")
	pFlags.StringVarP(&SparkConfig.StorageSecretKey, "storage-secret-key", "s", "", "Storage secret key (like for s3/oss/...) for pyspark cli")
	pFlags.StringVar(&SparkConfig.SSHPassword, "ssh-password", "", "SSH password for DB host")
	pFlags.StringVar(&SparkConfig.SSHPrivateKey, "ssh-private-key", "~/.ssh/id_rsa", "File path of SSH private key for DB host")
}
