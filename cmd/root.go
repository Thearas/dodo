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
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	GlobalConfig    = Global{}
	DefaultParallel = 10
)

type Global struct {
	ConfigFile  string
	LogLevel    string
	DodoDataDir string
	OutputDir   string
	DryRun      bool
	Parallel    int

	DBHost     string
	DBPort     uint16
	HTTPPort   uint16
	DBUser     string
	DBPassword string
	Catalog    string
	DBs        []string
	Tables     []string
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "dodo",
	Short: "Dump and replay queries from database. Also a powerful fake data generator.",
	Long: `
Dump and replay queries from database. Also a powerful fake data generator.

You may want to pass config by '$HOME/.dodo.yaml',
or environment variables with prefix 'DORIS_', e.g.
    DORIS_HOST=xxx
    DORIS_PORT=9030
	`,
	Example:          "dodo dump --help",
	SuggestFor:       []string{"dump", "replay"},
	ValidArgs:        []string{"completion", "help", "clean", "dump", "anonymize", "replay", "diff"},
	TraverseChildren: true,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		return cmd.Usage()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1) //nolint:revive
	}
}

func init() {
	rootCmd.PersistentFlags().SortFlags = false
	rootCmd.Flags().SortFlags = false

	parallel := runtime.NumCPU()
	if parallel > DefaultParallel {
		parallel = DefaultParallel
	}

	pFlags := rootCmd.PersistentFlags()
	pFlags.StringVarP(&GlobalConfig.ConfigFile, "config", "C", "", "Config file (default is $HOME/.dodo.yaml)")
	pFlags.StringVarP(&GlobalConfig.LogLevel, "log-level", "L", "info", "Log level, one of: trace, debug, info, warn")
	pFlags.StringVar(&GlobalConfig.DodoDataDir, "dodo-data-dir", "./.dodo/", "Directory for storing dodo self data")
	pFlags.StringVarP(&GlobalConfig.OutputDir, "output", "O", "./output/", "Directory for storing dump sql and replay result")
	pFlags.BoolVar(&GlobalConfig.DryRun, "dry-run", false, "Dry run")
	pFlags.IntVar(&GlobalConfig.Parallel, "parallel", parallel, "Parallel dump worker")

	pFlags.StringVarP(&GlobalConfig.DBHost, "host", "H", "127.0.0.1", "DB Host")
	pFlags.Uint16VarP(&GlobalConfig.DBPort, "port", "P", 9030, "DB Port")
	pFlags.Uint16Var(&GlobalConfig.HTTPPort, "http-port", 8030, "FE HTTP Port")
	pFlags.StringVarP(&GlobalConfig.DBUser, "user", "U", "root", "DB User")
	pFlags.StringVar(&GlobalConfig.DBPassword, "password", "", "DB password")
	pFlags.StringVar(&GlobalConfig.Catalog, "catalog", "", "Catalog to work on")
	pFlags.StringSliceVarP(&GlobalConfig.DBs, "dbs", "D", []string{}, "DBs to work on")
	pFlags.StringSliceVarP(&GlobalConfig.Tables, "tables", "T", []string{}, "Tables to work on")
}

// initConfig reads in config file and ENV variables if set.
func initConfig(cmd *cobra.Command, prefixs ...string) error {
	cfgFile := GlobalConfig.ConfigFile
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".dodo" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".dodo")
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	prefix := "DORIS"
	if len(prefixs) > 0 {
		prefix = strings.Join(append([]string{prefix}, prefixs...), "_")
		prefix = strings.ToUpper(prefix)
	}
	viper.SetEnvPrefix(prefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	} else if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		return err
	}

	bindFlags(cmd, viper.GetViper(), prefixs...)

	if err := initLog(); err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "")
	return nil
}

// Bind each cobra flag to its associated viper configuration (config file and environment variable)
func bindFlags(cmd *cobra.Command, v *viper.Viper, prefixs ...string) {
	prefix := ""
	if len(prefixs) > 0 {
		prefix = strings.Join(prefixs, ".") + "."
	}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Determine the naming convention of the flags when represented in the config file
		configName := prefix + f.Name
		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if !f.Changed && v.IsSet(configName) {
			val := v.Get(configName)

			vals := []any{val}
			if reflect.TypeOf(val).Kind() == reflect.Slice {
				var ok bool
				vals, ok = val.([]any)
				if !ok {
					panic(fmt.Sprintf("unexpected type %T for %s", val, configName))
				}
			}

			flags := cmd.Flags()
			for _, val := range vals {
				flags.Set(f.Name, fmt.Sprintf("%v", val))
			}
		}
	})
}

func initLog() error {
	logLevel, err := logrus.ParseLevel(GlobalConfig.LogLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %s, err: %v", GlobalConfig.LogLevel, err)
	}

	logrus.SetLevel(logLevel)
	logrus.SetOutput(os.Stderr)
	logrus.SetFormatter(&logrus.TextFormatter{})
	return nil
}

func completeDBTables(dbtableNotFoundErr ...string) error {
	GlobalConfig.DBs, GlobalConfig.Tables = lo.Uniq(GlobalConfig.DBs), lo.Uniq(GlobalConfig.Tables)
	dbs, tables := GlobalConfig.DBs, GlobalConfig.Tables
	if len(dbs) == 0 && len(tables) == 0 {
		if len(dbtableNotFoundErr) > 0 {
			return errors.New(dbtableNotFoundErr[0])
		}
		return errors.New("expected at least one database or tables, please use --dbs/--tables flag or --ddl flag with '.sql' file(s)")
	} else if len(dbs) == 1 {
		// prepend default database if only one database specified
		prefix := dbs[0] + "."
		for i, t := range GlobalConfig.Tables {
			if !strings.Contains(t, ".") {
				GlobalConfig.Tables[i] = prefix + t
			}
		}
	} else {
		for _, t := range tables {
			if !strings.Contains(t, ".") {
				return errors.New("expected database in table name when zero/multiple databases specified, e.g. --tables db1.table1,db2.table2")
			}
		}
	}
	return nil
}
