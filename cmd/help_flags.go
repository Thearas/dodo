package cmd

import "github.com/spf13/cobra"

var rootHelpGlobalFlagNames = []string{
	"config",
	"log-level",
	"dodo-data-dir",
	"output",
	"dry-run",
	"parallel",
	"host",
	"port",
	"http-port",
	"user",
	"password",
	"catalog",
	"dbs",
	"tables",
}

var alwaysVisibleRootHelpFlags = []string{"config", "log-level"}

func setVisibleRootHelpFlags(cmd *cobra.Command, visible ...string) {
	visibleSet := map[string]struct{}{}
	for _, name := range alwaysVisibleRootHelpFlags {
		visibleSet[name] = struct{}{}
	}
	for _, name := range visible {
		visibleSet[name] = struct{}{}
	}

	baseHelp := cmd.HelpFunc()
	cmd.SetHelpFunc(func(c *cobra.Command, args []string) {
		restore := hideUnusedRootHelpFlags(c, visibleSet)
		defer restore()

		baseHelp(c, args)
	})
}

func hideUnusedRootHelpFlags(cmd *cobra.Command, visible map[string]struct{}) func() {
	cmd.InheritedFlags()
	flags := cmd.Flags()
	originalHidden := map[string]bool{}

	for _, name := range rootHelpGlobalFlagNames {
		flag := flags.Lookup(name)
		if flag == nil {
			continue
		}

		originalHidden[name] = flag.Hidden
		if _, ok := visible[name]; ok {
			continue
		}

		_ = flags.MarkHidden(name)
	}

	return func() {
		for name, hidden := range originalHidden {
			flag := flags.Lookup(name)
			if flag == nil {
				continue
			}
			flag.Hidden = hidden
		}
	}
}
