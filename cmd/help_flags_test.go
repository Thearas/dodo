package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandHelpRootFlagVisibility(t *testing.T) {
	testCases := []struct {
		name    string
		path    []string
		visible []string
		hidden  []string
	}{
		{
			name:    "anonymize hides unrelated root flags",
			path:    []string{"anonymize"},
			visible: []string{"--config string", "--log-level string"},
			hidden:  []string{"--output string", "--host string", "--tables strings"},
		},
		{
			name:    "dump keeps used root flags but hides http port",
			path:    []string{"dump"},
			visible: []string{"--output string", "--host string", "--tables strings"},
			hidden:  []string{"--http-port uint16"},
		},
		{
			name:    "import keeps all used root flags",
			path:    []string{"import"},
			visible: []string{"--output string", "--http-port uint16", "--tables strings"},
		},
		{
			name:    "replay hides table and http port",
			path:    []string{"replay"},
			visible: []string{"--catalog string", "--dbs strings", "--output string"},
			hidden:  []string{"--tables strings", "--http-port uint16"},
		},
		{
			name:    "cluster parent keeps subtree root flags only",
			path:    []string{"cluster"},
			visible: []string{"--dodo-data-dir string", "--parallel int"},
			hidden:  []string{"--output string", "--host string", "--dry-run"},
		},
		{
			name:    "cluster deploy keeps subtree root flags only",
			path:    []string{"cluster", "deploy"},
			visible: []string{"--dodo-data-dir string", "--parallel int"},
			hidden:  []string{"--output string", "--host string", "--catalog string"},
		},
		{
			name:    "gendata prompt keeps only common root flags",
			path:    []string{"gendata", "prompt"},
			visible: []string{"--config string", "--log-level string"},
			hidden:  []string{"--output string", "--catalog string", "--tables strings"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := renderCommandHelp(t, tc.path...)
			for _, visible := range tc.visible {
				assert.Contains(t, output, visible)
			}
			for _, hidden := range tc.hidden {
				assert.NotContains(t, output, hidden)
			}
		})
	}
}

func TestRootHelpStillShowsAllRootFlagsAfterChildHelp(t *testing.T) {
	_ = renderCommandHelp(t, "cluster", "deploy")

	rootHelp := renderCommandHelp(t)
	assert.Contains(t, rootHelp, "--output string")
	assert.Contains(t, rootHelp, "--host string")
	assert.Contains(t, rootHelp, "--http-port uint16")
	assert.Contains(t, rootHelp, "--tables strings")
}

func renderCommandHelp(t *testing.T, path ...string) string {
	t.Helper()

	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)

	cmd := rootCmd
	if len(path) > 0 {
		found, _, err := rootCmd.Find(path)
		require.NoError(t, err)
		cmd = found
	}

	require.NoError(t, cmd.Help())

	return strings.ReplaceAll(buf.String(), "\r\n", "\n")
}
