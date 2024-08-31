package cmd

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCreateTableDDLsContinueOnError(t *testing.T) {
	ddls := []string{
		"db1.t1.table.sql",
		"db1.t2.table.sql",
		"db1.t3.table.sql",
	}

	var called []string
	errs, err := runCreateTableDDLs(ddls, 1, true, func(ddl string) error {
		called = append(called, ddl)
		if ddl == "db1.t2.table.sql" {
			return errors.New("boom")
		}
		return nil
	})

	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Equal(t, ddls, called)
	assert.ErrorContains(t, errs[0], "db1.t2.table.sql")
	assert.ErrorContains(t, errs[0], "boom")

	summary := summarizeCreateErrors(errs)
	require.Error(t, summary)
	assert.ErrorContains(t, summary, "create completed with 1 error(s)")
}

func TestRunCreateTableDDLsFailFast(t *testing.T) {
	ddls := []string{
		"db1.t1.table.sql",
		"db1.t2.table.sql",
		"db1.t3.table.sql",
	}

	var called []string
	errs, err := runCreateTableDDLs(ddls, 1, false, func(ddl string) error {
		called = append(called, ddl)
		if ddl == "db1.t2.table.sql" {
			return errors.New("boom")
		}
		return nil
	})

	require.Nil(t, errs)
	require.Error(t, err)
	assert.Equal(t, ddls, called)
	assert.ErrorContains(t, err, "boom")
}

func TestRunCreateOtherDDLsContinueOnError(t *testing.T) {
	ddls := []string{
		"db1.v1.view.sql",
		"db1.v2.view.sql",
	}

	attempts := map[string]int{}
	errs, err := runCreateOtherDDLs(ddls, true, func(ddl string) (string, error) {
		attempts[ddl]++
		switch ddl {
		case "db1.v1.view.sql":
			if attempts[ddl] == 1 {
				return "db1.v2", nil
			}
			return "", nil
		case "db1.v2.view.sql":
			return "", errors.New("boom")
		default:
			return "", nil
		}
	})

	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Equal(t, 2, attempts["db1.v1.view.sql"])
	assert.Equal(t, 1, attempts["db1.v2.view.sql"])
	assert.ErrorContains(t, errs[0], "db1.v2.view.sql")
	assert.ErrorContains(t, errs[0], "boom")
}

func TestRunCreateOtherDDLsContinueOnUnresolvedDependency(t *testing.T) {
	errs, err := runCreateOtherDDLs([]string{"db1.v1.view.sql"}, true, func(string) (string, error) {
		return "db1.t1", nil
	})

	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "db1.v1.view.sql needs db1.t1")
}
