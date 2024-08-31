package src_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src"
)

func TestNewDB(t *testing.T) {
	t.Skip()
	db, err := src.NewDB("172.20.48.242", 9030, "root", "", "", "")
	assert.NoError(t, err)
	defer db.Close()
}
