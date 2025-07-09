package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeGenRule(t *testing.T) {
	// without overrides
	src := GenRule{
		"min":    5,
		"max":    6,
		"length": GenRule{"min": 1, "max": 5},
		"gen":    GenRule{"type": "string", "length": GenRule{"min": 5, "max": 10}},
	}
	dst := GenRule{
		"max":    10,
		"length": GenRule{"max": 10},
		"gen":    GenRule{"type": "varchar(10)", "min": 5, "max": 10},
	}
	MergeGenRules(dst, src, false)
	assert.Equal(t, GenRule{
		"min":    5,
		"max":    10,
		"length": GenRule{"max": 10},
		"gen":    GenRule{"type": "varchar(10)", "min": 5, "max": 10},
	}, dst)

	// with overrides
	src = GenRule{
		"min":    5,
		"max":    6,
		"length": GenRule{"min": 1, "max": 5},
		"gen":    GenRule{"type": "string", "length": GenRule{"min": 5, "max": 10}},
	}
	dst = GenRule{
		"max":    10,
		"length": GenRule{"max": 10},
		"gen":    GenRule{"type": "varchar(10)", "min": 5, "max": 10},
	}
	MergeGenRules(dst, src, true)
	assert.Equal(t, GenRule{
		"min":    5,
		"max":    6,
		"length": GenRule{"min": 1, "max": 5},
		"gen":    GenRule{"type": "string", "length": GenRule{"min": 5, "max": 10}},
	}, dst)
}

func TestRandomStr(t *testing.T) {
	for range 1000 {
		RandomStr(0, 10)
	}
}
