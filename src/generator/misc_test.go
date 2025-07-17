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
		"extra":  GenRule{"k1": GenRule{"k1_1": 1}, "k2": []any{1, 2, 3}},
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
		"extra":  GenRule{"k1": GenRule{"k1_1": 1}, "k2": []any{1, 2, 3}},
	}, dst)
	// changes of dst won't affect src
	dst["extra"].(GenRule)["k1"].(GenRule)["k1_1"] = 2
	dst["extra"].(GenRule)["k2"].([]any)[0] = 2
	assert.Equal(t, GenRule{"k1": GenRule{"k1_1": 1}, "k2": []any{1, 2, 3}}, src["extra"])

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
