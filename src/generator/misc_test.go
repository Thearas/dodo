package generator

import (
	"testing"

	"github.com/goccy/go-json"
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

func TestMustJSONMarshal(t *testing.T) {
	b := []byte{123, 49, 50, 53, 50, 49, 54, 50, 52, 58, 50, 48, 53, 57, 50, 52, 50, 48, 52, 56, 44, 50, 49, 54, 50, 56, 56, 50, 57, 54, 58, 49, 49, 56, 56, 56, 55, 54, 51, 50, 125}
	assert.Equal(t, "{12521624:2059242048,216288296:118887632}", string(MustJSONMarshal(json.RawMessage(b))))
}
