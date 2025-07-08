package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/parser"
)

func TestPartsGen_Gen(t *testing.T) {
	v := NewTypeVisitor("t1.c1", GenRule{
		"format": "{{%d}}-{{%02d}}-{{%d}} 01:00:01",
		"gen": GenRule{
			"parts": []any{
				1997,
				GenRule{"gen": GenRule{"type": "int", "min": 2, "max": 2}},
				GenRule{"gen": GenRule{"enum": []any{16, 20, 24}, "weights": []any{1, 0, 0}}},
			},
		},
	})
	p := parser.NewParser(v.Colpath, "DATETIME")
	g := v.GetGen(p.DataType())

	assert.Equal(t, "1997-02-16 01:00:01", g.Gen())
}
