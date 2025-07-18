package generator

import (
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &IncGen{}

type IncGen struct {
	Start int64 `yaml:"start,omitempty"`
	Step  int64 `yaml:"step,omitempty"`
}

func (g *IncGen) Gen() any {
	result := g.Start
	g.Start = g.Start + g.Step
	return result
}

func NewIncGenerator(_ *TypeVisitor, _ parser.IDataTypeContext, r GenRule) (Gen, error) {
	start := cast.ToInt64(r["start"])
	step := cast.ToInt64(r["inc"])
	if start == 0 {
		start = 1
	}
	if step == 0 {
		step = 1
	}

	return &IncGen{
		Start: start,
		Step:  step,
	}, nil
}
