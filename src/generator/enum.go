package generator

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &EnumGen{}

type EnumGen struct {
	Enum              []any     `yaml:"enum,omitempty"`
	Weights           []float32 `yaml:"weights,omitempty"`
	cumulativeWeights []float32
}

func (g *EnumGen) Gen() any {
	v := g.gen()
	if gr, ok := v.(Gen); ok {
		return gr.Gen()
	}
	return v
}

//nolint:revive
func (g *EnumGen) gen() any {
	if len(g.Weights) == 0 {
		return g.Enum[gofakeit.IntN(len(g.Enum))]
	}

	weight := rand.Float32()
	// Use binary search on cumulative weights for efficient selection.
	i := sort.Search(len(g.cumulativeWeights), func(i int) bool {
		return g.cumulativeWeights[i] > weight
	})
	return g.Enum[i]
}

func NewEnumGenerator(visitor *TypeVisitor, dataType parser.IDataTypeContext, r GenRule) (Gen, error) {
	enum_ := r["enum"]
	if enum_ == nil {
		enum_ = cast.ToStringSlice(r["enums"])
	}
	enum, ok := enum_.([]any)
	if !ok || len(enum) == 0 {
		return nil, errors.New("enum is empty")
	}
	for i, v := range enum {
		gr, ok := v.(GenRule)
		if !ok {
			continue
		}
		enum[i] = visitor.GetChildGen(fmt.Sprintf("enum.%d", i), dataType, gr)
	}

	weights_ := r["weights"]
	if weights_ == nil {
		weights_ = r["weight"]
	}
	if weights_ == nil {
		return &EnumGen{Enum: enum}, nil
	}
	ws, ok := weights_.([]any)
	if !ok || len(ws) == 0 {
		return nil, fmt.Errorf("weights should be a [float], but got: %T", r["weights"])
	}
	weights := lo.Map(ws, func(w any, _ int) float32 { return cast.ToFloat32(w) })
	if len(weights) != len(enum) {
		return nil, errors.New("enum length not equals to weights length")
	}
	if lo.Sum(weights) != 1 {
		return nil, errors.New("sum of weights should be 1")
	}

	cumulativeWeights := make([]float32, len(weights))
	var sum float32
	for i, w := range weights {
		sum += w
		cumulativeWeights[i] = sum
	}
	if len(cumulativeWeights) > 0 {
		cumulativeWeights[len(cumulativeWeights)-1] = 1
	}

	return &EnumGen{
		Enum:              enum,
		Weights:           weights,
		cumulativeWeights: cumulativeWeights,
	}, nil
}
