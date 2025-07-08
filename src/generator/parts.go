package generator

import (
	"errors"
	"fmt"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &PartsGen{}

type PartsGen struct {
	Parts []any `yaml:"parts,omitempty"`
}

func (g *PartsGen) Gen() any {
	vs := make([]any, len(g.Parts))
	for i, part := range g.Parts {
		if gr, ok := part.(Gen); ok {
			vs[i] = gr.Gen()
			continue
		}
		vs[i] = part
	}
	return vs
}

func NewPartsGenerator(visitor *TypeVisitor, dataType parser.IDataTypeContext, r GenRule) (Gen, error) {
	parts_ := r["parts"]

	parts, ok := parts_.([]any)
	if !ok || len(parts) == 0 {
		return nil, errors.New("parts is empty")
	}

	for i, v := range parts {
		gr, ok := v.(GenRule)
		if !ok {
			continue
		}
		parts[i] = visitor.GetChildGen(fmt.Sprintf("parts.%d", i), dataType, gr)
	}

	return &PartsGen{
		Parts: parts,
	}, nil
}
