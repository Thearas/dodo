package generator

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &TypeGen{}

type TypeGen struct {
	GenRule GenRule

	gen Gen
}

func (g *TypeGen) Gen(c *GenContext) any {
	return g.gen.Gen(c)
}

func NewTypeGenerator(v *ColumnVisitor, _ parser.IDataTypeContext, r GenRule) (Gen, error) {
	p := parser.NewParser(v.Colpath, cast.ToString(r["type"]))
	dataType := p.DataType()
	if p.ErrListener.LastErr != nil {
		return nil, fmt.Errorf("parse type generator failed for column '%s', err: %v", v.Colpath, p.ErrListener.LastErr)
	}

	return &TypeGen{
		GenRule: r,
		gen:     v.GetChildGen(v.Colpath, dataType, r),
	}, nil
}
