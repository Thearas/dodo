package generator

import (
	"fmt"

	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

// RefSelfGen generates values by referencing another column in the same table.

type RefSelfGen struct {
	SourceColumn    string // source column
	SourceColumnIdx int    // source column idx in table
}

func (g *RefSelfGen) Gen(c *GenContext) any {
	return c.ColVals[g.SourceColumnIdx]
}

func NewRefSelfGenerator(v *ColumnVisitor, _ parser.IDataTypeContext, r GenRule) (Gen, error) {
	ref := cast.ToString(r["ref"])
	*v.SelfColRefs = lo.Uniq(append(*v.SelfColRefs, ref))
	srcIdx := lo.IndexOf(v.Columns, ref)
	if srcIdx == -1 {
		return nil, fmt.Errorf("not found the column ref to in same table: '%s'", ref)
	}
	return &RefSelfGen{
		SourceColumn:    ref,
		SourceColumnIdx: srcIdx,
	}, nil
}
