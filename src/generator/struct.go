package generator

import (
	"bytes"
	"encoding/json"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &StructGen{}

type StructGen struct {
	Fields []*StructFieldGen

	insertOrCSV bool
}

func NewStructGen() *StructGen {
	return &StructGen{insertOrCSV: GenInsertOrCSV}
}

func (g *StructGen) AddChild(name string, type_ parser.IDataTypeContext, child Gen) {
	var writeVal WriteColVal
	if g.insertOrCSV {
		writeVal = GetInsertColValWriter(type_)
	} else {
		writeVal = QuoteValFunc(type_)
	}
	g.Fields = append(g.Fields, &StructFieldGen{Name: name, Value: child, writeVal: writeVal})
}

//nolint:revive
func (g *StructGen) Gen(c *GenContext) any {
	b := &bytes.Buffer{}

	var kvSep byte
	if g.insertOrCSV {
		b.WriteString("named_struct(")
		kvSep = ','
	} else {
		b.WriteByte('{')
		kvSep = ':'
	}
	for i, fieldgen := range g.Fields {
		b.WriteByte('"')
		b.WriteString(fieldgen.Name)
		b.WriteByte('"')
		b.WriteByte(kvSep)
		fieldgen.writeVal(b, fieldgen.Gen(c))
		if i != len(g.Fields)-1 {
			b.WriteByte(',')
		}
	}
	if g.insertOrCSV {
		b.WriteByte(')')
	} else {
		b.WriteByte('}')
	}

	return json.RawMessage(b.Bytes())
}

type StructFieldGen struct {
	Name     string
	Value    Gen
	writeVal WriteColVal
}

func (g *StructFieldGen) Gen(c *GenContext) any {
	return g.Value.Gen(c)
}
