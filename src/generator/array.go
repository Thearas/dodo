package generator

import (
	"bytes"
	"math/rand/v2"

	"github.com/goccy/go-json"

	"github.com/Thearas/dodo/src/parser"
)

type ArrayGen struct {
	Element        Gen
	LenMin, LenMax int

	insertOrCSV bool
	writeVal    WriteColVal
}

func NewArrayGen(elementType parser.IDataTypeContext, elementGen Gen, lenMin, lenMax int) *ArrayGen {
	var writeVal WriteColVal
	if GenInsertOrCSV {
		writeVal = GetInsertColValWriter(elementType)
	} else {
		writeVal = QuoteValFunc(elementType)
	}
	return &ArrayGen{
		Element: elementGen,
		LenMin:  lenMin,
		LenMax:  lenMax,

		insertOrCSV: GenInsertOrCSV,
		writeVal:    writeVal,
	}
}

func (g *ArrayGen) Gen(c *GenContext) any {
	length := rand.IntN(g.LenMax-g.LenMin+1) + g.LenMin

	b := &bytes.Buffer{}
	if g.insertOrCSV {
		_, _ = b.WriteString("array(")
	} else {
		_ = b.WriteByte('[')
	}

	for i := range length {
		_, _ = g.writeVal(b, g.Element.Gen(c))
		if i != length-1 {
			_ = b.WriteByte(',')
		}
	}

	if g.insertOrCSV {
		_ = b.WriteByte(')')
	} else {
		_ = b.WriteByte(']')
	}

	return json.RawMessage(b.Bytes())
}
