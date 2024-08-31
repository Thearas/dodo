package generator

import (
	"bytes"
	"fmt"
	"math/rand/v2"

	"github.com/goccy/go-json"

	"github.com/Thearas/dodo/src/parser"
)

type MapGen struct {
	Key, Value     Gen
	LenMin, LenMax int
	KeyUniq        bool

	insertOrCSV        bool
	writeKey, writeVal WriteColVal
}

func NewMapGen(kType, vType parser.IDataTypeContext, kgen, vgen Gen, lenMin, lenMax int) *MapGen {
	var writeKey, writeVal WriteColVal
	if GenInsertOrCSV {
		writeKey = GetInsertColValWriter(kType)
		writeVal = GetInsertColValWriter(vType)
	} else {
		writeKey = QuoteValFunc(kType)
		writeVal = QuoteValFunc(vType)
	}
	return &MapGen{
		Key:     kgen,
		Value:   vgen,
		LenMin:  lenMin,
		LenMax:  lenMax,
		KeyUniq: true,

		insertOrCSV: GenInsertOrCSV,
		writeKey:    writeKey,
		writeVal:    writeVal,
	}
}

func (g *MapGen) SetKeyUniq(uniq bool) {
	g.KeyUniq = uniq
}

//nolint:revive
func (g *MapGen) Gen(c *GenContext) any {
	var (
		b           = &bytes.Buffer{}
		length      = rand.IntN(g.LenMax-g.LenMin+1) + g.LenMin
		insertOrCSV = g.insertOrCSV
		keySet      = map[string]struct{}{}
	)

	if insertOrCSV {
		b.WriteString("map(")
	} else {
		b.WriteByte('{')
	}

	for i := range length {
		kval := g.Key.Gen(c)
		// make map key uniqued
		if g.KeyUniq {
			kstr := fmt.Sprint(kval)
			if _, ok := keySet[kstr]; ok {
				continue
			}
			keySet[kstr] = struct{}{}
		}

		val := g.Value.Gen(c)

		if i > 0 {
			b.WriteByte(',')
		}

		g.writeKey(b, kval)
		if insertOrCSV {
			b.WriteByte(',')
		} else {
			b.WriteByte(':')
		}
		g.writeVal(b, val)
	}

	if insertOrCSV {
		b.WriteString(")")
	} else {
		b.WriteByte('}')
	}

	return json.RawMessage(b.Bytes())
}
