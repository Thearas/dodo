package generator

import (
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasttemplate"
)

var _ Gen = &FormatGen{}

type FormatGen struct {
	Format string
	inner  Gen

	template *fasttemplate.Template
}

func (g *FormatGen) Gen() any {
	var (
		result      any
		resultSlice []any
	)
	if g.inner != nil {
		result = g.inner.Gen()
	}
	if result == nil {
		return nil
	}
	if j, ok := result.(json.RawMessage); ok {
		result = string(j)
	} else if s, ok := result.([]any); ok {
		resultSlice = s
	}

	var valueIdx int
	formatted, err := g.template.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		// 1. inject underlying generator result
		if strings.HasPrefix(tag, "%") {
			res := result
			if resultSlice != nil {
				if valueIdx >= len(resultSlice) {
					panic(fmt.Errorf("format parts out of range: %d, format: %s", valueIdx, g.Format))
				}
				res = resultSlice[valueIdx]
				valueIdx++
			}
			return w.Write(fmt.Appendf(nil, tag, res))
		}

		// 2. inject built-in format tag
		tagF, ok := FormatTags[tag]
		if !ok {
			return 0, fmt.Errorf("unknown format tag '%s'", tag)
		}

		result := tagF.Call(nil)[0].Interface()
		if result == nil {
			return w.Write([]byte(`\N`))
		} else if s, ok := result.(string); ok {
			return w.Write([]byte(s))
		}
		return w.Write(fmt.Append(nil, result))
	})
	if err != nil {
		logrus.Errorf("format execute templace failed, err: %v\n", err)
	}

	return formatted
}

func NewFormatGenerator(format string, inner Gen) (Gen, error) {
	t, err := fasttemplate.NewTemplate(format, "{{", "}}")
	if err != nil {
		return nil, err
	}

	return &FormatGen{
		Format:   format,
		inner:    inner,
		template: t,
	}, nil
}
