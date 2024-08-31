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

func (g *FormatGen) Gen(c *GenContext) any {
	var (
		result      any
		resultSlice []any
	)

	if g.inner != nil {
		result = g.inner.Gen(c)
		if result == nil {
			return nil
		}
		if j, ok := result.(json.RawMessage); ok {
			result = string(j)
		} else if s, ok := result.([]any); ok {
			resultSlice = s
		}
	}

	var valueIdx int
	formatted, err := g.template.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		if !strings.HasPrefix(tag, "%") {
			return 0, fmt.Errorf("unknown format tag '%s'", tag)
		}

		// inject underlying generator result
		res := result
		if resultSlice != nil {
			if valueIdx >= len(resultSlice) {
				panic(fmt.Errorf("format parts out of range: %d, format: %s", valueIdx, g.Format))
			}
			res = resultSlice[valueIdx]
			valueIdx++
		}
		return w.Write(fmt.Appendf(nil, tag, res))
	})
	if err != nil {
		logrus.Fatalf("format execute template failed, err: %v", err)
	}

	return formatted
}

func NewFormatGenerator(format string, inner Gen) (Gen, error) {
	t, err := fasttemplate.NewTemplate(format, "{{", "}}")
	if err != nil {
		return nil, err
	}

	// ignore inner generator when no {{%xxx}} found
	if !strings.Contains(format, "{{%") {
		inner = nil
	}

	return &FormatGen{
		Format:   format,
		inner:    inner,
		template: t,
	}, nil
}
