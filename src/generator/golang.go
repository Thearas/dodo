package generator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

var _ Gen = &GolangGen{}

type GolangGen struct {
	Code string

	genF func() any
}

func (g *GolangGen) Gen() any {
	return g.genF()
}

func NewGolangGenerator(_ string, r GenRule) (Gen, error) {
	// The code snippet must have a function `func gen() any {...}`
	codeSnippet, ok := r["golang"].(string)
	if !ok {
		return nil, errors.New("golang code is not a string")
	}

	// complete golang code with snippet
	code := fmt.Sprintf(`package gen

%s
`, codeSnippet)

	// compile ahead of time
	i := interp.New(interp.Options{})
	// check if possible to use golang stdlib
	if strings.Contains(code, "import") {
		i.Use(stdlib.Symbols)
	}
	_, err := i.Eval(code)
	if err != nil {
		return nil, fmt.Errorf("golang eval code failed, err: %v, code:\n%s", err, code)
	}
	v, err := i.Eval("gen.gen")
	if err != nil {
		return nil, fmt.Errorf("golang eval function gen() failed, err: %v, code:\n%s", err, code)
	}

	genF, ok := v.Interface().(func() any)
	if !ok {
		return nil, fmt.Errorf("golang expect a function with signature: 'func gen() any'")
	}

	return &GolangGen{
		Code: code,
		genF: genF,
	}, nil
}
