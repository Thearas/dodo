package generator

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &GolangGen{}

type GolangGen struct {
	Code     string
	Parallel bool

	genF func() any
	lock *sync.Mutex
}

func (g *GolangGen) Gen(_ *GenContext) any {
	if !g.Parallel {
		g.lock.Lock()
		defer g.lock.Unlock()
	}
	return g.genF()
}

func NewGolangGenerator(_ *ColumnVisitor, _ parser.IDataTypeContext, r GenRule) (Gen, error) {
	// The code snippet must have a function `func gen() any {...}`
	codeSnippet, ok := r["golang"].(string)
	if !ok {
		return nil, errors.New("golang code is not a string")
	}
	parallel, ok := r["parallel"].(bool)
	if !ok {
		parallel = true
	}

	// complete golang code with snippet
	code := fmt.Sprintf(`package gen

%s
`, codeSnippet)

	// compile ahead of time
	i := interp.New(interp.Options{})
	// check if possible to use golang stdlib
	if strings.Contains(code, "import") {
		if err := i.Use(stdlib.Symbols); err != nil {
			return nil, fmt.Errorf("golang import stdlib failed, err: %v", err)
		}
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
		return nil, errors.New("golang expect a function with signature: 'func gen() any'")
	}

	return &GolangGen{
		Code:     code,
		Parallel: parallel,
		genF:     genF,
		lock:     &sync.Mutex{},
	}, nil
}
