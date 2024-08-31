package generator

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGolangGenerator(t *testing.T) {
	tests := []struct {
		name string
		rule GenRule
		want any
	}{
		{
			name: "simple",
			rule: GenRule{
				"golang": `
import "fmt"

var i int

func gen() any {
	result := i
	i++	
	return fmt.Sprintf("%d", result)
}`},
			want: "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := NewGolangGenerator(nil, nil, tt.rule)
			assert.NoError(t, err)
			for i := range 10 {
				if got := g.Gen(nil); strconv.Itoa(i) != got {
					t.Errorf("GolangGen.Gen() = %v, want %v", got, tt.want)
				}

			}
		})
	}
}
