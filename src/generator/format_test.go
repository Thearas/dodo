package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatGen_Gen(t *testing.T) {
	innerGen, err := NewEnumGenerator(nil, nil, GenRule{"enum": []any{1, 2, 3, 4, 5}})
	assert.NoError(t, err)

	type fields struct {
		Format string
		inner  Gen
	}
	tests := []struct {
		name   string
		fields fields
		check  func(string) bool
	}{
		{
			name: "simple",
			fields: fields{
				Format: "type is {{%T}}",
				inner:  innerGen,
			},
			check: func(s string) bool { return s == "type is int" },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := NewFormatGenerator(
				tt.fields.Format,
				tt.fields.inner,
			)
			assert.NoError(t, err)
			if got := g.Gen(nil); !tt.check(got.(string)) {
				t.Errorf("FormatGen.Gen() = %v", got)
			}
		})
	}
	// assert.Fail(t, "")
}
