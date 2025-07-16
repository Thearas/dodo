package generator

import (
	"reflect"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/parser"
)

func TestEnumGen_Gen(t *testing.T) {
	type fields struct {
		Enum              []any
		Weights           []float32
		cumulativeWeights []float32
	}
	tests := []struct {
		name   string
		fields fields
		want   any
	}{
		{
			name: "simple",
			fields: fields{
				Enum:              []any{1, 2, 3.0},
				Weights:           []float32{0, 0, 1},
				cumulativeWeights: []float32{0, 0, 1},
			},
			want: 3.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &EnumGen{
				Enum:              tt.fields.Enum,
				Weights:           tt.fields.Weights,
				cumulativeWeights: tt.fields.cumulativeWeights,
			}
			if got := g.Gen(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EnumGen.Gen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnumGen_Gen_Distribution(t *testing.T) {
	enum := []any{"a", "b", "c", "d"}
	weights := []float32{0.1, 0.2, 0.3, 0.4}
	cumulativeWeights := make([]float32, len(weights))
	var sum float32
	for i, w := range weights {
		sum += w
		cumulativeWeights[i] = sum
	}
	g := &EnumGen{
		Enum:              enum,
		Weights:           weights,
		cumulativeWeights: cumulativeWeights,
	}

	const totalRuns = 100000
	counts := make(map[any]int)
	for range totalRuns {
		counts[g.Gen()]++
	}

	assert.Equal(t, len(enum), len(counts))

	for i, e := range enum {
		expected := weights[i]
		actual := float32(counts[e]) / totalRuns
		// Allow a small tolerance for randomness
		assert.InDelta(t, expected, actual, 0.01, "distribution for %v is not as expected", e)
	}
}

func TestNewEnumGenRule(t *testing.T) {
	type args struct {
		dataType string
		r        GenRule
	}
	tests := []struct {
		name    string
		args    args
		want    Gen
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				dataType: "int",
				r:        MustYAMLUmarshal("{enum: [1, 2, 3], weights: [0.4, 0.5, 0.1]}"),
			},
			want: &EnumGen{
				Enum:              []any{1, 2, 3},
				Weights:           []float32{0.4, 0.5, 0.1},
				cumulativeWeights: []float32{0.4, 0.9, 1.0},
			},
			wantErr: false,
		},
		{
			name: "complex",
			args: args{
				dataType: "varchar(100)",
				r: MustYAMLUmarshal(`
enum:
    - length: 5
    - length: {min: 5, max: 10}
    - format: "int to str: {{%d}}"
      gen:
          enum: [1, 2, 3]
    - format: "{{%d}}"
      gen:
          ref: t1.col1
weights: [0.4, 0.4, 0.1, 0.1]
 `),
			},
			wantErr: false,
		},
		{
			name: "err: less weights",
			args: args{
				dataType: "int",
				r:        MustYAMLUmarshal("{enum: [1, 2, 3], weights: [0.4, 0.5]}"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.name, tt.args.dataType)
			dataType := p.DataType()
			assert.NoError(t, p.ErrListener.LastErr)
			got, err := NewEnumGenerator(NewTypeVisitor(tt.name, nil), dataType, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEnumGenRule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.name == "simple" {
				assert.Equal(t, tt.want, got)
			} else if strings.HasPrefix(tt.name, "complex") {
				// inject values to ref t1.col1
				refgen := getColumnRefGen("t1", "col1")
				refgen.AddRefVals(lo.ToAnySlice(lo.Range(996))...)
				enum := got.(*EnumGen).Enum
				for _, v := range enum {
					for range 100 {
						assert.IsType(t, "", v.(Gen).Gen())
					}
				}
			}
		})
	}
}
