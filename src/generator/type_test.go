package generator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeGenerator(t *testing.T) {
	type args struct {
		colpath string
		r       GenRule
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "complex_json",
			args: args{
				colpath: "table1.col1",
				r: GenRule{
					"type":      "json",
					"structure": "struct<foo:int, bar:int>",
					"fields": []any{
						GenRule{
							"name": "foo",
							"min":  1,
							"max":  1,
						},
						GenRule{
							"name": "bar",
							"gen": GenRule{
								"enum": []any{1},
							},
						},
					},
				},
			},
			want:    `{"foo":1,"bar":1}`,
			wantErr: false,
		},
		{
			name: "complex_struct",
			args: args{
				colpath: "table1.col2",
				r: GenRule{
					"type": "struct<foo:int, bar:int>",
					"fields": []any{
						GenRule{
							"name": "foo",
							"min":  1,
							"max":  1,
						},
						GenRule{
							"name": "bar",
							"gen": GenRule{
								"enum": []any{1},
							},
						},
					},
				},
			},
			want:    `{"foo":1,"bar":1}`,
			wantErr: false,
		},
		{
			name: "nested",
			args: args{
				colpath: "table1.col_varchar",
				r: GenRule{
					"type": "string",
					"gen": GenRule{
						"enum": []any{json.RawMessage(`123`)},
					},
				},
			},
			want:    `123`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTypeGenerator(NewColumnVisitor("", nil, tt.args.colpath, nil), nil, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTypeGenerator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			result := string(got.Gen(nil).(json.RawMessage))
			assert.JSONEq(t, result, tt.want)
		})
	}
}
