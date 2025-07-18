package generator

import (
	"reflect"
	"testing"
)

func TestIncGenerator(t *testing.T) {
	type args struct {
		r      GenRule
		repeat int
	}
	tests := []struct {
		name    string
		args    args
		want    []int64
		wantErr bool
	}{
		{
			name: "default",
			args: args{
				r:      GenRule{"inc": nil},
				repeat: 3,
			},
			want:    []int64{1, 2, 3},
			wantErr: false,
		},
		{
			name: "simple",
			args: args{
				r:      GenRule{"inc": 1000, "start": 1000},
				repeat: 3,
			},
			want:    []int64{1000, 2000, 3000},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIncGenerator(NewTypeVisitor(tt.name, nil), nil, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIncGenerator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := range tt.args.repeat {
				got := got.Gen()
				if !reflect.DeepEqual(got, tt.want[i]) {
					t.Errorf("IncGenerator() = %v, want %v", got, tt.want[i])
				}
			}
		})
	}
}
