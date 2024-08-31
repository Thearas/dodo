package src

import (
	"testing"
)

func Test_minifyHash(t *testing.T) {
	type args struct {
		dict map[string]string
		s    string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "new",
			args: args{
				dict: map[string]string{},
				s:    "table1",
			},
			want: "a",
		},
		{
			name: "simple1",
			args: args{
				dict: map[string]string{"@@last": "zza"},
				s:    "table1",
			},
			want: "aab",
		},
		{
			name: "simple2",
			args: args{
				dict: map[string]string{"@@last": "z"},
				s:    "table1",
			},
			want: "aa",
		},
		{
			name: "simple3",
			args: args{
				dict: map[string]string{"@@last": "zbc"},
				s:    "table1",
			},
			want: "acc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := minifyHash(tt.args.dict, tt.args.s); got != tt.want {
				t.Errorf("minifyHash() = %v, want %v", got, tt.want)
			}
		})
	}
}
