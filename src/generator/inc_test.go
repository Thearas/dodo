package generator

import (
	"reflect"
	"testing"

	"github.com/Thearas/dodo/src/parser"
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
		{
			name: "with end",
			args: args{
				r:      GenRule{"inc": 1000, "start": 1000, "end": 2500},
				repeat: 6,
			},
			want:    []int64{1000, 2000, 1000, 2000, 1000, 2000},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIncGenerator(NewColumnVisitor("", nil, tt.name, nil), nil, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIncGenerator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := range tt.args.repeat {
				got := got.Gen(nil)
				if !reflect.DeepEqual(got, tt.want[i]) {
					t.Errorf("IncGenerator() = %v, want %v", got, tt.want[i])
				}
			}
		})
	}
}

func mustParseDataType(ty string) parser.IDataTypeContext {
	p := parser.NewParser("test.col", ty)
	return p.DataType()
}

func TestIncFloatGenerator(t *testing.T) {
	tests := []struct {
		name    string
		r       GenRule
		repeat  int
		want    []float64
		wantErr bool
	}{
		{
			name:   "default float",
			r:      GenRule{"inc": nil},
			repeat: 3,
			want:   []float64{1, 2, 3},
		},
		{
			name:   "float with start and step",
			r:      GenRule{"inc": 0.5, "start": 1.0},
			repeat: 4,
			want:   []float64{1.0, 1.5, 2.0, 2.5},
		},
		{
			name:   "float with end wraps",
			r:      GenRule{"inc": GenRule{"start": 1.0, "step": 0.5, "end": 2.0}},
			repeat: 4,
			want:   []float64{1.0, 1.5, 2.0, 1.0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := mustParseDataType("DOUBLE")
			got, err := NewIncGenerator(NewColumnVisitor("", nil, tt.name, nil), dt, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIncGenerator(float) error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := range tt.repeat {
				val := got.Gen(nil).(float64)
				if val != tt.want[i] {
					t.Errorf("IncFloatGenerator() = %v, want %v", val, tt.want[i])
				}
			}
		})
	}
}

func TestIncTimeGenerator(t *testing.T) {
	tests := []struct {
		name    string
		dt      string
		r       GenRule
		repeat  int
		want    []string
		wantErr bool
	}{
		{
			name:   "datetime default step 1s",
			dt:     "DATETIME",
			r:      GenRule{"inc": GenRule{"start": "2025-01-01 00:00:00"}},
			repeat: 3,
			want:   []string{"2025-01-01 00:00:00", "2025-01-01 00:00:01", "2025-01-01 00:00:02"},
		},
		{
			name:   "datetime step 1h",
			dt:     "DATETIME",
			r:      GenRule{"inc": GenRule{"start": "2025-01-01 00:00:00", "step": "1h"}},
			repeat: 3,
			want:   []string{"2025-01-01 00:00:00", "2025-01-01 01:00:00", "2025-01-01 02:00:00"},
		},
		{
			name:   "date default step 24h",
			dt:     "DATE",
			r:      GenRule{"inc": GenRule{"start": "2025-01-01"}},
			repeat: 3,
			want:   []string{"2025-01-01", "2025-01-02", "2025-01-03"},
		},
		{
			name:   "datetime with end wraps",
			dt:     "DATETIME",
			r:      GenRule{"inc": GenRule{"start": "2025-01-01 00:00:00", "step": "1s", "end": "2025-01-01 00:00:02"}},
			repeat: 4,
			want:   []string{"2025-01-01 00:00:00", "2025-01-01 00:00:01", "2025-01-01 00:00:02", "2025-01-01 00:00:00"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := mustParseDataType(tt.dt)
			got, err := NewIncGenerator(NewColumnVisitor("", nil, tt.name, nil), dt, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIncGenerator(time) error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := range tt.repeat {
				val := got.Gen(nil).(string)
				if val != tt.want[i] {
					t.Errorf("IncTimeGenerator() = %v, want %v", val, tt.want[i])
				}
			}
		})
	}
}
