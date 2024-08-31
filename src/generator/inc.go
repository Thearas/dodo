package generator

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var _ Gen = &IncGen{}

type IncGen struct {
	Start int64  `yaml:"start,omitempty"`
	End   *int64 `yaml:"end,omitempty"`
	Step  int64  `yaml:"step,omitempty"`

	counter    atomic.Int64
	rangeSteps int64 // number of steps before wrapping, 0 = no end
}

func (g *IncGen) Gen(_ *GenContext) any {
	n := g.counter.Add(1)
	if g.rangeSteps > 0 {
		n = ((n - 1) % g.rangeSteps) + 1
	}
	return g.Start + (n-1)*g.Step
}

// IncFloatGen generates incrementing float64 values.
var _ Gen = &IncFloatGen{}

type IncFloatGen struct {
	Start float64
	End   *float64
	Step  float64

	counter    atomic.Int64
	rangeSteps int64 // number of steps before wrapping, 0 = no end
}

func (g *IncFloatGen) Gen(_ *GenContext) any {
	n := g.counter.Add(1)
	if g.rangeSteps > 0 {
		n = ((n - 1) % g.rangeSteps) + 1
	}
	return g.Start + float64(n-1)*g.Step
}

// IncTimeGen generates incrementing time.Time values.
var _ Gen = &IncTimeGen{}

type IncTimeGen struct {
	Start  time.Time
	End    *time.Time
	Step   time.Duration
	IsDate bool // true for DATE type (format as 2006-01-02), false for DATETIME (format as 2006-01-02 15:04:05)

	counter    atomic.Int64
	rangeSteps int64  // number of steps before wrapping, 0 = no end
	startNano  int64  // cached Start.UnixNano()
	format     string // cached format string
}

func (g *IncTimeGen) Gen(_ *GenContext) any {
	n := g.counter.Add(1)
	if g.rangeSteps > 0 {
		n = ((n - 1) % g.rangeSteps) + 1
	}
	nanos := g.startNano + int64(g.Step)*int64(n-1)
	return time.Unix(0, nanos).UTC().Format(g.format)
}

// Support both syntax:
// 1. {inc: {step: 1, start: 100}}
// 2. {inc: 1, start: 100}
func NewIncGenerator(v *ColumnVisitor, dataType parser.IDataTypeContext, r GenRule) (Gen, error) {
	baseType := ""
	if dataType != nil {
		baseType = GetBaseType(dataType, v.Colpath)
	}

	switch {
	case isIncFloatType(baseType):
		return newIncFloatGenerator(r)
	case isIncTimeType(baseType):
		return newIncTimeGenerator(r, baseType)
	default:
		return newIncIntGenerator(r)
	}
}

func newIncIntGenerator(r GenRule) (Gen, error) {
	var (
		start, step int64
		end         *int64
		end_        any
	)

	tryOrOne := func(v any) int64 {
		if v == nil {
			return 1
		}
		res, _ := lo.TryOr(func() (int64, error) { return cast.ToInt64E(v) }, 1)
		return res
	}

	var hasEnd bool
	if inc, ok := r["inc"].(GenRule); ok {
		// Syntax 1
		start = tryOrOne(inc["start"])
		end_, hasEnd = inc["end"]
		step = tryOrOne(inc["step"])
	} else {
		// Syntax 2
		start = tryOrOne(r["start"])
		end_, hasEnd = r["end"]
		step = tryOrOne(r["inc"])
	}
	if hasEnd {
		end__, err := cast.ToInt64E(end_)
		if err != nil {
			return nil, errors.New("inc end should be an integer")
		}
		if end__ <= start {
			return nil, errors.New("inc end should be greater than start")
		}
		end = &end__
	}

	g := &IncGen{
		Start: start,
		End:   end,
		Step:  step,
	}
	if end != nil {
		g.rangeSteps = (*end-start)/step + 1
	}
	return g, nil
}

func newIncFloatGenerator(r GenRule) (Gen, error) {
	var (
		start, step float64
		end         *float64
		end_        any
	)

	tryOrOne := func(v any) float64 {
		if v == nil {
			return 1
		}
		res, _ := lo.TryOr(func() (float64, error) { return cast.ToFloat64E(v) }, 1)
		return res
	}

	var hasEnd bool
	if inc, ok := r["inc"].(GenRule); ok {
		start = tryOrOne(inc["start"])
		end_, hasEnd = inc["end"]
		step = tryOrOne(inc["step"])
	} else {
		start = tryOrOne(r["start"])
		end_, hasEnd = r["end"]
		step = tryOrOne(r["inc"])
	}
	if hasEnd {
		end__, err := cast.ToFloat64E(end_)
		if err != nil {
			return nil, errors.New("inc end should be a number")
		}
		if end__ <= start {
			return nil, errors.New("inc end should be greater than start")
		}
		end = &end__
	}

	g := &IncFloatGen{
		Start: start,
		End:   end,
		Step:  step,
	}
	if end != nil {
		g.rangeSteps = int64((*end-start)/step) + 1
	}
	return g, nil
}

func newIncTimeGenerator(r GenRule, baseType string) (Gen, error) {
	var (
		start time.Time
		end   *time.Time
		step  time.Duration
		end_  any
	)

	parseTime := func(v any) (time.Time, error) {
		if v == nil {
			return time.Time{}, errors.New("time value is nil")
		}
		return cast.ToTimeE(v)
	}

	parseDuration := func(v any) time.Duration {
		if v == nil {
			return time.Second
		}
		// try parsing as duration string first (e.g. "1s", "1h", "24h")
		if s, ok := v.(string); ok {
			if d, err := time.ParseDuration(s); err == nil {
				return d
			}
		}
		// fallback: treat as seconds
		secs, _ := lo.TryOr(func() (int64, error) { return cast.ToInt64E(v) }, 1)
		return time.Duration(secs) * time.Second
	}

	var hasEnd bool
	if inc, ok := r["inc"].(GenRule); ok {
		var err error
		start, err = parseTime(inc["start"])
		if err != nil {
			return nil, fmt.Errorf("inc start should be a valid time: %w", err)
		}
		end_, hasEnd = inc["end"]
		step = parseDuration(inc["step"])
	} else {
		var err error
		if r["start"] != nil {
			start, err = parseTime(r["start"])
			if err != nil {
				return nil, fmt.Errorf("inc start should be a valid time: %w", err)
			}
		}
		end_, hasEnd = r["end"]
		step = parseDuration(r["inc"])
	}
	if hasEnd {
		endTime, err := parseTime(end_)
		if err != nil {
			return nil, fmt.Errorf("inc end should be a valid time: %w", err)
		}
		if !endTime.After(start) {
			return nil, errors.New("inc end should be after start")
		}
		end = &endTime
	}

	isDate := baseType == "DATE" || baseType == "DATEV1" || baseType == "DATEV2"
	// default step for DATE type: 1 day
	if isDate && step == time.Second {
		if inc, ok := r["inc"].(GenRule); ok {
			if inc["step"] == nil {
				step = 24 * time.Hour
			}
		} else if r["inc"] == nil {
			step = 24 * time.Hour
		}
	}

	format := "2006-01-02 15:04:05"
	if isDate {
		format = "2006-01-02"
	}
	g := &IncTimeGen{
		Start:     start,
		End:       end,
		Step:      step,
		IsDate:    isDate,
		format:    format,
		startNano: start.UnixNano(),
	}
	if end != nil {
		g.rangeSteps = int64(end.Sub(start)/step) + 1
	}
	return g, nil
}

func isIncFloatType(baseType string) bool {
	switch baseType {
	case "FLOAT", "DOUBLE", "DECIMAL", "DECIMALV2", "DECIMALV3":
		return true
	}
	return false
}

func isIncTimeType(baseType string) bool {
	switch baseType {
	case "DATE", "DATEV1", "DATEV2", "DATETIME", "DATETIMEV1", "DATETIMEV2", "TIMESTAMP":
		return true
	}
	return false
}
