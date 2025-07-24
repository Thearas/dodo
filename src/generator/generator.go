package generator

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

var CustomGenConstructors map[string]CustomGenConstructor

func init() {
	CustomGenConstructors = map[string]CustomGenConstructor{
		"inc":  NewIncGenerator,
		"enum": NewEnumGenerator, "enums": NewEnumGenerator,
		"parts":  NewPartsGenerator,
		"ref":    NewRefGenerator,
		"type":   NewTypeGenerator,
		"golang": NewGolangGenerator,
	}
}

func Setup(genconf string, confIdx int) error {
	SetupFormatTags()
	return SetupGenRules(genconf, confIdx)
}

type GenRule = map[string]any
type Gen interface {
	Gen() any
}

type CustomGenConstructor = func(v *TypeVisitor, dataType parser.IDataTypeContext, r GenRule) (Gen, error)

type TypeVisitor struct {
	Colpath string  // the path of the column, e.g. "db.table.col"
	GenRule GenRule // rules of generator

	// the tables that ref generator point to
	TableRefs *[]string
}

func NewTypeVisitor(colpath string, genRule GenRule) *TypeVisitor {
	if genRule == nil {
		genRule = GenRule{}
	}
	return &TypeVisitor{
		Colpath:   colpath,
		GenRule:   genRule,
		TableRefs: &[]string{},
	}
}

func (v *TypeVisitor) GetGen(type_ parser.IDataTypeContext) Gen {
	var (
		g        Gen
		err      error
		baseType = v.GetBaseType(type_)
	)

	// Merge global (aka. default) generation rules.
	v.MergeDefaultRule(baseType)
	if logrus.GetLevel() > logrus.DebugLevel {
		logrus.Tracef("gen rule of '%s': %s", v.Colpath, string(MustJSONMarshal(v.GenRule)))
	}

	if customGenRule, ok := v.GetRule("gen").(GenRule); ok {
		// 1. custom generator
		g = v.getCustomGen(type_, customGenRule)
	} else {
		// 2. type generator
		g = v.getTypeGen(type_, baseType)
	}

	// format generator
	if format, ok := v.GetRule("format").(string); ok && format != "" {
		g, err = NewFormatGenerator(format, g)
		if err != nil {
			logrus.Fatalf("The format rule '%s' of column '%s' compile failed, err: %v", format, v.Colpath, err)
		}
	} else if _, ok := g.(*PartsGen); ok {
		logrus.Fatalf("Parts generator cannot be used without format rule, please add 'format' rule for column '%s'", v.Colpath)
	}

	// null generator
	nullFrequency := v.GetNullFrequency()
	if nullFrequency > 0 && nullFrequency <= 1 && baseType != "BITMAP" {
		return NewFuncGen(func() any {
			if rand.Float32() < nullFrequency {
				return nil
			}
			return g.Gen()
		})
	}

	return g
}

func (v *TypeVisitor) getCustomGen(type_ parser.IDataTypeContext, customGenRule GenRule) Gen {
	var (
		g       Gen
		genName string
		err     error
	)
	for name, newCustomGen := range CustomGenConstructors {
		if _, ok := customGenRule[name]; !ok {
			continue
		}
		if g != nil {
			logrus.Fatalf("Multiple custom generators found for column '%s', only one is allowed, but got both: %s and %s", v.Colpath, genName, name)
		}

		g, err = newCustomGen(v, type_, customGenRule)
		if err != nil {
			logrus.Fatalf("Invalid custom generator '%s' for column '%s', err: %v", name, v.Colpath, err)
		}
		genName = name
	}
	if g == nil {
		logrus.Fatalf("Custom generator not found for column '%s', expect one of %v",
			v.Colpath,
			lo.MapToSlice(CustomGenConstructors, func(name string, _ CustomGenConstructor) string { return name }),
		)
	}
	return g
}

func (v *TypeVisitor) getTypeGen(type_ parser.IDataTypeContext, baseType string) Gen {
	var g Gen
	switch ty := type_.(type) {
	case *parser.ComplexDataTypeContext:
		switch baseType {
		case "ARRAY":
			// Handle array type
			g_ := &ArrayGen{}
			g_.LenMin, g_.LenMax = v.GetLength()
			g_.SetElementGen(v.GetChildGen("element", ty.DataType(0)))
			g = g_
		case "MAP":
			// Handle map type
			kv := ty.AllDataType()
			if len(kv) != 2 {
				logrus.Fatalf("Invalid map type: '%s' for column '%s', expected 2 types for key and value", ty.GetText(), v.Colpath)
			}

			// Handle key-value pair in map
			g_ := &MapGen{}
			g_.LenMin, g_.LenMax = v.GetLength()
			g_.SetKeyGen(v.GetChildGen("key", kv[0]))
			g_.SetValueGen(v.GetChildGen("value", kv[1]))
			g = g_
		case "STRUCT":
			// Handle struct type
			g_ := &StructGen{}

			// Handle each field in the struct
			fields_ := v.GetRule("fields")
			if fields_ == nil {
				fields_ = v.GetRule("field")
			}
			fieldRules, ok := fields_.([]any) // Ensure fields is a slice of maps
			if !ok {
				if fields_ != nil {
					logrus.Fatalf("Invalid struct fields type '%T' for column '%s'", fields_, v.Colpath)
				}
				fieldRules = lo.ToAnySlice([]GenRule{})
			}
			i := 0
			fields := lo.SliceToMap(fieldRules, func(field_ any) (string, GenRule) {
				field, ok := field_.(GenRule)
				if !ok {
					logrus.Fatalf("Invalid struct field #%d in column '%s'", i, v.Colpath)
				}
				fieldName, ok := field["name"].(string)
				if !ok {
					logrus.Fatalf("Struct field #%d has no name in column '%s'", i, v.Colpath)
				}
				i++
				return fieldName, field
			})
			for _, field := range ty.ComplexColTypeList().AllComplexColType() {
				fieldName := strings.Trim(field.Identifier().GetText(), "`")
				fieldType := field.DataType()
				g_.AddChild(fieldName, v.GetChildGen(fieldName, fieldType, fields[fieldName]))
			}
			g = g_
		default:
			logrus.Fatalf("Unsupported complex type: '%s' for column '%s'", ty.GetComplex_().GetText(), v.Colpath)
		}
	case *parser.PrimitiveDataTypeContext:
		min_, max_ := v.GetMinMax()
		switch baseType {
		case "BITMAP":
			// Generate a random bitmap array with a length between lenMin and lenMax
			lenMin, lenMax := v.GetLength()
			minVal, maxVal := CastMinMax[int64](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any {
				return json.RawMessage(MustJSONMarshal(lo.RepeatBy(gofakeit.IntRange(lenMin, lenMax), func(_ int) int64 {
					return rand.Int64N(maxVal-minVal+1) + minVal
				})))
			})
		case "JSON", "JSONB", "VARIANT":
			var genRule GenRule
			structure, ok := v.GetRule("structure").(string)
			structure = strings.TrimSpace(structure)
			if ok && structure != "" {
				genRule = maps.Clone(v.GenRule)
				delete(genRule, "structure")
			} else {
				logrus.Fatalf("JSON/JSONB/VARIANT must have gen rule 'structure' or 'gen' at column '%s'", v.Colpath)
			}

			p := parser.NewParser(v.Colpath, structure)
			dataType := p.DataType()
			if err := p.ErrListener.LastErr; err != nil {
				logrus.Fatalf("Invalid JSON structure '%s' for column '%s': %v", structure, v.Colpath, err)
			}
			visitor := NewTypeVisitor(v.Colpath, genRule)
			g = visitor.GetGen(dataType)
		case "BOOL", "BOOLEAN":
			enum := []int{0, 1}
			g = NewFuncGen(func() any { return gofakeit.RandomInt(enum) }) // BOOLEAN is typically 0 or 1
		case "TINYINT":
			minVal, maxVal := CastMinMax[int8](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "SMALLINT":
			minVal, maxVal := CastMinMax[int16](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "INT", "INTEGER":
			minVal, maxVal := CastMinMax[int32](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "BIGINT", "LARGEINT": // TODO: Need larger INT?
			minVal, maxVal := CastMinMax[int64](min_, max_, baseType, v.Colpath)
			range_ := maxVal - minVal + 1
			g = NewFuncGen(func() int64 { return rand.Int64N(range_) + minVal })
		case "FLOAT":
			minVal, maxVal := CastMinMax[float32](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any { return gofakeit.Float32Range(minVal, maxVal) })
		case "DOUBLE":
			minVal, maxVal := CastMinMax[float64](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any { return gofakeit.Float64Range(minVal, maxVal) })
		case "DECIMAL", "DECIMALV2", "DECIMALV3": // TODO: Need larger DECIMAL?
			var precision, scale int = 999, 999
			if v.GetRule("precision") != nil {
				precision = cast.ToInt(v.GetRule("precision"))
			}
			if v.GetRule("scale") != nil {
				scale = cast.ToInt(v.GetRule("scale"))
			}

			intVals := ty.AllINTEGER_VALUE()
			var p, s int
			if len(intVals) > 0 {
				p = cast.ToInt(intVals[0].GetText())
			} else {
				p = 8
			}
			if p > 38 {
				p = 38
			}
			if precision > p {
				precision = p
				// logrus.Debugf("Precision '%d' is larger than the defined precision '%d' for column '%s', using %d instead", precision, p, v.Colpath, p)
			}
			if len(intVals) > 1 {
				s = cast.ToInt(intVals[1].GetText())
			}
			if s < 0 || s > precision {
				// logrus.Debugf("Scale '%d' is invalid for precision '%d' in column '%s', using 0 instead", s, precision, v.Colpath)
				s = 0
			}
			if scale > s {
				// logrus.Debugf("Scale '%d' is larger than the defined scale '%d' for column '%s', using %d instead", scale, s, v.Colpath, s)
				scale = s
			}

			var minVal, maxVal int64
			if min_ == nil {
				minVal = -int64(math.Pow10(int(precision))) + 1 // Default min value
			} else {
				minVal = cast.ToInt64(min_)
			}
			if max_ == nil {
				maxVal = int64(math.Pow10(int(precision))) - 1 // Default max value
			} else {
				maxVal = cast.ToInt64(max_)
			}

			// TODO: Support larger precision
			intLen := min(precision-scale, MAX_DECIMAL_INT_LEN)

			g = NewFuncGen(func() any {
				var res [2]int64
				if intLen == 0 {
					res[0] = 0
				} else if minVal < 0 && rand.Float32() < 0.5 {
					delta := -float64(minVal)
					n := int64(min(delta, math.Pow10(intLen)-1))
					res[0] = -rand.Int64N(n)
				} else {
					delta := float64(maxVal) - max(0, float64(minVal)) + 1
					lowerBound := int64(max(0, float64(minVal)))
					n := int64(min(delta, math.Pow10(intLen)-1))
					res[0] = lowerBound + rand.Int64N(n)
				}

				n := int64(math.Pow10(scale))
				if n <= 0 {
					res[1] = 0
				} else {
					res[1] = rand.Int64N(n)
				}

				return json.RawMessage(fmt.Sprintf("%d.%0*d", res[0], scale, res[1])) // Format as decimal string
			})
		case "DATE", "DATEV1", "DATEV2":
			minVal, maxVal := CastMinMax[time.Time](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any { return gofakeit.DateRange(minVal, maxVal).Format("2006-01-02") })
		case "DATETIME", "DATETIMEV1", "DATETIMEV2", "TIMESTAMP":
			minVal, maxVal := CastMinMax[time.Time](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any { return gofakeit.DateRange(minVal, maxVal).Format("2006-01-02 15:04:05") })
		case "TEXT", "STRING":
			lenMin, lenMax := v.GetLength()
			lenMin = max(1, lenMin)
			lenMax = max(1, lenMax)
			g = NewFuncGen(func() any { return RandomStr(lenMin, lenMax) })
		case "VARCHAR":
			var (
				length         int
				lenMin, lenMax = v.GetLength()
			)
			lenMin = max(1, lenMin)
			lenMax = max(1, lenMax)
			length_ := ty.INTEGER_VALUE(0)
			if length_ != nil {
				length = max(1, cast.ToInt(length_.GetText()))
			} else {
				length = lenMax
			}
			if length < lenMax {
				lenMax = length
			}
			if lenMin > lenMax {
				lenMin = 1
			}
			g = NewFuncGen(func() any { return RandomStr(lenMin, lenMax) })
		case "CHAR":
			length_ := ty.INTEGER_VALUE(0)
			if length_ == nil {
				logrus.Fatalf("CHAR type must have a length in column '%s'", v.Colpath)
			}
			length := min(max(1, cast.ToInt(length_.GetText())), 255)
			g = NewFuncGen(func() any { return RandomStr(length, length) })
		case "IPV4":
			g = NewFuncGen(func() any { return gofakeit.IPv4Address() })
		case "IPV6":
			g = NewFuncGen(func() any { return gofakeit.IPv6Address() })
		case "HLL":
			// skip gen HLL
			g = NewFuncGen(func() any { return "" })
		default: // TODO: AGG_STATE, QUANTILE_STATE
			logrus.Fatalf("Unsupported column type '%s' for column '%s'", type_.GetText(), v.Colpath)
		}
	}
	return g
}

func (v *TypeVisitor) GetBaseType(type_ parser.IDataTypeContext) (t string) {
	switch ty := type_.(type) {
	case *parser.ComplexDataTypeContext:
		t = ty.GetComplex_().GetText()
	case *parser.PrimitiveDataTypeContext:
		t = ty.PrimitiveColType().GetType_().GetText()
	default:
		logrus.Fatalf("Unsupported column type '%s' for column '%s'", type_.GetText(), v.Colpath)
	}
	return strings.ToUpper(t)
}

func (v *TypeVisitor) MergeDefaultRule(baseType string) *TypeVisitor {
	defaultGenRule, ok := DefaultTypeGenRules[baseType].(GenRule)
	if !ok {
		if ty_, ok := TypeAlias[baseType]; ok {
			baseType = ty_ //nolint:revive
		}
		defaultGenRule, ok = DefaultTypeGenRules[baseType].(GenRule)
		if !ok {
			return v
		}
	}
	if len(defaultGenRule) == 0 {
		return v
	}

	MergeGenRules(v.GenRule, defaultGenRule, false)

	return v
}

func (v *TypeVisitor) HasGenRule() bool {
	return len(v.GenRule) > 0
}

func (v *TypeVisitor) GetRule(name string, defaultValue ...any) any {
	if !v.HasGenRule() {
		return nil
	}
	if r, ok := v.GenRule[name]; ok {
		return r
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

func (v *TypeVisitor) GetMinMax() (any, any) {
	return v.GetRule("min"), v.GetRule("max")
}

func (v *TypeVisitor) GetLength() (minVal, maxVal int) {
	l := v.GetRule("length")
	if l == nil {
		logrus.Fatalf("length not found for column '%s'", v.Colpath)
	}

	switch l := l.(type) {
	case int, float32, float64:
		length := cast.ToInt(l)
		minVal, maxVal = length, length
	case GenRule:
		minVal, maxVal = cast.ToInt(l["min"]), cast.ToInt(l["max"])
	}
	if maxVal < minVal {
		logrus.Debugf("length max(%d) < min(%d), set max to min for column '%s'", maxVal, minVal, v.Colpath)
		minVal = maxVal
	}
	return
}

func (v *TypeVisitor) ChildGenRule(name string) GenRule {
	r := v.GetRule(name)
	if r == nil {
		return nil
	}
	return r.(GenRule) //nolint:revive
}

func (v *TypeVisitor) GetChildGen(name string, childType parser.IDataTypeContext, childGenRule ...GenRule) Gen {
	var visitor *TypeVisitor
	if len(childGenRule) > 0 {
		// If the child already has gen rule, use it
		visitor = NewTypeVisitor(v.Colpath+"."+name, childGenRule[0])
	} else {
		visitor = NewTypeVisitor(v.Colpath+"."+name, v.ChildGenRule(name))
	}

	// child visitor uses the same table ref records as root visitor's
	visitor.TableRefs = v.TableRefs

	return visitor.GetGen(childType)
}

func (v *TypeVisitor) GetNullFrequency() float32 {
	nullFrequency, err := cast.ToFloat32E(v.GetRule("null_frequency", GLOBAL_NULL_FREQUENCY))
	if err != nil || nullFrequency < 0 || nullFrequency > 1 {
		logrus.Fatalf("Invalid null frequency '%v' for column '%s': %v", v.GetRule("null_frequency"), v.Colpath, err)
	}
	return nullFrequency
}

type fgen[T any] struct {
	f func() T
}

func (g *fgen[T]) Gen() any {
	return g.f()
}

func NewFuncGen[T any](f func() T) Gen {
	return &fgen[T]{f: f}
}

func NewIntGen[T int8 | int16 | int | int32](minVal, maxVal T) Gen {
	return NewFuncGen(func() int { return gofakeit.IntRange(int(minVal), int(maxVal)) })
}
