package generator

import (
	crand "crypto/rand"
	"fmt"
	"maps"
	"math"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

const (
	ColumnSeparator = '☆' // make me happy
)

var (
	// gen data output insert sql or csv
	GenInsertOrCSV        bool
	CustomGenConstructors map[string]CustomGenConstructor
)

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

func Setup(genconf string, confIdx int, genInsertOrCSV bool) error {
	GenInsertOrCSV = genInsertOrCSV
	CleanRefData()
	return SetupGenRules(genconf, confIdx)
}

// Not thread safe!
type GenContext struct {
	// table name -> row index
	RefSourceTableRowIdx map[string]int

	// column value cache
	ColVals []any
}

func NewGenContext(colValCache []any) *GenContext {
	return &GenContext{
		RefSourceTableRowIdx: make(map[string]int),
		ColVals:              colValCache,
	}
}

func (c *GenContext) ClearCache() {
	clear(c.ColVals)
}

type GenRule = map[string]any
type Gen interface {
	Gen(ctx *GenContext) any
}

type CustomGenConstructor = func(v *ColumnVisitor, dataType parser.IDataTypeContext, r GenRule) (Gen, error)

type ColumnVisitor struct {
	Table   string   // table name (without db prefix)
	Columns []string // columns in the table
	Colpath string   // the path of the column, only for logging, e.g. "db.table.col"
	GenRule GenRule  // rules of generator

	// the tables that ref generator point to
	TableRefs *[]string
	// the columns the self ref generator point to
	SelfColRefs *[]string
}

func NewColumnVisitor(table string, columns []string, colpath string, genRule GenRule) *ColumnVisitor {
	if genRule == nil {
		genRule = GenRule{}
	}
	return &ColumnVisitor{
		Table:       table,
		Columns:     columns,
		Colpath:     colpath,
		GenRule:     genRule,
		TableRefs:   &[]string{},
		SelfColRefs: &[]string{},
	}
}

func (v *ColumnVisitor) GetGen(type_ parser.IDataTypeContext) Gen {
	var (
		g        Gen
		err      error
		baseType string
	)

	if type_ != nil {
		baseType = GetBaseType(type_, v.Colpath)
		// Merge global (aka. default) generation rules.
		v.MergeDefaultRule(type_)
	}
	if logrus.GetLevel() > logrus.DebugLevel {
		logrus.Tracef("gen rule of '%s': %s", v.Colpath, string(MustJSONMarshal(v.GenRule)))
	}

	if customGenRule, ok := v.GetRule("gen").(GenRule); ok {
		// 1. custom generator
		g = v.getCustomGen(type_, customGenRule)
	} else if type_ != nil {
		// 2. type generator (requires a data type)
		g = v.getTypeGen(type_, baseType)
	} else {
		logrus.Fatalf("Ghost column '%s' must have a 'gen' rule with a custom generator (inc, enum, parts, ref, type, golang)", v.Colpath)
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
		return NewFuncCtxGen(func(c *GenContext) any {
			if rand.Float32() < nullFrequency {
				return nil
			}
			return g.Gen(c)
		})
	}

	return g
}

func (v *ColumnVisitor) getCustomGen(type_ parser.IDataTypeContext, customGenRule GenRule) Gen {
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

//nolint:revive
func (v *ColumnVisitor) getTypeGen(type_ parser.IDataTypeContext, baseType string) Gen {
	var g Gen
	switch ty := type_.(type) {
	case *parser.ComplexDataTypeContext:
		switch baseType {
		case "ARRAY":
			// Handle array type
			elementType := ty.DataType(0)
			lenMin, lenMax := v.GetLength()
			g = NewArrayGen(elementType, v.GetChildGen("element", elementType), lenMin, lenMax)
		case "MAP":
			// Handle map type
			kv := ty.AllDataType()
			if len(kv) != 2 {
				logrus.Fatalf("Invalid map type: '%s' for column '%s', expected 2 types for key and value", ty.GetText(), v.Colpath)
			}

			// Handle key-value pair in map
			lenMin, lenMax := v.GetLength()
			mapgen := NewMapGen(
				kv[0], kv[1],
				v.GetChildGen("key", kv[0]), v.GetChildGen("value", kv[1]),
				lenMin, lenMax,
			)

			// map key is uniqued by default
			if kg := v.ChildGenRule("key"); kg != nil {
				if uniqueKey, ok := kg["unique"].(bool); ok {
					mapgen.SetKeyUniq(uniqueKey)
				}
			}
			g = mapgen
		case "STRUCT":
			// Handle struct type
			g_ := NewStructGen()

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
				if !ok || fieldName == "" {
					logrus.Fatalf("Struct field #%d has no name in column '%s'", i, v.Colpath)
				}
				i++
				return fieldName, field
			})
			for _, field := range ty.ComplexColTypeList().AllComplexColType() {
				fieldName := strings.Trim(field.Identifier().GetText(), "`")
				fieldType := field.DataType()
				g_.AddChild(fieldName, fieldType, v.GetChildGen(fieldName, fieldType, fields[fieldName]))
			}
			g = g_
		default:
			logrus.Fatalf("Unsupported complex type: '%s' for column '%s'", ty.GetComplex_().GetText(), v.Colpath)
		}
	case *parser.AggStateDataTypeContext:
		// Handle agg_state type
		// skip gen AGG_STATE, because it's usually generated from other values, e.g. `from: avg_state(decimal(20,9))`
		var (
			fn        = ty.FunctionNameIdentifier().GetText() + "_state"
			dataTypes = ty.GetDataTypes()
		)
		// set agg function and args to gen rule for later use, in src/gendata.go
		v.GenRule["agg_func"] = fn
		v.GenRule["agg_args"] = dataTypes

		// Handle each arg in the agg function
		args_ := v.GetRule("args")
		if args_ == nil {
			args_ = v.GetRule("arg")
		}
		argRules, ok := args_.([]any) // Ensure args is a slice of maps
		if !ok {
			if args_ != nil {
				logrus.Fatalf("Invalid agg args fields type '%T' for column '%s'", args_, v.Colpath)
			}
			argRules = lo.ToAnySlice([]GenRule{})
		}
		i := 0
		args := lo.SliceToMap(argRules, func(arg_ any) (int, GenRule) {
			arg, ok := arg_.(GenRule)
			if !ok {
				logrus.Fatalf("Invalid agg arg #%d in column '%s'", i, v.Colpath)
			}
			idx, err := cast.ToIntE(arg["name"])
			if err != nil {
				logrus.Fatalf("Invalid agg arg index name at #%d in column '%s'", i, v.Colpath)
			}
			i++
			return idx, arg
		})
		argsGens := lo.Map(dataTypes, func(dt parser.IDataTypeWithNullableContext, i int) Gen {
			arg, ok := args[i]
			if !ok {
				arg = GenRule{}
			}
			if dt.NOT() != nil {
				arg["null_frequency"] = 0.0
			}
			return v.GetChildGen(strconv.Itoa(i), dt.DataType(), arg)
		})
		isInsert := GenInsertOrCSV
		cs := ColumnSeparator
		if isInsert {
			cs = ','
		}
		g = NewFuncCtxGen(func(ctx *GenContext) any {
			var sb strings.Builder
			if isInsert {
				sb.WriteString(fn)
				sb.WriteRune('(')
			}
			for i, argGen := range argsGens {
				argVal := argGen.Gen(ctx)
				WriteCSVColVal(&sb, argVal)
				if i < len(argsGens)-1 {
					sb.WriteRune(cs)
				}
			}
			if isInsert {
				sb.WriteRune(')')
			}
			return sb.String()
		})
	case *parser.PrimitiveDataTypeContext, *parser.VariantPredefinedFieldsContext:
		min_, max_ := v.GetMinMax()
		charset := cast.ToString(v.GetRule("charset", ""))
		letters := cast.ToString(v.GetRule("letters", ""))
		switch baseType {
		case "BITMAP":
			// Generate a random bitmap array with a length between lenMin and lenMax
			lenMin, lenMax := v.GetLength()
			minVal, maxVal := CastMinMax[int64](min_, max_, baseType, v.Colpath)
			g = NewFuncGen(func() any {
				return json.RawMessage(MustJSONMarshal(lo.RepeatBy(rand.IntN(lenMax-lenMin+1)+lenMin, func(_ int) int64 {
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
			v.GenRule = genRule
			g = v.GetGen(dataType)
		case "BOOL", "BOOLEAN":
			enum := []int{0, 1}
			g = NewFuncGen(func() any { return enum[rand.IntN(2)] }) // BOOLEAN is typically 0 or 1
		case "TINYINT":
			minVal, maxVal := CastMinMax[int8](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "SMALLINT":
			minVal, maxVal := CastMinMax[int16](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "INT", "INTEGER":
			minVal, maxVal := CastMinMax[int32](min_, max_, baseType, v.Colpath)
			g = NewIntGen(minVal, maxVal)
		case "BIGINT":
			minVal, maxVal := CastMinMax[int64](min_, max_, baseType, v.Colpath)
			range_ := maxVal - minVal + 1
			g = NewFuncGen(func() int64 { return rand.Int64N(range_) + minVal })
		case "LARGEINT":
			minVal, maxVal := CastMinMax[*big.Int](min_, max_, baseType, v.Colpath)
			// fallback to BIGINT for fast generation
			if minVal.Cmp(big.NewInt(math.MinInt64)) >= 0 && maxVal.Cmp(big.NewInt(math.MaxInt64)) <= 0 {
				minVal, maxVal := minVal.Int64(), maxVal.Int64()
				range_ := maxVal - minVal + 1
				g = NewFuncGen(func() int64 { return rand.Int64N(range_) + minVal })
			} else {
				range_ := new(big.Int).Sub(maxVal, minVal)
				range_.Add(range_, big.NewInt(1)) // max - min + 1
				g = NewFuncGen(func() json.RawMessage {
					r, _ := crand.Int(crand.Reader, range_)
					return json.RawMessage(r.Add(r, minVal).String())
				})
			}
		case "FLOAT":
			minVal, maxVal := CastMinMax[float32](min_, max_, baseType, v.Colpath)
			range_ := maxVal - minVal
			g = NewFuncGen(func() any { return rand.Float32()*range_ + minVal })
		case "DOUBLE":
			minVal, maxVal := CastMinMax[float64](min_, max_, baseType, v.Colpath)
			range_ := maxVal - minVal
			g = NewFuncGen(func() any { return rand.Float64()*range_ + minVal })
		case "DECIMAL", "DECIMALV2", "DECIMALV3":
			var precision, scale int = 999, 999
			if v.GetRule("precision") != nil {
				precision = cast.ToInt(v.GetRule("precision"))
			}
			if v.GetRule("scale") != nil {
				scale = cast.ToInt(v.GetRule("scale"))
			}

			intVals := ty.(*parser.PrimitiveDataTypeContext).AllINTEGER_VALUE()
			var p, s int
			if len(intVals) > 0 {
				p = cast.ToInt(intVals[0].GetText())
			} else {
				p = 12
			}
			if p > 76 {
				logrus.Fatalf("Decimal precision '%d' is larger than the maximum allowed precision of 76 for column '%s', using 76 instead", p, v.Colpath)
			}
			precision = min(precision, p)
			if len(intVals) > 1 {
				s = cast.ToInt(intVals[1].GetText())
			} else {
				s = 2
			}
			if s < 0 || s > precision {
				s = 0
			}
			scale = min(scale, s)

			intLen := precision - scale
			maxIntFromLen := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(intLen)), nil), big.NewInt(1))
			if min_ == nil {
				min_ = "0" // Default min value
			}
			if max_ == nil {
				max_ = maxIntFromLen.String()
			}

			// FIXME: Care about the scale of decimal?
			minInt, maxInt := CastMinMax[*big.Int](min_, max_, baseType, v.Colpath)
			// should not lagger than precision
			if maxInt.Cmp(maxIntFromLen) > 0 {
				maxInt = maxIntFromLen
			}
			if minInt.Cmp(maxInt) > 0 {
				minInt = big.NewInt(0)
			}

			// max - min + 1
			intDelta := new(big.Int).Sub(maxInt, minInt)
			intDelta.Add(intDelta, big.NewInt(1))
			scaleDelta := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)

			// fallback to BIGINT for fast generation
			if minInt.Cmp(big.NewInt(math.MinInt64)) >= 0 && maxInt.Cmp(big.NewInt(math.MaxInt64)) <= 0 && scaleDelta.Cmp(big.NewInt(math.MaxInt64)) <= 0 {
				minInt, maxInt := minInt.Int64(), maxInt.Int64()
				intDelta := maxInt - minInt + 1
				scaleDelta := scaleDelta.Int64()
				g = NewFuncGen(func() any {
					intPart := rand.Int64N(intDelta) + minInt
					scalePart := rand.Int64N(scaleDelta)
					return json.RawMessage(fmt.Sprintf("%d.%d", intPart, scalePart)) // Format as decimal string
				})
			} else {
				g = NewFuncGen(func() any {
					intPart, _ := crand.Int(crand.Reader, intDelta)
					intPart.Add(intPart, minInt)
					scalePart, _ := crand.Int(crand.Reader, scaleDelta)
					return json.RawMessage(fmt.Sprintf("%s.%s", intPart.String(), scalePart.String())) // Format as decimal string
				})
			}
		case "DATE", "DATEV1", "DATEV2":
			minVal, maxVal := CastMinMax[time.Time](min_, max_, baseType, v.Colpath)
			minUnix, maxUnix := minVal.Unix(), maxVal.Unix()
			range_ := maxUnix - minUnix + 1
			g = NewFuncGen(func() any { return time.Unix(rand.Int64N(range_)+minUnix, 0).UTC().Format("2006-01-02") })
		case "DATETIME", "DATETIMEV1", "DATETIMEV2", "TIMESTAMP":
			minVal, maxVal := CastMinMax[time.Time](min_, max_, baseType, v.Colpath)
			minUnix, maxUnix := minVal.UnixNano(), maxVal.UnixNano()
			range_ := maxUnix - minUnix + 1
			g = NewFuncGen(func() any {
				return time.Unix(0, rand.Int64N(range_)+minUnix).UTC().Format("2006-01-02 15:04:05")
			})
		case "TEXT", "STRING":
			lenMin, lenMax := v.GetLength()
			lenMin = max(1, lenMin)
			lenMax = max(1, lenMax)
			g = RandomStrGen(lenMin, lenMax, charset, letters)
		case "VARCHAR":
			var (
				length         int
				lenMin, lenMax = v.GetLength()
			)
			lenMin = max(1, lenMin)
			lenMax = max(1, lenMax)
			length_ := ty.(*parser.PrimitiveDataTypeContext).INTEGER_VALUE(0)
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
			g = RandomStrGen(lenMin, lenMax, charset, letters)
		case "CHAR", "CHARACTER":
			length_ := ty.(*parser.PrimitiveDataTypeContext).INTEGER_VALUE(0)
			if length_ == nil {
				logrus.Fatalf("CHAR type must have a length in column '%s'", v.Colpath)
			}
			length := min(max(1, cast.ToInt(length_.GetText())), 255)
			g = RandomStrGen(length, length, charset, letters)
		case "IPV4":
			num := func() int { return rand.IntN(256) }
			g = NewFuncGen(func() any { return fmt.Sprintf("%d.%d.%d.%d", num(), num(), num(), num()) })
		case "IPV6":
			num := func() int { return rand.IntN(65536) }
			g = NewFuncGen(func() any {
				return fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x", num(), num(), num(), num(), num(), num(), num(), num())
			})
		case "HLL", "AGG_STATE":
			// skip gen HLL
			g = NewFuncGen(func() any { return "" })
		default: // TODO: AGG_STATE, QUANTILE_STATE
			logrus.Fatalf("Unsupported column type '%s' for column '%s'", type_.GetText(), v.Colpath)
		}
	}
	return g
}

func GetBaseType(type_ parser.IDataTypeContext, colpath string) (t string) {
	t = parser.GetBaseType(type_)
	if t == "" {
		logrus.Fatalf("Failed to get column type '%s' for column '%s'", type_.GetText(), colpath)
	}
	return strings.ToUpper(t)
}

var spaceReplacer = strings.NewReplacer(
	"\n", "",
	"\r", "",
	" ", "",
)

func (v *ColumnVisitor) MergeDefaultRule(type_ parser.IDataTypeContext) *ColumnVisitor {
	ty := spaceReplacer.Replace(strings.ToUpper(type_.GetText()))
	logType := ty
	defaultGenRule, ok := DefaultTypeGenRules[ty].(GenRule)
	if !ok {
		baseType := GetBaseType(type_, v.Colpath)
		logType = baseType
		defaultGenRule, ok = DefaultTypeGenRules[baseType].(GenRule)
		if !ok {
			if baseType, ok = TypeAlias[baseType]; ok {
				defaultGenRule, ok = DefaultTypeGenRules[baseType].(GenRule)
			}
		}
		if !ok {
			return v
		}
	}
	if len(defaultGenRule) == 0 {
		return v
	}

	logrus.Traceln("merging default gen rule:", defaultGenRule, "for column:", v.Colpath, logType)
	MergeGenRules(v.GenRule, defaultGenRule, false)

	return v
}

func (v *ColumnVisitor) HasGenRule() bool {
	return len(v.GenRule) > 0
}

func (v *ColumnVisitor) GetRule(name string, defaultValue ...any) any {
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

func (v *ColumnVisitor) GetMinMax() (any, any) {
	return v.GetRule("min"), v.GetRule("max")
}

func (v *ColumnVisitor) GetLength() (minVal, maxVal int) {
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
	default:
		logrus.Fatalf("invalid length rule type %T for column '%s'", l, v.Colpath)
	}
	if maxVal < minVal {
		logrus.Debugf("length max(%d) < min(%d), set max to min for column '%s'", maxVal, minVal, v.Colpath)
		minVal = maxVal
	}
	return
}

func (v *ColumnVisitor) ChildGenRule(name string) GenRule {
	r := v.GetRule(name)
	if r == nil {
		return nil
	}
	return r.(GenRule) //nolint:revive
}

func (v *ColumnVisitor) GetChildGen(name string, childType parser.IDataTypeContext, childGenRule ...GenRule) Gen {
	var visitor *ColumnVisitor
	if len(childGenRule) > 0 {
		// If the child already has gen rule, use it
		visitor = NewColumnVisitor(v.Table, v.Columns, v.Colpath+"."+name, childGenRule[0])
	} else {
		visitor = NewColumnVisitor(v.Table, v.Columns, v.Colpath+"."+name, v.ChildGenRule(name))
	}

	// child visitor uses the same table/column ref records as root visitor's
	visitor.TableRefs = v.TableRefs
	visitor.SelfColRefs = v.SelfColRefs

	return visitor.GetGen(childType)
}

func (v *ColumnVisitor) GetNullFrequency() float32 {
	nullFrequency, err := cast.ToFloat32E(v.GetRule("null_frequency", GLOBAL_NULL_FREQUENCY))
	if err != nil || nullFrequency < 0 || nullFrequency > 1 {
		logrus.Fatalf("Invalid null frequency '%v' for column '%s': %v", v.GetRule("null_frequency"), v.Colpath, err)
	}
	return nullFrequency
}

type fgen[T any] struct {
	f func() T
}

func (g *fgen[T]) Gen(_ *GenContext) any {
	return g.f()
}

func NewFuncGen[T any](f func() T) Gen {
	return &fgen[T]{f: f}
}

type fcgen[T any] struct {
	f func(*GenContext) T
}

func (g *fcgen[T]) Gen(c *GenContext) any {
	return g.f(c)
}

func NewFuncCtxGen[T any](f func(*GenContext) T) Gen {
	return &fcgen[T]{f: f}
}

func NewIntGen[T int8 | int16 | int | int32](minVal, maxVal T) Gen {
	minInt, maxInt := int(minVal), int(maxVal)
	range_ := maxInt - minInt + 1
	return NewFuncGen(func() int { return rand.IntN(range_) + minInt })
}
