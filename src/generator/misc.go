package generator

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"go.yaml.in/yaml/v4"

	"github.com/Thearas/dodo/src/parser"
)

var (
	NeedCastInInsertVal bool
	NumberRe            = regexp.MustCompile(`\d+`)
)

type ColValWriter interface {
	io.Writer
	io.StringWriter
}

type WriteColVal func(w ColValWriter, val any) (int, error)

func WriteCSVColVal(w ColValWriter, val any) (int, error) {
	if val == nil {
		return w.WriteString(`\N`)
	}

	switch v := val.(type) {
	case string:
		return w.WriteString(v)
	case []byte:
		return w.Write(v)
	case json.RawMessage:
		return w.Write(v)
	default:
		return fmt.Fprint(w, val)
	}
}

func GetInsertColValWriter(type_ parser.IDataTypeContext) WriteColVal {
	baseType := strings.ToUpper(parser.GetBaseType(type_))

	var needCastTo string
	if NeedCastInInsertVal &&
		!IsComplexType(baseType) && !slices.Contains([]string{"STRING", "INT"}, ToCommonType(baseType)) {
		switch baseType {
		case "BITMAP":
			needCastTo = "ARRAY<BIGINT(20)>"
		default:
			needCastTo = ToCommonType(strings.ToUpper(type_.GetText()))
		}
	}

	var transFunc string
	switch baseType {
	case "BITMAP":
		transFunc = "bitmap_from_array"
	case "HLL":
		logrus.Warnln("Not support inserting HLL type yet, fallback to hll_empty()")
		transFunc = "hll_empty"
	default:
	}

	valF := QuoteValFunc(type_)

	//nolint:revive
	return func(w ColValWriter, val any) (n int, e error) {
		if val == nil {
			return w.WriteString(`null`)
		}
		if transFunc != "" {
			w.WriteString(transFunc)
			w.WriteString("(")
		}
		if needCastTo != "" {
			w.WriteString(`CAST(`)
		}

		// write value
		n, e = valF(w, val)

		if needCastTo != "" {
			fmt.Fprintf(w, ` AS %s)`, needCastTo)
		}
		if transFunc != "" {
			w.WriteString(")")
		}
		return n, e
	}
}

func QuoteValFunc(type_ parser.IDataTypeContext) WriteColVal {
	baseType := strings.ToUpper(parser.GetBaseType(type_))

	needQuote := slices.Contains([]string{
		"DATE", "DATEV1", "DATEV2", "DATETIME", "DATETIMEV1", "DATETIMEV2", "TIMESTAMP",
		"TEXT", "STRING", "VARCHAR", "CHAR", "CHARACTER",
		"IPV4", "IPV6",
	}, baseType)

	//nolint:revive
	return func(w ColValWriter, val any) (n int, e error) {
		if val == nil {
			return w.WriteString(`null`)
		}

		switch v := val.(type) {
		case string:
			if needQuote {
				n, e = w.Write(MustJSONMarshal(v))
			} else {
				n, e = w.WriteString(v)
			}
		case []byte:
			if needQuote {
				n, e = w.Write(MustJSONMarshal(string(v)))
			} else {
				n, e = w.Write(v)
			}
		case json.RawMessage:
			n, e = w.Write(v)
		default:
			n, e = fmt.Fprint(w, val)
		}
		return n, e
	}
}

var commonTypeReplacer = strings.NewReplacer(
	"V1", "",
	"V2", "",
	"V3", "",
	"DATETIME(3)", "TIMESTAMP",
	"DATETIME(6)", "TIMESTAMP",
	"DATETIME", "TIMESTAMP",
	"VARCHAR(2147483647)", "STRING",
	"TEXT", "STRING",
)

// Convert doris type to more common-used type (like Hive/Spark).
func ToCommonType(ty string) string {
	return commonTypeReplacer.Replace(ty)
}

//nolint:revive
func MergeGenRules(dst, src GenRule, overwrite bool) {
	for k, v := range src {
		if overwrite {
			dst[k] = CloneGenRules(v)
		} else if _, ok := dst[k]; !ok {
			dst[k] = CloneGenRules(v)
		}
	}
}

func CloneGenRules(src any) any {
	// if src is a slice, copy its elements
	if s, ok := src.([]any); ok {
		return lo.Map(s, func(v any, _ int) any { return CloneGenRules(v) })
	}

	// if src is a GenRule, copy its values
	r, ok := src.(GenRule)
	if ok {
		return lo.MapValues(r, func(v any, _ string) any { return CloneGenRules(v) })
	}

	// otherwise, return the original value
	return src
}

func IsComplexType(baseType string) bool {
	return slices.Contains([]string{"ARRAY", "MAP", "STRUCT", "JSON", "JSONB", "VARIANT", "HLL"}, baseType)
}

func CastMinMax[R CastType](min_, max_ any, baseType, colpath string, errmsg ...string) (R, R) {
	minVal, maxVal, err := CastMinMaxImpl[R](min_, max_)
	if err != nil {
		msg := fmt.Sprintf("Invalid min/max '%v/%v' for %s column '%s': %v", min_, max_, baseType, colpath, err)
		if len(errmsg) > 0 {
			msg += ", " + errmsg[0]
		}
		logrus.Fatalln(msg)
	}

	minBigger := false
	switch any(minVal).(type) {
	case int8:
		minBigger = any(maxVal).(int8) < any(minVal).(int8)
	case int16:
		minBigger = any(maxVal).(int16) < any(minVal).(int16)
	case int:
		minBigger = any(maxVal).(int) < any(minVal).(int)
	case int32:
		minBigger = any(maxVal).(int32) < any(minVal).(int32)
	case int64:
		minBigger = any(maxVal).(int64) < any(minVal).(int64)
	case float32:
		minBigger = any(maxVal).(float32) < any(minVal).(float32)
	case float64:
		minBigger = any(maxVal).(float64) < any(minVal).(float64)
	case time.Time:
		minBigger = any(maxVal).(time.Time).Before(any(minVal).(time.Time))
	case big.Int:
		minBigger = any(minVal).(*big.Int).Cmp(any(maxVal).(*big.Int)) == -1
	}
	if minBigger {
		maxVal_, err := MinMaxVal[R](false)
		if err == nil {
			logrus.Warnf("Column '%s' max(%v) < min(%v), set max to %v", colpath, maxVal, minVal, maxVal)
			maxVal = maxVal_
		} else {
			logrus.Warnf("Column '%s' max(%v) < min(%v), set max to min", colpath, maxVal, minVal)
			maxVal = minVal
		}
	}
	return minVal, maxVal
}

type CastTypePrimaryNumber interface {
	int8 | int16 | int | int32 | int64 | float32 | float64
}

type CastType interface {
	CastTypePrimaryNumber | string | time.Time | *big.Int
}

func CastMinMaxImpl[R CastType](v1, v2 any) (r1, r2 R, err error) {
	r1, err = Cast[R](v1)
	if err != nil {
		// raw value is string and construct by number
		s, ok := v1.(string)
		if !(ok && strings.Contains(fmt.Sprint(err), "unable to cast") && NumberRe.MatchString(s)) {
			return
		}
		var err_ error
		r1, err_ = MinMaxVal[R](true)
		if err_ != nil {
			return
		}
		// err = nil
	}
	r2, err = Cast[R](v2)
	if err != nil {
		// raw value is string and construct by number
		s, ok := v2.(string)
		if !(ok && strings.Contains(fmt.Sprint(err), "unable to cast") && NumberRe.MatchString(s)) {
			return
		}
		var err_ error
		r2, err_ = MinMaxVal[R](false)
		if err_ != nil {
			return
		}
		err = nil
	}
	return
}

func Cast[R CastType](v any) (r R, err error) {
	var r_ any

	switch any(r).(type) {
	case int8:
		r_, err = cast.ToInt8E(v)
	case int16:
		r_, err = cast.ToInt16E(v)
	case int:
		r_, err = cast.ToIntE(v)
	case int32:
		r_, err = cast.ToInt32E(v)
	case int64:
		r_, err = cast.ToInt64E(v)
	case float32:
		r_, err = cast.ToFloat32E(v)
	case float64:
		r_, err = cast.ToFloat64E(v)
	case string:
		r_, err = cast.ToStringE(v)
	case time.Time:
		r_, err = cast.ToTimeE(v)
	case *big.Int:
		var s string
		s, err = cast.ToStringE(v)
		if err != nil {
			return r, err
		}
		// for decimal, remove numbers behind the dot
		s = strings.SplitN(s, ".", 2)[0]
		b := new(big.Int)
		if _, ok := b.SetString(s, 10); !ok {
			return r, fmt.Errorf("unable cast '%s' to '%T'", s, r)
		}
		r_ = b
	default:
		return r, fmt.Errorf("unsupported cast type '%T' to '%T'", v, r)
	}

	if converted, ok := r_.(R); ok {
		return converted, err
	}
	panic("unreachable")
}

//nolint:revive
func MinMaxVal[R CastType](minOrMax bool) (R, error) {
	var (
		r_ any
		r  R
	)
	switch any(r).(type) {
	case int8:
		r_ = int8(math.MinInt8)
		if !minOrMax {
			r_ = int8(math.MaxInt8)
		}
	case int16:
		r_ = int16(math.MinInt16)
		if !minOrMax {
			r_ = int16(math.MaxInt16)
		}
	case int32:
		r_ = int32(math.MinInt32)
		if !minOrMax {
			r_ = int32(math.MaxInt32)
		}
	case int64:
		r_ = int64(math.MinInt64)
		if !minOrMax {
			r_ = int64(math.MaxInt64)
		}
	case float32:
		r_ = float32(-math.MaxFloat32)
		if !minOrMax {
			r_ = float32(math.MaxFloat32)
		}
	case float64:
		r_ = -math.MaxFloat64
		if !minOrMax {
			r_ = math.MaxFloat64
		}
	default:
		return r, errors.New("unsupported type")
	}
	if converted, ok := r_.(R); ok {
		return converted, nil
	}
	panic("unreachable")
}

func MustJSONMarshal(v any) []byte {
	if v, ok := v.(json.RawMessage); ok {
		return v
	}
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func MustYAMLUmarshal(s string) map[string]any {
	result := map[string]any{}
	if err := yaml.Unmarshal([]byte(s), result); err != nil {
		panic(err)
	}
	return result
}

// https://stackoverflow.com/a/31832326/7929631
func RandomStr(lenMin, lenMax int, bLetters ...string) string {
	var letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if len(bLetters) > 0 && len(bLetters[0]) > 0 {
		letterBytes = bLetters[0]
	}
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	n := rand.IntN(lenMax-lenMin+1) + lenMin
	b := make([]byte, n)
	// A rand.Int64() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int64(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func RandomStrGen(lenMin, lenMax int, charset string, letters ...string) Gen {
	switch strings.ToLower(charset) {
	case "english":
		return NewFuncGen(func() string {
			return lo.RandomString(rand.IntN(lenMax-lenMin+1)+lenMin, lo.LettersCharset)
		})
	default:
		if len(letters) == 0 || isASCII(letters[0]) {
			return NewFuncGen(func() string {
				return RandomStr(lenMin, lenMax, letters...)
			})
		}
		return NewFuncGen(func() string {
			return lo.RandomString(rand.IntN(lenMax-lenMin+1)+lenMin, []rune(letters[0]))
		})
	}
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > 127 {
			return false
		}
	}
	return true
}
