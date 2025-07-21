package generator

import (
	"fmt"
	"io"
	"math/rand/v2"
	"time"
	"unsafe"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/goccy/go-json"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

type ColValWriter interface {
	io.Writer
	io.StringWriter
}

func WriteColVal(w ColValWriter, val any) (int, error) {
	if val == nil {
		return w.WriteString(`\N`)
	}

	switch v := val.(type) {
	case string:
		return w.WriteString(v)
	case json.RawMessage:
		return w.Write(v)
	case []byte:
		return w.Write(v)
	default:
		return fmt.Fprint(w, val)
	}
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

func CastMinMax[R int8 | int16 | int | int32 | int64 | float32 | float64 | time.Time](min_, max_ any, baseType, colpath string, errmsg ...string) (R, R) {
	minVal, maxVal, err := Cast2[R](min_, max_)
	if err != nil {
		msg := fmt.Sprintf("Invalid min/max %s '%v/%v' for column '%s': %v, expect %T", baseType, min_, max_, colpath, err, minVal)
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
	}
	if minBigger {
		logrus.Warnf("Column '%s' max(%v) < min(%v), set max to min\n", colpath, maxVal, minVal)
		maxVal = minVal
	}
	return minVal, maxVal
}

type CastType interface {
	int8 | int16 | int | int32 | int64 | float32 | float64 | string | time.Time
}

func Cast2[R CastType](v1, v2 any) (r1, r2 R, err error) {
	r1, err = Cast[R](v1)
	if err != nil {
		return
	}
	r2, err = Cast[R](v2)
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
		r_, err = cast.ToInt16E(v)
	case time.Time:
		r_, err = cast.ToTimeE(v)
	default:
		return r, fmt.Errorf("unsupported cast type '%T' to '%T'", v, r)
	}

	if converted, ok := r_.(R); ok {
		return converted, err
	}
	panic("unreachable")
}

func MustJSONMarshal(v any) []byte {
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
func RandomStr(lenMin, lenMax int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	n := gofakeit.IntRange(lenMin, lenMax)
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
