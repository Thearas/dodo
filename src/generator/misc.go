package generator

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

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

func RandomStr(lenMin, lenMax int) string {
	const allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	length := gofakeit.IntRange(lenMin, lenMax)
	b := make([]byte, length)
	for i := range length {
		b[i] = allowed[rand.IntN(len(allowed))]
	}
	return string(b)
}
