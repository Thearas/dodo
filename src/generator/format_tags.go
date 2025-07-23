package generator

import (
	"reflect"
	"slices"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/iancoleman/strcase"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var FormatTags map[string]reflect.Value

// functions that wont work with template engine
var templateExclusion = []string{
	// copy from `gofakeit/template.go#templateExclusion`
	"RandomMapKey",
	"SQL",
	"Template",

	"Book",
	"Song",
	"Contact",
	"RgbColor",
	"Car",
	"ProductAudience",
	"Product",
	"Job",
	"NiceColors",
	"Person",
	"Address",
	"Currency",
	"CreditCard",
	"Map",
	"Movie",
}

func SetupFormatTags() {
	if FormatTags != nil {
		// already setup
		return
	}

	v := reflect.ValueOf(gofakeit.GlobalFaker)

	FormatTags = map[string]reflect.Value{}

	// Add all zero args fake functions to tags
	for i := range v.NumMethod() {
		// check if the method is in the exclusion list
		if slices.Contains(templateExclusion, v.Type().Method(i).Name) {
			continue
		}

		// Check if method has no args
		// If not don't add to function map
		if v.Type().Method(i).Type.NumIn() != 1 {
			continue
		}

		// Check if method has 1 return values
		// If not don't add to function map
		if v.Type().Method(i).Type.NumOut() != 1 {
			continue
		}

		tagName := strcase.ToSnake(v.Type().Method(i).Name)

		// add the method to the function map
		FormatTags[tagName] = v.Method(i)
	}

	if logrus.GetLevel() > logrus.DebugLevel {
		logrus.Traceln("setup all generator format tags:", lo.MapToSlice(FormatTags, func(k string, _ reflect.Value) string {
			return k
		}))
	}

	// print tags markdown table
	// fmt.Println("| Name | Return Type |")
	// fmt.Println("| --- | --- |")
	// fmt.Println(strings.Join(lo.MapToSlice(FormatTags, func(k string, v reflect.Value) string {
	// 	return "| " + k + " | " + v.Call(nil)[0].Type().Name() + " |"
	// }), "\n"))
}
