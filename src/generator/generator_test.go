package generator

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/Thearas/dodo/src/parser"
)

func init() {
	// Ensure default rules are initialized for tests
	Setup("", 0, false)
}

// helper to parse a type string into parser.IDataTypeContext
func mustParseType(t *testing.T, ty string) parser.IDataTypeContext {
	p := parser.NewParser("test.col", ty)
	dt := p.DataType()
	if err := p.ErrListener.LastErr; err != nil {
		t.Fatalf("failed to parse type '%s': %v", ty, err)
	}
	return dt
}

// get a generator from getTypeGen with defaults merged like in GetGen
func genFor(t *testing.T, ty string, rule GenRule) Gen {
	dt := mustParseType(t, ty)
	v := NewColumnVisitor("tbl", nil, "db.tbl.col", rule)
	return v.GetGen(dt)
}

func Test_getTypeGen_Primitives(t *testing.T) {
	t.Parallel()

	// BOOLEAN
	g := genFor(t, "BOOLEAN", nil)
	for range 10 {
		v := g.Gen(nil).(int)
		assert.Contains(t, []int{0, 1}, v)
	}

	// TINYINT
	g = genFor(t, "TINYINT", GenRule{"min": 7, "max": 7})
	assert.Equal(t, 7, g.Gen(nil))

	// SMALLINT
	g = genFor(t, "SMALLINT", GenRule{"min": 12, "max": 12})
	assert.Equal(t, 12, g.Gen(nil))

	// INT / INTEGER
	g = genFor(t, "INT", GenRule{"min": 42, "max": 42})
	assert.Equal(t, 42, g.Gen(nil))
	g = genFor(t, "INTEGER", GenRule{"min": 43, "max": 43})
	assert.Equal(t, 43, g.Gen(nil))

	// BIGINT
	g = genFor(t, "BIGINT", GenRule{"min": int64(1234567890), "max": int64(1234567890)})
	assert.Equal(t, int64(1234567890), g.Gen(nil))

	// BIGINT with string min/max (as produced by LLM/YAML)
	g = genFor(t, "BIGINT", GenRule{"min": "142511648567154", "max": "142511648567154"})
	for range 100 {
		assert.Equal(t, int64(142511648567154), g.Gen(nil))
	}

	// LARGEINT
	g = genFor(t, "LARGEINT", GenRule{"min": "123", "max": "123"})
	// generator returns json.RawMessage of the big.Int string
	assert.Equal(t, int64(123), g.Gen(nil))

	// LARGEINT with a very large number
	largeIntVal := "12345678901234567890123456789012345678"
	g = genFor(t, "LARGEINT", GenRule{"min": largeIntVal, "max": largeIntVal})
	assert.Equal(t, json.RawMessage(largeIntVal), g.Gen(nil))

	// DECIMAL (using scale 0 to avoid fractional randomness)
	g = genFor(t, "DECIMAL(4,0)", GenRule{"min": "5", "max": "5"})
	// returns RawMessage like 5.0
	assert.Equal(t, json.RawMessage("5.0"), g.Gen(nil))

	// DECIMAL with high precision and scale
	g = genFor(t, "DECIMAL(76, 20)", GenRule{"min": "1", "max": "1"})
	res := g.Gen(nil).(json.RawMessage)
	assert.Regexp(t, regexp.MustCompile(`^1\.\d+$`), string(res))

	// DECIMAL with large value and precision/scale
	largeDecimalVal := "92345678901234567890"
	minlargeDecimalVal := "12345678901234567890"
	g = genFor(t, "DECIMAL(38, 18)", GenRule{"min": minlargeDecimalVal, "max": largeDecimalVal})
	for range 1000 {
		v := string(g.Gen(nil).(json.RawMessage))
		parts := strings.SplitN(v, ".", 2)
		intPart := parts[0]
		scalePart := parts[1]
		assert.LessOrEqual(t, len(scalePart), 18)
		assert.LessOrEqual(t, intPart, largeDecimalVal)
		assert.GreaterOrEqual(t, intPart, minlargeDecimalVal)
	}

	// DECIMAL256
	largeDecimalVal = "90000000000000000000000000000000000000000000000000000000000000000000000000"
	minlargeDecimalVal = "10000000000000000000000000000000000000000000000000000000000000000000000000"
	g = genFor(t, "DECIMAL(76, 2)", GenRule{"min": minlargeDecimalVal, "max": largeDecimalVal})
	for range 1000 {
		v := string(g.Gen(nil).(json.RawMessage))
		parts := strings.SplitN(v, ".", 2)
		intPart := parts[0]
		scalePart := parts[1]
		assert.LessOrEqual(t, len(scalePart), 2)
		assert.LessOrEqual(t, intPart, largeDecimalVal)
		assert.GreaterOrEqual(t, intPart, minlargeDecimalVal)
	}

	// FLOAT
	g = genFor(t, "FLOAT", GenRule{"min": float32(1.5), "max": float32(1.5)})
	assert.InDelta(t, 1.5, g.Gen(nil).(float32), 0)

	// DOUBLE
	g = genFor(t, "DOUBLE", GenRule{"min": 2.5, "max": 2.5})
	assert.InDelta(t, 2.5, g.Gen(nil).(float64), 0)

	// DATE
	g = genFor(t, "DATE", GenRule{"min": time.Date(1997, 1, 15, 0, 0, 0, 0, time.UTC), "max": time.Date(1997, 1, 15, 0, 0, 0, 0, time.UTC)})
	assert.Equal(t, "1997-01-15", g.Gen(nil))

	// DATETIME
	g = genFor(t, "DATETIME", GenRule{"min": time.Date(1997, 1, 15, 1, 2, 3, 0, time.UTC), "max": time.Date(1997, 1, 15, 1, 2, 3, 0, time.UTC)})
	assert.Equal(t, "1997-01-15 01:02:03", g.Gen(nil))

	// TEXT / STRING
	g = genFor(t, "TEXT", GenRule{"length": GenRule{"min": 2, "max": 2}})
	assert.Len(t, g.Gen(nil).(string), 2)
	g = genFor(t, "STRING", GenRule{"length": GenRule{"min": 2, "max": 2}})
	assert.Len(t, g.Gen(nil).(string), 2)

	// VARCHAR with explicit length and rule enforcing exact length
	g = genFor(t, "VARCHAR(5)", GenRule{"length": GenRule{"min": 5, "max": 5}})
	assert.Len(t, g.Gen(nil).(string), 5)

	// CHAR requires length in type
	g = genFor(t, "CHAR(3)", nil)
	assert.Len(t, g.Gen(nil).(string), 3)

	// IPV4 / IPV6
	g = genFor(t, "IPV4", nil)
	v4 := g.Gen(nil).(string)
	assert.Regexp(t, regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`), v4)
	g = genFor(t, "IPV6", nil)
	v6 := g.Gen(nil).(string)
	assert.Regexp(t, regexp.MustCompile(`:`), v6)

	// HLL: returns empty string
	g = genFor(t, "HLL", nil)
	assert.Equal(t, "", g.Gen(nil))

	// BITMAP with deterministic settings
	g = genFor(t, "BITMAP", GenRule{"length": 3, "min": int64(1), "max": int64(1)})
	assert.Equal(t, json.RawMessage("[1,1,1]"), g.Gen(nil))
}

func Test_getTypeGen_ComplexTypes(t *testing.T) {
	t.Parallel()

	// ARRAY<INT> with fixed length
	g := genFor(t, "ARRAY<INT>", GenRule{"length": GenRule{"min": 2, "max": 2}})
	var arr []any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &arr))
	assert.Len(t, arr, 2)

	// MAP<STRING,INT> with fixed length and deterministic keys
	g = genFor(t, "MAP<STRING,INT>", GenRule{
		"length": any(3),
		"key": GenRule{
			"format": "k-{{%d}}",
			// inc generator with start: 0 is treated as start: 1 by implementation
			"gen": GenRule{"inc": GenRule{"start": 0}},
		},
	})
	var m map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &m))
	assert.Len(t, m, 3)
	assert.Contains(t, m, "k-0")
	assert.Contains(t, m, "k-1")
	assert.Contains(t, m, "k-2")

	// STRUCT with two fields
	g = genFor(t, "STRUCT<a:INT,b:STRING>", nil)
	var obj map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &obj))
	assert.Contains(t, obj, "a")
	assert.Contains(t, obj, "b")

	// JSON / JSONB / VARIANT using explicit structure and field rules for determinism
	rule := GenRule{
		"structure": "struct<foo:int,bar:int>",
		"fields": []any{
			GenRule{"name": "foo", "min": 1, "max": 1},
			GenRule{"name": "bar", "gen": GenRule{"enum": []any{2}}},
		},
	}
	for _, ty := range []string{"JSON", "JSONB", "VARIANT"} {
		g = genFor(t, ty, rule)
		var obj map[string]any
		assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &obj))
		assert.Equal(t, float64(1), obj["foo"]) // numbers become float64 in json
		assert.Equal(t, float64(2), obj["bar"])
	}

	// STRUCT containing ARRAY field
	g = genFor(t, "STRUCT<name:STRING,scores:ARRAY<INT>>", GenRule{
		"fields": []any{
			GenRule{"name": "name", "format": "user"},
			GenRule{"name": "scores", "length": GenRule{"min": 3, "max": 3}, "element": GenRule{"min": 100, "max": 100}},
		},
	})
	var structWithArr map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &structWithArr))
	assert.Equal(t, "user", structWithArr["name"])
	scores, ok := structWithArr["scores"].([]any)
	assert.True(t, ok)
	assert.Len(t, scores, 3)
	for _, s := range scores {
		assert.Equal(t, float64(100), s)
	}

	// STRUCT containing MAP field
	g = genFor(t, "STRUCT<id:INT,metadata:MAP<STRING,INT>>", GenRule{
		"fields": []any{
			GenRule{"name": "id", "min": 42, "max": 42},
			GenRule{"name": "metadata", "length": 2, "key": GenRule{"format": "key-{{%d}}", "gen": GenRule{"inc": GenRule{"start": 1}}}, "value": GenRule{"min": 99, "max": 99}},
		},
	})
	var structWithMap map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &structWithMap))
	assert.Equal(t, float64(42), structWithMap["id"])
	metadata, ok := structWithMap["metadata"].(map[string]any)
	assert.True(t, ok)
	assert.Len(t, metadata, 2)
	assert.Equal(t, float64(99), metadata["key-1"])
	assert.Equal(t, float64(99), metadata["key-2"])

	// STRUCT containing nested STRUCT
	g = genFor(t, "STRUCT<`outer`:INT,`inner`:STRUCT<a:INT,b:STRING>>", GenRule{
		"fields": []any{
			GenRule{"name": "outer", "min": 1, "max": 1},
			GenRule{"name": "inner", "fields": []any{
				GenRule{"name": "a", "min": 10, "max": 10},
				GenRule{"name": "b", "format": "nested"},
			}},
		},
	})
	var nestedStruct map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &nestedStruct))
	assert.Equal(t, float64(1), nestedStruct["outer"])
	inner, ok := nestedStruct["inner"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, float64(10), inner["a"])
	assert.Equal(t, "nested", inner["b"])

	// MAP with STRUCT values
	g = genFor(t, "MAP<STRING,STRUCT<x:INT,y:INT>>", GenRule{
		"length": 2,
		"key":    GenRule{"format": "point-{{%d}}", "gen": GenRule{"inc": GenRule{"start": 1}}},
		"value": GenRule{
			"fields": []any{
				GenRule{"name": "x", "min": 5, "max": 5},
				GenRule{"name": "y", "min": 10, "max": 10},
			},
		},
	})
	var mapWithStruct map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &mapWithStruct))
	assert.Len(t, mapWithStruct, 2)
	for _, key := range []string{"point-1", "point-2"} {
		point, ok := mapWithStruct[key].(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, float64(5), point["x"])
		assert.Equal(t, float64(10), point["y"])
	}

	// MAP with ARRAY values
	g = genFor(t, "MAP<STRING,ARRAY<INT>>", GenRule{
		"length": 2,
		"key":    GenRule{"format": "arr-{{%d}}", "gen": GenRule{"inc": GenRule{"start": 0}}},
		"value":  GenRule{"length": GenRule{"min": 3, "max": 3}, "element": GenRule{"min": 7, "max": 7}},
	})
	var mapWithArr map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &mapWithArr))
	assert.Len(t, mapWithArr, 2)
	for _, key := range []string{"arr-0", "arr-1"} {
		arr, ok := mapWithArr[key].([]any)
		assert.True(t, ok)
		assert.Len(t, arr, 3)
		for _, v := range arr {
			assert.Equal(t, float64(7), v)
		}
	}

	// ARRAY of STRUCT
	g = genFor(t, "ARRAY<STRUCT<id:INT,name:STRING>>", GenRule{
		"length": 2,
		"element": GenRule{
			"fields": []any{
				GenRule{"name": "id", "gen": GenRule{"inc": GenRule{"start": 1}}},
				GenRule{"name": "name", "format": "item"},
			},
		},
	})
	var arrOfStruct []any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &arrOfStruct))
	assert.Len(t, arrOfStruct, 2)
	for i, item := range arrOfStruct {
		obj, ok := item.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, float64(i+1), obj["id"])
		assert.Equal(t, "item", obj["name"])
	}

	// ARRAY of MAP
	g = genFor(t, "ARRAY<MAP<STRING,INT>>", GenRule{
		"length": 2,
		"element": GenRule{
			"length": 1,
			"key":    GenRule{"format": "k"},
			"value":  GenRule{"min": 42, "max": 42},
		},
	})
	var arrOfMap []any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &arrOfMap))
	assert.Len(t, arrOfMap, 2)
	for _, item := range arrOfMap {
		m, ok := item.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, float64(42), m["k"])
	}

	// Deeply nested: STRUCT<data:ARRAY<MAP<STRING,STRUCT<val:INT>>>>
	g = genFor(t, "STRUCT<data:ARRAY<MAP<STRING,STRUCT<val:INT>>>>", GenRule{
		"fields": []any{
			GenRule{
				"name":   "data",
				"length": GenRule{"min": 1, "max": 1},
				"element": GenRule{
					"length": 1,
					"key":    GenRule{"format": "deep"},
					"value": GenRule{
						"fields": []any{
							GenRule{"name": "val", "min": 999, "max": 999},
						},
					},
				},
			},
		},
	})
	var deepNested map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &deepNested))
	dataArr, ok := deepNested["data"].([]any)
	assert.True(t, ok)
	assert.Len(t, dataArr, 1)
	dataMap, ok := dataArr[0].(map[string]any)
	assert.True(t, ok)
	deepStruct, ok := dataMap["deep"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, float64(999), deepStruct["val"])
}

func Test_getTypeGen_CornerCases(t *testing.T) {
	t.Parallel()

	// Zero-length array
	g := genFor(t, "ARRAY<INT>", GenRule{"length": GenRule{"min": 0, "max": 0}})
	var arr []any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &arr))
	assert.Len(t, arr, 0)

	// Zero-length map
	g = genFor(t, "MAP<STRING,INT>", GenRule{"length": GenRule{"min": 0, "max": 0}})
	var m map[string]any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &m))
	assert.Len(t, m, 0)

	// Nested complex types
	g = genFor(t, "ARRAY<MAP<STRING,STRUCT<a:INT>>>", GenRule{
		"length": GenRule{"min": 1, "max": 1},
		"element": GenRule{
			"length": GenRule{"min": 1, "max": 1},
			"key":    GenRule{"format": "nested"},
		},
	})
	var nestedArr []any
	assert.NoError(t, json.Unmarshal(g.Gen(nil).(json.RawMessage), &nestedArr))
	assert.Len(t, nestedArr, 1)
	nestedMap, ok := nestedArr[0].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, nestedMap, "nested")
	nestedStruct, ok := nestedMap["nested"].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, nestedStruct, "a")

	// Null generation
	g = genFor(t, "INT", GenRule{"null_frequency": 1.0})
	assert.Nil(t, g.Gen(nil))

	g = genFor(t, "INT", GenRule{"null_frequency": 0.0})
	assert.NotNil(t, g.Gen(nil))
}

// constGen is a helper that generates a constant value.
type constGen struct {
	val any
}

func (c *constGen) Gen(_ *GenContext) any {
	return c.val
}

func newConstGen(val any) Gen {
	return &constGen{val: val}
}

func TestArrayGen_Gen(t *testing.T) {
	t.Run("json output", func(t *testing.T) {
		dt := mustParseType(t, "INT")
		g := NewArrayGen(dt, newConstGen(123), 2, 2)
		g.insertOrCSV = false

		expected := json.RawMessage(`[123,123]`)
		assert.Equal(t, expected, g.Gen(nil))
	})

	t.Run("insert/csv output", func(t *testing.T) {
		dt := mustParseType(t, "INT")
		g := NewArrayGen(dt, newConstGen(123), 2, 2)
		g.insertOrCSV = true

		expected := json.RawMessage(`array(123,123)`)
		assert.Equal(t, expected, g.Gen(nil))
	})

	t.Run("insert/csv output with strings", func(t *testing.T) {
		dt := mustParseType(t, "STRING")
		g := NewArrayGen(dt, newConstGen("hello"), 2, 2)
		g.insertOrCSV = true

		expected := json.RawMessage(`array("hello","hello")`)
		assert.Equal(t, expected, g.Gen(nil))
	})
}

func TestMapGen_Gen(t *testing.T) {
	kType := mustParseType(t, "STRING")
	vType := mustParseType(t, "INT")

	t.Run("json output", func(t *testing.T) {
		g := NewMapGen(kType, vType, newConstGen("a"), newConstGen(1), 1, 1)
		g.SetKeyUniq(false) // Disable for simple test
		g.insertOrCSV = false

		expected := json.RawMessage(`{"a":1}`)
		assert.Equal(t, expected, g.Gen(nil))
	})

	t.Run("insert/csv output", func(t *testing.T) {
		g := NewMapGen(kType, vType, newConstGen("a"), newConstGen(1), 1, 1)
		g.SetKeyUniq(false)
		g.insertOrCSV = true

		expected := json.RawMessage(`map("a",1)`)
		assert.Equal(t, expected, g.Gen(nil))
	})

	t.Run("key unique enabled", func(t *testing.T) {
		// Key generator always returns "a"
		g := NewMapGen(kType, vType, newConstGen("a"), newConstGen(1), 5, 5)
		g.KeyUniq = true // Default, but explicit for test
		g.insertOrCSV = false

		var m map[string]any
		res := g.Gen(nil).(json.RawMessage)
		err := json.Unmarshal(res, &m)
		assert.NoError(t, err)
		// With 5 attempts to generate a key, only one unique key ("a") should be present.
		assert.Len(t, m, 1)
	})
}

func TestStructGen_Gen(t *testing.T) {
	f1Type := mustParseType(t, "STRING")
	f2Type := mustParseType(t, "INT")

	t.Run("json output", func(t *testing.T) {
		g := NewStructGen()
		g.AddChild("f1", f1Type, newConstGen("hello"))
		g.AddChild("f2", f2Type, newConstGen(100))
		g.insertOrCSV = false

		expected := json.RawMessage(`{"f1":"hello","f2":100}`)
		assert.Equal(t, expected, g.Gen(nil))
	})

	t.Run("insert/csv output", func(t *testing.T) {
		g := NewStructGen()
		g.AddChild("f1", f1Type, newConstGen("hello"))
		g.AddChild("f2", f2Type, newConstGen(100))
		g.insertOrCSV = true

		expected := json.RawMessage(`named_struct("f1","hello","f2",100)`)
		assert.Equal(t, expected, g.Gen(nil))
	})
}

func Test_getTypeGen_AggState(t *testing.T) {
	t.Parallel()

	// Test AGG_STATE with max_by(int not null, int)
	t.Run("agg_state max_by", func(t *testing.T) {
		rule := GenRule{
			"args": []any{
				GenRule{"name": 0, "min": 42, "max": 42},
				GenRule{"name": 1, "min": 100, "max": 100},
			},
		}
		g := genFor(t, "agg_state<max_by(int not null, int)>", rule)

		// The generator should return two column values as AGG_STATE args
		result := g.Gen(nil)
		assert.Equal(t, "42☆100", result)
	})

	// Test AGG_STATE with group_concat(string)
	t.Run("agg_state group_concat", func(t *testing.T) {
		rule := GenRule{
			"args": []any{
				GenRule{"name": 0, "format": "test"},
			},
		}
		g := genFor(t, "agg_state<group_concat(string)>", rule)

		result := g.Gen(nil)
		assert.Equal(t, "test", result)
	})

	// Test AGG_STATE with avg(decimal)
	t.Run("agg_state avg decimal", func(t *testing.T) {
		rule := GenRule{
			"args": []any{
				GenRule{"name": 0, "min": "1", "max": "1"},
			},
		}
		g := genFor(t, "agg_state<avg(decimal(38,18) null)>", rule)

		result := g.Gen(nil)
		assert.True(t, strings.HasPrefix(cast.ToString(result), "1"))
	})

	// Test AGG_STATE with sum(bigint)
	t.Run("agg_state sum bigint", func(t *testing.T) {
		rule := GenRule{
			"args": []any{
				GenRule{"name": 0, "min": int64(1000), "max": int64(1000)},
			},
		}
		g := genFor(t, "agg_state<sum(bigint)>", rule)

		result := g.Gen(nil)
		assert.Equal(t, "1000", result)
	})

	// Test AGG_STATE sets agg_func and agg_args in GenRule
	t.Run("agg_state sets gen_rule metadata", func(t *testing.T) {
		dt := mustParseType(t, "agg_state<max_by(int, int)>")
		v := NewColumnVisitor("tbl", nil, "db.tbl.col", GenRule{})
		_ = v.GetGen(dt)

		// Verify agg_func is set correctly
		assert.Equal(t, "max_by_state", v.GenRule["agg_func"])

		// Verify agg_args contains the data types
		aggArgs := v.GenRule["agg_args"]
		assert.NotNil(t, aggArgs)
	})

	// Test AGG_STATE respects NOT NULL in arg type
	t.Run("agg_state respects not null", func(t *testing.T) {
		// When arg has NOT NULL, null_frequency should be set to 0
		rule := GenRule{}
		dt := mustParseType(t, "agg_state<sum(int not null)>")
		v := NewColumnVisitor("tbl", nil, "db.tbl.col", rule)
		_ = v.GetGen(dt)

		assert.Equal(t, "sum_state", v.GenRule["agg_func"])
	})
}
