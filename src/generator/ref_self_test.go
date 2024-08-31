package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefSelfGenerator(t *testing.T) {
	columns := []string{"id", "name", "manager_id"}

	t.Run("valid self ref", func(t *testing.T) {
		v := NewColumnVisitor("employee", columns, "employee.manager_id", nil)
		rule := GenRule{"ref": "id"}

		gen, err := NewRefSelfGenerator(v, nil, rule)
		assert.NoError(t, err)
		assert.NotNil(t, gen)

		refSelfGen, ok := gen.(*RefSelfGen)
		assert.True(t, ok)
		assert.Equal(t, "id", refSelfGen.SourceColumn)
		assert.Equal(t, 0, refSelfGen.SourceColumnIdx)
		assert.Contains(t, *v.SelfColRefs, "id")

		// Test Gen
		ctx := NewGenContext([]any{123, "Alice", nil})
		val := gen.Gen(ctx)
		assert.Equal(t, 123, val)
	})

	t.Run("invalid self ref", func(t *testing.T) {
		v := NewColumnVisitor("employee", columns, "employee.manager_id", nil)
		rule := GenRule{"ref": "non_existent_column"}

		gen, err := NewRefSelfGenerator(v, nil, rule)
		assert.Error(t, err)
		assert.Nil(t, gen)
		assert.Contains(t, err.Error(), "not found the column ref to in same table")
	})

	t.Run("multiple self refs", func(t *testing.T) {
		v := NewColumnVisitor("employee", columns, "employee.manager_id", nil)

		rule1 := GenRule{"ref": "id"}
		_, err := NewRefSelfGenerator(v, nil, rule1)
		assert.NoError(t, err)

		rule2 := GenRule{"ref": "name"}
		_, err = NewRefSelfGenerator(v, nil, rule2)
		assert.NoError(t, err)

		assert.ElementsMatch(t, []string{"id", "name"}, *v.SelfColRefs)
	})
}
