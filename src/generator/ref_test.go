package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func resetRefState() {
	sourceTableDataMapLock.Lock()
	sourceTableDataMap = map[string]*SourceTableData{}
	sourceTableDataMapLock.Unlock()

	rowRefGroupMapLock.Lock()
	rowRefGroupMap = map[string]map[string]*RowRefGroup{}
	rowRefGroupMapLock.Unlock()
}

func TestRefGenerator(t *testing.T) {
	resetRefState()

	// Create refs from dest table "test" to source table "t1"
	tests := []struct {
		name    string
		rule    GenRule
		wantErr bool
	}{
		{"test.c1", GenRule{"ref": "t1.col1", "limit": 10}, false},
		{"test.c2", GenRule{"ref": "t1.other_col", "limit": 10}, false},
		{"test.c3", GenRule{"ref": "t1.col1", "limit": 20}, false},
		{"test.c4", GenRule{"ref": "t1.col1", "limit": 100}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRefGenerator(NewColumnVisitor("test", nil, tt.name, nil), nil, tt.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRefGenerator() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Verify SourceTableData was created
	sourceData := GetSourceTableData("t1")
	assert.NotNil(t, sourceData)
	assert.Equal(t, "t1", sourceData.Table)
	assert.Equal(t, []string{"col1", "other_col"}, sourceData.Columns)
	assert.Equal(t, 100, sourceData.Limit) // max of all limits

	// Verify RowRefGroup was created
	groups := getRowRefGroups("t1")
	assert.Len(t, groups, 1)
	grp := groups[0]
	assert.Equal(t, "test", grp.DestTable)
	assert.Equal(t, "t1", grp.SourceTable)
	assert.Equal(t, []string{"col1", "other_col"}, grp.refColumns)

	// Add test rows to SourceTableData
	for i := range 100 {
		sourceData.AddRow([]any{i, i + 100})
	}

	// Test that generation works
	visitor := NewColumnVisitor("test", nil, "test.x", nil)
	gen1, _ := NewRefGenerator(visitor, nil, GenRule{"ref": "t1.col1"})
	for range 100 {
		c := &GenContext{
			RefSourceTableRowIdx: map[string]int{"t1": grp.SelectOneRow()},
		}
		v := gen1.Gen(c).(int)
		assert.GreaterOrEqual(t, v, 0)
		assert.Less(t, v, 100)
	}
}

func TestRowRefGenerator(t *testing.T) {
	resetRefState()

	// Simulate: t2.c1 refs t1.c1, t2.c2 refs t1.c2
	// Both should automatically get values from the same row of t1
	gen1, err := NewRefGenerator(NewColumnVisitor("t2", nil, "t2.c1", nil), nil, GenRule{"ref": "t1.c1", "limit": 2})
	assert.NoError(t, err)
	refGen1 := gen1.(*RefGen)
	assert.Equal(t, "t1", refGen1.Table)
	assert.Equal(t, "c1", refGen1.Column)
	assert.Equal(t, 0, refGen1.sourceColIdx)

	gen2, err := NewRefGenerator(NewColumnVisitor("t2", nil, "t2.c2", nil), nil, GenRule{"ref": "t1.c2", "limit": 100})
	assert.NoError(t, err)
	refGen2 := gen2.(*RefGen)
	assert.Equal(t, "t1", refGen2.Table)
	assert.Equal(t, "c2", refGen2.Column)
	assert.Equal(t, 1, refGen2.sourceColIdx)

	// Both should share the same RowRefGroup
	assert.Equal(t, refGen1.group, refGen2.group)

	// Get SourceTableData and add test rows
	sourceData := GetSourceTableData("t1")
	assert.NotNil(t, sourceData)
	assert.Equal(t, []string{"c1", "c2"}, sourceData.Columns)

	// Add test rows: each row has [c1_val, c2_val]
	sourceData.AddRow([]any{"a1", "b1"})
	sourceData.AddRow([]any{"a2", "b2"})
	sourceData.AddRow([]any{"a3", "b3"})
	sourceData.AddRow([]any{"a4", "b4"})
	sourceData.AddRow([]any{"a5", "b5"})

	// Get RowRefGroup for dest table t2
	grp := refGen1.group

	// Generate values - both columns should come from the same row
	for range 10 {
		c := &GenContext{
			RefSourceTableRowIdx: map[string]int{"t1": grp.SelectOneRow()},
		}
		v1 := refGen1.Gen(c).(string)
		v2 := refGen2.Gen(c).(string)
		// v1 should be "aX" and v2 should be "bX" where X is the same number
		assert.Equal(t, v1[1:], v2[1:], "c1 and c2 should come from the same row, got c1=%s, c2=%s", v1, v2)
	}
}

func TestRowRefGeneratorLimitWarning(t *testing.T) {
	resetRefState()

	// First column with limit 50
	gen1, err := NewRefGenerator(NewColumnVisitor("t3", nil, "t3.c1", nil), nil, GenRule{"ref": "t1.c1", "limit": 50})
	assert.NoError(t, err)

	// Second column with different limit 100 - should use max
	gen2, err := NewRefGenerator(NewColumnVisitor("t3", nil, "t3.c2", nil), nil, GenRule{"ref": "t1.c2", "limit": 100})
	assert.NoError(t, err)

	// Both should share the same SourceTableData with max limit
	refGen1, refGen2 := gen1.(*RefGen), gen2.(*RefGen)
	sourceData := refGen1.group.sourceData
	assert.Equal(t, 100, sourceData.Limit)
	assert.Equal(t, sourceData, refGen2.group.sourceData)
}
