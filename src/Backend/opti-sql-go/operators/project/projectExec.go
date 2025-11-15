package project

import (
	"errors"

	"github.com/apache/arrow/go/v17/arrow"
)

// handle keeping only the request columsn but make sure the schema and columns are also aligned
// returns error if a column doesnt exist
func ProjectSchemaFilterDown(schema *arrow.Schema, cols []arrow.Array, keepCols ...string) (*arrow.Schema, []arrow.Array, error) {
	if len(keepCols) == 0 {
		return arrow.NewSchema([]arrow.Field{}, nil), nil, errors.New("no columns passed in")
	}

	// Build map: columnName -> original index
	fieldIndex := make(map[string]int)
	for i, f := range schema.Fields() {
		fieldIndex[f.Name] = i
	}

	newFields := make([]arrow.Field, 0, len(keepCols))
	newCols := make([]arrow.Array, 0, len(keepCols))

	// Preserve order from keepCols, not schema order
	for _, name := range keepCols {
		idx, exists := fieldIndex[name]
		if !exists {
			return arrow.NewSchema([]arrow.Field{}, nil), []arrow.Array{}, errors.New("invalid column passed in to be pruned")
		}

		newFields = append(newFields, schema.Field(idx))
		newCols = append(newCols, cols[idx])
	}

	newSchema := arrow.NewSchema(newFields, nil)
	return newSchema, newCols, nil
}
