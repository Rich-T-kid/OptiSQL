package project

import (
	"errors"
	"io"
	"opti-sql-go/operators"

	"github.com/apache/arrow/go/v17/arrow"
)

var (
	_ = (operators.Operator)(&ProjectExec{})
)

var (
	ErrProjectColumnNotFound = errors.New("project: column not found")
	ErrEmptyColumnsToProject = errors.New("project: no columns to project")
)

type ProjectExec struct {
	child         operators.Operator
	outputschema  arrow.Schema
	columnsToKeep []string
	done          bool
}

// columns to keep and existing schema
func NewProjectExec(projectColumns []string, input operators.Operator) (*ProjectExec, error) {
	newSchema, err := prunedSchema(input.Schema(), projectColumns)
	if err != nil {
		return nil, err
	}
	// return new exec
	return &ProjectExec{
		child:         input,
		outputschema:  *newSchema,
		columnsToKeep: projectColumns,
	}, nil
}

// pretty simple, read from child operator and prune columns
// pass through error && handles EOF alike
func (p *ProjectExec) Next(n uint16) (*operators.RecordBatch, error) {
	if p.done {
		return nil, io.EOF
	}

	rc, err := p.child.Next(n)
	if err != nil {
		return nil, err
	}
	_, orderCols, err := ProjectSchemaFilterDown(rc.Schema, rc.Columns, p.columnsToKeep...)
	if err != nil {
		return nil, err
	}
	for _, c := range rc.Columns {
		c.Release()
	}
	if rc.RowCount == 0 {
		p.done = true
	}
	return &operators.RecordBatch{
		Schema:   &p.outputschema,
		Columns:  orderCols,
		RowCount: rc.RowCount,
	}, nil
}
func (p *ProjectExec) Close() error {
	return nil
}
func (p *ProjectExec) Schema() *arrow.Schema {
	return &p.outputschema
}

// handle keeping only the request columns but make sure the schema and columns are also aligned
// returns error if a column doesnt exist
func ProjectSchemaFilterDown(schema *arrow.Schema, cols []arrow.Array, keepCols ...string) (*arrow.Schema, []arrow.Array, error) {
	if len(keepCols) == 0 {
		return arrow.NewSchema([]arrow.Field{}, nil), nil, ErrEmptyColumnsToProject
	}

	// Build map: columnName -> original index
	fieldIndex := make(map[string]int) // age -> 0
	for i, f := range schema.Fields() {
		fieldIndex[f.Name] = i
	}

	newFields := make([]arrow.Field, 0, len(keepCols))
	newCols := make([]arrow.Array, 0, len(keepCols))

	// Preserve order from keepCols, not schema order
	for _, name := range keepCols {
		idx, exists := fieldIndex[name]
		if !exists {
			return arrow.NewSchema([]arrow.Field{}, nil), []arrow.Array{}, ErrProjectColumnNotFound
		}

		newFields = append(newFields, schema.Field(idx))
		col := cols[idx]
		col.Retain()
		newCols = append(newCols, col)
	}

	newSchema := arrow.NewSchema(newFields, nil)
	return newSchema, newCols, nil
}

func prunedSchema(schema *arrow.Schema, keepCols []string) (*arrow.Schema, error) {
	if len(keepCols) == 0 {
		return arrow.NewSchema([]arrow.Field{}, nil), ErrEmptyColumnsToProject
	}
	newFields := make([]arrow.Field, 0)
	for _, colName := range keepCols {
		idx := schema.FieldIndices(colName)
		if len(idx) == 0 {
			return nil, ErrProjectColumnNotFound
		}
		// append the field
		newFields = append(newFields, schema.Field(idx[0]))
	}
	newSchema := arrow.NewSchema(newFields, nil)
	return newSchema, nil
}
