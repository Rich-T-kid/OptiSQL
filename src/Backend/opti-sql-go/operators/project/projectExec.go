package project

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
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
	input        operators.Operator
	outputschema arrow.Schema
	expr         []Expr.Expression
	done         bool
}

// columns to keep and existing schema
func NewProjectExec(input operators.Operator, exprs []Expr.Expression) (*ProjectExec, error) {
	fields := make([]arrow.Field, len(exprs))
	for i, e := range exprs {
		switch ex := e.(type) {
		case *Expr.Alias:
			tp, err := Expr.ExprDataType(ex.Expr, input.Schema())
			if err != nil {
				return nil, fmt.Errorf("project exec: failed to get expression data type for expr %d: %w", i, err)
			}
			fields[i] = arrow.Field{
				Name:     ex.Name,
				Type:     tp,
				Nullable: true,
			}
		default:
			name := fmt.Sprintf("col_%d", i)
			Type, err := Expr.ExprDataType(e, input.Schema())
			if err != nil {
				return nil, fmt.Errorf("project exec: failed to get expression data type for expr %d: %w", i, err)
			}
			fields[i] = arrow.Field{
				Name:     name,
				Type:     Type,
				Nullable: true,
			}
		}
	}
	// Use a generic column naming pattern ("col_%d") when an expression doesn't have an explicit alias.
	// This ensures every projected column has a name in the output schema.

	outputschema := arrow.NewSchema(fields, nil)
	// return new exec
	return &ProjectExec{
		input:        input,
		outputschema: *outputschema,
		expr:         exprs,
	}, nil
}

// pretty simple, read from child operator and prune columns
// pass through error && handles EOF alike
func (p *ProjectExec) Next(n uint16) (*operators.RecordBatch, error) {
	if p.done {
		return nil, io.EOF
	}

	childBatch, err := p.input.Next(n)
	if err != nil {
		return nil, err
	}
	if childBatch.RowCount == 0 {
		p.done = true
		return &operators.RecordBatch{
			Schema:   &p.outputschema,
			Columns:  []arrow.Array{},
			RowCount: 0,
		}, nil
	}
	outPutCols := make([]arrow.Array, len(p.expr))
	for i, e := range p.expr {
		arr, err := Expr.EvalExpression(e, childBatch)
		if err != nil {
			return nil, fmt.Errorf("project eval expression failed for expr %d: %w", i, err)
		}
		outPutCols[i] = arr
		arr.Retain()
	}
	operators.ReleaseArrays(childBatch.Columns)
	return &operators.RecordBatch{
		Schema:   &p.outputschema,
		Columns:  outPutCols,
		RowCount: childBatch.RowCount,
	}, nil
}
func (p *ProjectExec) Close() error {
	return p.input.Close()
}
func (p *ProjectExec) Schema() *arrow.Schema {
	return &p.outputschema
}

// handle keeping only the request columns but make sure the schema and columns are also aligned
// returns error if a column doesnt exist

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
