package filter

import (
	"context"
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
)

var (
	_ = (operators.Operator)(&FilterExec{})
)

// FilterExpr takes in a field and column and yeilds a function that takes in an index and returns a bool indicating whether the row at that index satisfies the filter condition.
type FilterExec struct {
	input     operators.Operator
	schema    *arrow.Schema
	predicate Expr.Expression
	done      bool
}

func NewFilterExec(input operators.Operator, pred Expr.Expression) (*FilterExec, error) {
	if !validPredicates(pred, input.Schema()) {
		return nil, errors.New("predicates passed to FilterExec are invalid")
	}
	return &FilterExec{
		input:     input,
		predicate: pred,
		schema:    input.Schema(),
	}, nil
}
func (f *FilterExec) Next(n uint16) (*operators.RecordBatch, error) {
	if n == 0 {
		return nil, errors.New("must pass in wanted batch size > 0")
	}
	if f.done {
		return nil, io.EOF
	}
	batch, err := f.input.Next(n)
	if err != nil {
		return nil, err
	}
	booleanMask, err := Expr.EvalExpression(f.predicate, batch)
	if err != nil {
		return nil, err
	}
	boolArr := booleanMask.(*array.Boolean) // impossible for this to not be a boolean array,assuming validPredicates works as it should
	filteredCol := make([]arrow.Array, len(batch.Columns))
	for i, col := range batch.Columns {
		filteredCol[i], err = applyBooleanMask(col, boolArr)
		if err != nil {
			return nil, err
		}
	}
	// release old columns
	for _, c := range batch.Columns {
		c.Release()
	}
	size := uint64(filteredCol[0].Len())

	return &operators.RecordBatch{
		Schema:   batch.Schema,
		Columns:  filteredCol,
		RowCount: size,
	}, nil
}
func (f *FilterExec) Schema() *arrow.Schema {
	return f.schema
}

// TODO: check if this pattern is good
func (f *FilterExec) Close() error {
	return f.input.Close()
}

func applyBooleanMask(col arrow.Array, mask *array.Boolean) (arrow.Array, error) {
	datum, err := compute.Filter(
		context.TODO(),
		compute.NewDatum(col),
		compute.NewDatum(mask),
		*compute.DefaultFilterOptions(),
	)
	if err != nil {
		return nil, err
	}

	arr := datum.(*compute.ArrayDatum).MakeArray()
	return arr, nil
}
func validPredicates(pred Expr.Expression, schema *arrow.Schema) bool {
	switch p := pred.(type) {
	case *Expr.ColumnResolve:
		idx := schema.FieldIndices(p.Name)
		if len(idx) == 0 {
			return false
		}
		return true

	case *Expr.BinaryExpr:
		// Check valid operator
		// these return boolean arrays
		switch p.Op {
		case Expr.Equal, Expr.NotEqual,
			Expr.GreaterThan, Expr.GreaterThanOrEqual,
			Expr.LessThan, Expr.LessThanOrEqual,
			Expr.And, Expr.Or:
			// supported
		default:
			return false
		}
		dt1, err := Expr.ExprDataType(p.Left, schema)
		if err != nil {
			return false
		}
		dt2, err := Expr.ExprDataType(p.Right, schema)
		if err != nil {
			return false
		}
		if !arrow.TypeEqual(dt1, dt2) {
			return false
		}
		// recursively validate children
		return validPredicates(p.Left, schema) &&
			validPredicates(p.Right, schema)

	case *Expr.LiteralResolve:
		return true

	default:
		return false
	}
}
