package filter

import (
	"context"
	"errors"
	"fmt"
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

// FilterExec is an operator that filters input records according to a predicate expression.
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
	childBatch, err := f.input.Next(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			f.done = true
			return nil, io.EOF
		}
		return nil, err
	}
	booleanMask, err := Expr.EvalExpression(f.predicate, childBatch)
	if err != nil {
		return nil, err
	}
	boolArr, ok := booleanMask.(*array.Boolean) // impossible for this to not be a boolean array,assuming validPredicates works as it should
	if !ok {
		return nil, errors.New("predicate did not evaluate to boolean array")
	}
	filteredCol := make([]arrow.Array, len(childBatch.Columns))
	for i, col := range childBatch.Columns {
		filteredCol[i], err = ApplyBooleanMask(col, boolArr)
		if err != nil {
			return nil, err
		}
	}
	booleanMask.Release()
	// release old columns
	operators.ReleaseArrays(childBatch.Columns)
	size := uint64(filteredCol[0].Len())

	return &operators.RecordBatch{
		Schema:   childBatch.Schema,
		Columns:  filteredCol,
		RowCount: size,
	}, nil
}
func (f *FilterExec) Schema() *arrow.Schema {
	return f.schema
}

func (f *FilterExec) Close() error {
	return f.input.Close()
}

func ApplyBooleanMask(col arrow.Array, mask *array.Boolean) (arrow.Array, error) {
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
		//TODO: allow for nulls to be comparable
		fmt.Printf("dt1:\t%v\ndt2:\t%v\n", dt1, dt2)
		if !arrow.TypeEqual(dt1, dt2) {
			return false
		}
		// recursively validate children
		return validPredicates(p.Left, schema) &&
			validPredicates(p.Right, schema)

	case *Expr.LiteralResolve:
		return true

	case *Expr.NullCheckExpr:
		return validPredicates(p.Expr, schema)
	default:
		return false
	}
}
