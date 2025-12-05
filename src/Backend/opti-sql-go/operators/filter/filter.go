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
	"github.com/apache/arrow/go/v17/arrow/memory"
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
	//
	bufferedCols []arrow.Array // not yet returned
	bufferedSize int64
}

func NewFilterExec(input operators.Operator, pred Expr.Expression) (*FilterExec, error) {
	if !validPredicates(pred, input.Schema()) {
		return nil, errors.New("predicates passed to FilterExec are invalid")
	}
	return &FilterExec{
		input:        input,
		predicate:    pred,
		schema:       input.Schema(),
		bufferedCols: make([]arrow.Array, input.Schema().NumFields()),
	}, nil
}
func (f *FilterExec) Next(n uint16) (*operators.RecordBatch, error) {
	if f.done && f.bufferedSize == 0 {
		return nil, io.EOF
	}
	mem := memory.NewGoAllocator()
	for f.bufferedSize < int64(n) && !f.done {
		childBatch, err := f.input.Next(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				f.done = true
				break // might be some in the buffer still
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
		// combine with buffered columns
		for i, col := range f.bufferedCols {
			if col == nil {
				f.bufferedCols[i] = filteredCol[i]
				continue
			}
			// otherwise concate old + new
			combined, err := array.Concatenate([]arrow.Array{col, filteredCol[i]}, mem)
			if err != nil {
				return nil, err
			}

			// Release old buffer column
			col.Release()

			f.bufferedCols[i] = combined
		}
		if len(childBatch.Columns) > 0 {
			size := int64(filteredCol[0].Len())
			f.bufferedSize += int64(size)
		}
	}
	if f.bufferedSize == 0 {
		return nil, io.EOF
	}
	toEmit := min(int64(n), f.bufferedSize)
	out, err := f.sliceFilterCols(toEmit, mem)
	if err != nil {
		return nil, err
	}
	// subtract emitted rows from buffer; guard against accidental negative values

	size := uint64(out[0].Len())

	rc := &operators.RecordBatch{
		Schema:   f.schema,
		Columns:  out,
		RowCount: size,
	}
	return rc, nil
}

func (f *FilterExec) Schema() *arrow.Schema {
	return f.schema
}

func (f *FilterExec) Close() error {
	return f.input.Close()
}

func ApplyBooleanMask(col arrow.Array, mask *array.Boolean) (arrow.Array, error) {
	datum, err := compute.Filter(
		context.Background(),
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
		fmt.Printf("dt1:\t%v\ndt2:\t%v\n", dt1, dt2)
		if !arrow.TypeEqual(dt1, dt2) {
			return false
		}
		fmt.Printf("left:\t%v\nright:\t%v\n", p.Left, p.Right)
		return validPredicates(p.Left, schema) &&
			validPredicates(p.Right, schema)

	case *Expr.LiteralResolve:
		return true

	case *Expr.NullCheckExpr:
		return validPredicates(p.Expr, schema)
	case *Expr.ScalarFunction:
		return true
	default:
		return false
	}
}

func (f *FilterExec) sliceFilterCols(n int64, mem memory.Allocator) ([]arrow.Array, error) {
	out := make([]arrow.Array, len(f.bufferedCols))

	// Build index arrays for:
	// 1) rows to emit: 0 .. n-1
	// 2) rows to keep: n .. f.bufferedSize-1
	emitIdx := array.NewInt64Builder(mem)
	keepIdx := array.NewInt64Builder(mem)

	total := f.bufferedSize
	limit := n
	if limit > total {
		limit = total
	}

	// emit rows [0 , limit)
	for i := int64(0); i < limit; i++ {
		emitIdx.Append(i)
	}

	// keep rows [limit , total)
	for i := limit; i < total; i++ {
		keepIdx.Append(i)
	}

	emitArr := emitIdx.NewArray()
	keepArr := keepIdx.NewArray()
	emitIdx.Release()
	keepIdx.Release()
	defer emitArr.Release()
	defer keepArr.Release()

	// For each column: materialize output slice + update buffer
	ctx := context.Background()
	for i, col := range f.bufferedCols {
		// emit slice
		sliceOut, err := compute.TakeArray(ctx, col, emitArr)
		if err != nil {
			return nil, err
		}
		out[i] = sliceOut

		// keep remaining slice
		keepSlice, err := compute.TakeArray(ctx, col, keepArr)
		if err != nil {
			return nil, err
		}

		// release old buffer column
		col.Release()

		// store updated buffer
		f.bufferedCols[i] = keepSlice
	}

	// update size
	f.bufferedSize = total - limit

	return out, nil
}
