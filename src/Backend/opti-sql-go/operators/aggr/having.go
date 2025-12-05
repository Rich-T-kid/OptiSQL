package aggr

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"opti-sql-go/operators/filter"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// carbon copy of filter.go with minor changes to fit having semantics
var (
	_ = (operators.Operator)(&HavingExec{})
)

type HavingExec struct {
	input  operators.Operator
	schema *arrow.Schema

	havingExpr Expr.Expression
	done       bool
}

func NewHavingExec(input operators.Operator, havingFilter Expr.Expression) (*HavingExec, error) {

	return &HavingExec{
		input:      input,
		schema:     input.Schema(),
		havingExpr: havingFilter,
	}, nil
}

func (h *HavingExec) Next(n uint16) (*operators.RecordBatch, error) {
	if h.done {
		return nil, io.EOF
	}
	childBatch, err := h.input.Next(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			h.done = true
		}
		return nil, err
	}
	booleanMask, err := Expr.EvalExpression(h.havingExpr, childBatch)
	if err != nil {
		return nil, err
	}
	boolArr, ok := booleanMask.(*array.Boolean) // impossible for this to not be a boolean array,assuming validPredicates works as it should
	if !ok {
		return nil, errors.New("having predicate did not evaluate to boolean array")
	}
	filteredCol := make([]arrow.Array, len(childBatch.Columns))
	for i, col := range childBatch.Columns {
		filteredCol[i], err = filter.ApplyBooleanMask(col, boolArr)
		if err != nil {
			return nil, err
		}
	}
	// release old columns
	operators.ReleaseArrays(childBatch.Columns)
	size := uint64(filteredCol[0].Len())

	return &operators.RecordBatch{
		Schema:   childBatch.Schema,
		Columns:  filteredCol,
		RowCount: size,
	}, nil
}
func (h *HavingExec) Schema() *arrow.Schema {
	return h.schema
}
func (h *HavingExec) Close() error {
	return h.input.Close()
}
