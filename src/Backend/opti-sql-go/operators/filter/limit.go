package filter

import (
	"io"
	"opti-sql-go/operators"

	"github.com/apache/arrow/go/v17/arrow"
)

var (
	_ = (operators.Operator)(&LimitExec{})
)

type LimitExec struct {
	input     operators.Operator
	schema    *arrow.Schema
	remaining uint16
	done      bool
}

func NewLimitExec(input operators.Operator, count uint16) (*LimitExec, error) {
	return &LimitExec{
		input:     input,
		schema:    input.Schema(),
		remaining: count,
	}, nil
}

func (l *LimitExec) Next(n uint16) (*operators.RecordBatch, error) {
	if n == 0 {
		return &operators.RecordBatch{
			Schema:   l.schema,
			Columns:  []arrow.Array{},
			RowCount: 0,
		}, nil
	}
	if l.remaining == 0 {
		return nil, io.EOF
	}
	var childN uint16
	switch {
	case n < l.remaining:
		// We can fulfill the request fully
		childN = n
		l.remaining -= n

	case n == l.remaining:
		// Exact request - done afterwards
		childN = n
		l.remaining = 0
		l.done = true

	case n > l.remaining:
		// Only have l.remaining left
		childN = l.remaining
		l.remaining = 0
		l.done = true
	}
	childBatch, err := l.input.Next(childN)
	if err != nil {
		return nil, err
	}
	return childBatch, nil
}
func (l *LimitExec) Schema() *arrow.Schema {
	return l.schema
}

func (l *LimitExec) Close() error {
	return l.input.Close()
}

/*
type Distinct struct {
	child      operators.Operator
	schema     *arrow.Schema
	colExpr    Expr.Expression     // resolves to column that we want distinct values of
	seenValues map[string]struct{} // arrow.Array.value(i) (stored and compared as string , structs occupie no space
	done       bool
}
*/
