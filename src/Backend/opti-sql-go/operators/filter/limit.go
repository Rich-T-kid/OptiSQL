package filter

import (
	"context"
	"errors"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var (
	_ = (operators.Operator)(&LimitExec{})
	_ = (operators.Operator)(&Distinct{})
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

type Distinct struct {
	input               operators.Operator
	schema              *arrow.Schema
	colExpr             []Expr.Expression   // resolves to column that we want distinct values of
	seenValues          map[string]struct{} // arrow.Array.value(i) (stored and compared as string , structs occupie no space
	distinctValuesArray []arrow.Array       // hold arrays of distinct values
	consumedOffset      uint64              // where did we leave off at when returning the distinct arrays to the caller
	consumedInput       bool                // did we consume all the input record batches?
	totalRows           uint64
	done                bool
}

func NewDistinctExec(input operators.Operator, colExpr []Expr.Expression) (*Distinct, error) {
	return &Distinct{
		input:               input,
		schema:              input.Schema(),
		colExpr:             colExpr,
		seenValues:          make(map[string]struct{}),
		distinctValuesArray: make([]arrow.Array, len(input.Schema().Fields())),
	}, nil
}

// pipeline breaker. consume all, if row combonation is already seen, dont include in output
func (d *Distinct) Next(n uint16) (*operators.RecordBatch, error) {
	if d.done {
		return nil, io.EOF
	}
	mem := memory.NewGoAllocator()
	if !d.consumedInput {
		for {
			childBatch, err := d.input.Next(math.MaxUint16)
			if err != nil {
				if errors.Is(err, io.EOF) {
					d.consumedInput = true
					if d.distinctValuesArray[0] != nil { // nill check in case of no distict elements being found or even just input operator doesnt return anything
						d.totalRows = uint64(d.distinctValuesArray[0].Len())
					}
					break
				}
				return nil, err
			}
			// resolve the columns we care about
			evaluatedArrays := make([]arrow.Array, len(d.colExpr))
			for i := range d.colExpr {
				arr, err := Expr.EvalExpression(d.colExpr[i], childBatch)
				if err != nil {
					return nil, err
				}
				evaluatedArrays[i] = arr
			}
			var idxTracker []int32
			// Now iterate through each row in the batch
			numRows := int(childBatch.RowCount)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				// Build a key from the combination of values in this row
				var keyBuilder strings.Builder
				for colIdx, arr := range evaluatedArrays {
					if colIdx > 0 {
						keyBuilder.WriteString("|") // separator between columns
					}
					// Check if value is null
					if arr.IsNull(rowIdx) {
						keyBuilder.WriteString("NULL")
					} else {
						keyBuilder.WriteString(arr.ValueStr(rowIdx))
					}
				}

				key := keyBuilder.String()
				if _, seen := d.seenValues[key]; !seen {
					d.seenValues[key] = struct{}{}
					idxTracker = append(idxTracker, int32(rowIdx))
					// check if its been seen, if it hasnt been add it to the table,
					// and keep track of the index so we can grab the value from the array
				}
			}
			takeArray := idxToArrowArray(idxTracker, mem)
			for i := range len(childBatch.Columns) {
				largeArray := childBatch.Columns[i]
				uniqueElements, err := compute.TakeArray(context.TODO(), largeArray, takeArray)
				if err != nil {
					return nil, err
				}
				joinedArray, err := joinArrays(d.distinctValuesArray[i], uniqueElements, mem)
				if err != nil {
					return nil, err
				}
				d.distinctValuesArray[i] = joinedArray
			}
		}
	}
	var readsize uint64
	remaining := d.totalRows - d.consumedOffset
	if remaining == 0 { // we've consumed all the distinct arrays, operator is finished
		d.done = true
		return nil, io.EOF
	}
	// If remaining >= n, read n. Otherwise read what's left.
	if remaining >= uint64(n) {
		readsize = uint64(n)
	} else {
		readsize = remaining
	}
	distinctArraySlice, err := d.consumeDistinctArrays(readsize, mem)
	if err != nil {
		return nil, err
	}

	var rc uint64
	if len(distinctArraySlice) == 0 {
		rc = 0
	} else {
		rc = uint64(distinctArraySlice[0].Len())
	}
	return &operators.RecordBatch{
		Schema:   d.schema,
		Columns:  distinctArraySlice,
		RowCount: rc,
	}, nil
}
func (d *Distinct) Schema() *arrow.Schema { return d.schema }
func (d *Distinct) Close() error          { return d.input.Close() }
func (d *Distinct) consumeDistinctArrays(readSize uint64, mem memory.Allocator) ([]arrow.Array, error) {
	ctx := context.TODO()
	resultColumns := make([]arrow.Array, len(d.schema.Fields()))
	offsetArray := genoffsetTakeIdx(d.consumedOffset, readSize, mem)
	defer offsetArray.Release()
	for i := range d.distinctValuesArray {
		col := d.distinctValuesArray[i]
		slice, err := compute.TakeArray(ctx, col, offsetArray)
		if err != nil {
			return nil, err
		}
		resultColumns[i] = slice
	}
	d.consumedOffset += readSize
	return resultColumns, nil
}

func idxToArrowArray(v []int32, mem memory.Allocator) arrow.Array {
	// turn to array first
	b := array.NewInt32Builder(mem)
	defer b.Release()
	for _, val := range v {
		b.Append(val)
	}
	arr := b.NewArray()
	return arr
}
func joinArrays(a1, a2 arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if a1 == nil || a1.Len() == 0 {
		return a2, nil
	}
	if a2 == nil || a2.Len() == 0 {
		return a1, nil
	}
	return array.Concatenate([]arrow.Array{a1, a2}, mem)
}
func genoffsetTakeIdx(offset, size uint64, mem memory.Allocator) arrow.Array {
	b := array.NewUint64Builder(mem)
	defer b.Release()
	for i := range size {
		b.Append(offset + i)
	}
	return b.NewArray()
}
