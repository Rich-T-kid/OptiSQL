package aggr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"sort"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// order by col asc, col 2 desc .... etc
var (
	_ = (operators.Operator)(&SortExec{})
	_ = (operators.Operator)(&TopKSortExec{})
)

type SortKey struct {
	Expr      Expr.Expression
	Ascending bool // by default false -- DESC (highest values first -> smaller values)
	NullFirst bool // by default false -- nulls last
}

func NewSortKey(expr Expr.Expression, options ...bool) *SortKey {
	var asc, nullF bool
	switch len(options) {
	case 2:
		asc = options[0]
		nullF = options[1]
	case 1:
		asc = options[0]
	}
	return &SortKey{
		Expr:      expr,
		Ascending: asc,
		NullFirst: nullF,
	}
}
func CombineSortKeys(sk ...*SortKey) []SortKey {
	var res []SortKey
	for _, s := range sk {
		res = append(res, *s)
	}
	return res
}

type SortExec struct {
	child    operators.Operator
	schema   *arrow.Schema
	sortKeys []SortKey // resolves to columns
	// internal book keeping
	totalColumns   []arrow.Array
	consumedOffset uint64
	totalRows      uint64
	consumed       bool // did we finish reading all of the child record batches?
	done           bool // have we already produced all the sorted record batches?
}

func NewSortExec(child operators.Operator, sortKeys []SortKey) (*SortExec, error) {
	fmt.Printf("sorts Keys %v\n", sortKeys)
	return &SortExec{
		child:    child,
		schema:   child.Schema(),
		sortKeys: sortKeys,
	}, nil
}

// for now read everything into memory and sort -- next steps will be to do external merge

// n is the number of records we will return,sortExec will read in 2^16-1 column entries from its child, this is more efficient that trusting the caller to pass in a reasonable
// n so that we avoid small/frequent IO operations
func (s *SortExec) Next(n uint16) (*operators.RecordBatch, error) {
	if s.done {
		return nil, io.EOF
	}
	if !s.consumed {
		allColumns := make([]arrow.Array, len(s.schema.Fields())) // concated columns
		mem := memory.NewGoAllocator()
		var count uint64
		for {
			childBatch, err := s.child.Next(math.MaxUint16)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			for i := range childBatch.Columns {
				if allColumns[i] == nil {
					allColumns[i] = childBatch.Columns[i]
					continue
				}
				largerArray, err := array.Concatenate([]arrow.Array{allColumns[i], childBatch.Columns[i]}, mem)
				if err != nil {
					return nil, err
				}
				allColumns[i] = largerArray
			}
		}
		s.consumed = true
		if len(allColumns) > 0 {
			count = uint64(allColumns[0].Len())
		}
		idx, err := sortBatches(&operators.RecordBatch{
			Schema:   s.schema,
			Columns:  allColumns,
			RowCount: count,
		}, s.sortKeys)
		if err != nil {
			return nil, err
		}
		// now update all mappings
		for i := range len(allColumns) {
			arr, err := compute.TakeArray(context.TODO(), allColumns[i], idxToArrowArray(idx, mem))
			if err != nil {
				return nil, err
			}
			allColumns[i] = arr
		}
		s.totalColumns = allColumns
		s.totalRows = count
	}
	var readSize uint64
	remaining := s.totalRows - s.consumedOffset
	if remaining < uint64(n) {
		// if n is more than we have left just read up to remaining
		readSize = uint64(remaining)
		s.done = true
	} else {
		// remaining > n or remaining = n then just read n and return
		readSize = uint64(n)
	}
	mem := memory.NewGoAllocator()
	sortedColumns, err := s.consumeSortedBatch(readSize, mem)
	if err != nil {
		return nil, err
	}

	return &operators.RecordBatch{
		Schema:   s.schema,
		Columns:  sortedColumns,
		RowCount: readSize,
	}, nil
}
func (s *SortExec) Schema() *arrow.Schema {
	return s.schema
}
func (s *SortExec) Close() error {
	return s.child.Close()
}
func (s *SortExec) consumeSortedBatch(readsize uint64, mem memory.Allocator) ([]arrow.Array, error) {
	ctx := context.TODO()
	resultColumns := make([]arrow.Array, len(s.schema.Fields()))
	offsetArray := genoffsetTakeIdx(s.consumedOffset, readsize, mem)
	for i := range s.totalColumns {
		sortArr := s.totalColumns[i]
		arr, err := compute.TakeArray(ctx, sortArr, offsetArray)
		if err != nil {
			return nil, err
		}
		resultColumns[i] = arr

	}
	s.consumedOffset += readsize
	return resultColumns, nil
}

/*
only sort and keep the top k elements in memory
*/
type TopKSortExec struct {
	child    operators.Operator
	schema   *arrow.Schema
	done     bool
	sortKeys []SortKey // resolves to columns
	k        uint16    // top k
}

func NewTopKSortExec(child operators.Operator, sortKeys []SortKey, k uint16) (*TopKSortExec, error) {
	fmt.Printf("sort keys %v\n", sortKeys)
	return &TopKSortExec{
		child:    child,
		schema:   child.Schema(),
		sortKeys: sortKeys,
		k:        k,
	}, nil
}

// for now read everything into memory and sort -- next steps will be to do external merge
func (t *TopKSortExec) Next(n uint16) (*operators.RecordBatch, error) {
	if t.done {
		return nil, io.EOF
	}
	return nil, nil
}
func (t *TopKSortExec) Schema() *arrow.Schema {
	return t.schema
}
func (t *TopKSortExec) Close() error {
	return t.child.Close()
}

/*
shared functions
*/
func sortBatches(fullRC *operators.RecordBatch, sortKeys []SortKey) ([]uint64, error) {
	keyColumns := make([]arrow.Array, len(sortKeys))
	for i, sk := range sortKeys {
		arr, err := Expr.EvalExpression(sk.Expr, fullRC)
		if err != nil {
			return nil, fmt.Errorf("sort batches: failed to eval sort expression: %v", err)
		}
		keyColumns[i] = arr
	}
	idVector := make([]uint64, fullRC.RowCount)
	for i := 0; uint64(i) < fullRC.RowCount; i++ {
		idVector[i] = uint64(i)
	}
	sortIndexVector(idVector, keyColumns, sortKeys)
	return idVector, nil
}

// sortIndexVector sorts idVec based on keyColumns + sortKeys.
// keyColumns[i] corresponds to sortKeys[i].
func sortIndexVector(idVec []uint64, keyColumns []arrow.Array, sortKeys []SortKey) {
	sort.Slice(idVec, func(a, b int) bool {
		i := idVec[a]
		j := idVec[b]

		// lexicographic: go through each sort key
		for k, col := range keyColumns {
			sk := sortKeys[k]
			cmp := compareArrowValues(col, i, j)

			if cmp == 0 {
				continue // equal → move to next key
			}

			if sk.Ascending {
				return cmp < 0
			} else {
				return cmp > 0
			}
		}

		// completely equal for all keys
		return false
	})
}

func compareArrowValues(col arrow.Array, i, j uint64) int {
	// Handle nulls (treat as lowest value for now)
	if col.IsNull(int(i)) && col.IsNull(int(j)) {
		return 0
	}
	if col.IsNull(int(i)) {
		return -1
	}
	if col.IsNull(int(j)) {
		return 1
	}

	switch arr := col.(type) {

	case *array.String:
		vi := arr.Value(int(i))
		vj := arr.Value(int(j))
		switch {
		case vi < vj:
			return -1
		case vi > vj:
			return 1
		default:
			return 0
		}

	case *array.Int8:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Int16:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Int32:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Int64:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Uint8:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Uint16:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Uint32:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Uint64:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareNumeric(vi, vj)

	case *array.Float32:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareFloat(vi, vj)

	case *array.Float64:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		return compareFloat(vi, vj)

	case *array.Boolean:
		vi, vj := arr.Value(int(i)), arr.Value(int(j))
		if vi == vj {
			return 0
		}
		if !vi && vj {
			return -1
		}
		return 1

	default:
		panic("unsupported Arrow type in compareArrowValues")
	}
}

func compareNumeric[T int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareFloat[T float32 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
func idxToArrowArray(v []uint64, mem memory.Allocator) arrow.Array {
	// turn to array first
	b := array.NewUint64Builder(mem)
	for _, val := range v {
		b.Append(val)
	}
	arr := b.NewArray()
	return arr
}
func genoffsetTakeIdx(offset, size uint64, mem memory.Allocator) arrow.Array {
	b := array.NewUint64Builder(mem)
	for i := range size {
		b.Append(offset + i)
	}
	arr := b.NewArray()
	return arr
}
