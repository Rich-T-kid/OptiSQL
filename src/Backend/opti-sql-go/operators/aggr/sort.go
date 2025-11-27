package aggr

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	done     bool
	sortKeys []SortKey // resolves to columns
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
func (s *SortExec) Next(n uint16) (*operators.RecordBatch, error) {
	if s.done {
		return nil, io.EOF
	}
	allColumns := make([]arrow.Array, len(s.schema.Fields())) // concated columns
	mem := memory.NewGoAllocator()
	fmt.Printf("all columns init %v\n", allColumns)
	var count uint64
	for {
		childBatch, err := s.child.Next(n)
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
			largerArray, err := concatarr(allColumns[i], childBatch.Columns[i], mem)
			if err != nil {
				return nil, err
			}
			allColumns[i] = largerArray
		}
	}
	if len(allColumns) > 0 {
		count = uint64(allColumns[0].Len())
	}
	idx := sortBatches(&operators.RecordBatch{
		Schema:   s.schema,
		Columns:  allColumns,
		RowCount: count,
	}, s.sortKeys)
	// now update all mappings
	for i := range len(allColumns) {
		tmpDatum, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), compute.NewDatum(allColumns[i]), compute.NewDatum(toDatumFormat(idx, mem)))
		if err != nil {
			return nil, err
		}
		array, ok := tmpDatum.(*compute.ArrayDatum)
		if !ok {
			return nil, fmt.Errorf("non datum was returned from take")
		}
		allColumns[i] = array.MakeArray()
	}
	// TOOD: break this uo into N chunks
	return &operators.RecordBatch{
		Schema:   s.schema,
		Columns:  allColumns,
		RowCount: count,
	}, nil
}
func (s *SortExec) Schema() *arrow.Schema {
	return s.schema
}
func (s *SortExec) Close() error {
	return s.child.Close()
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
func sortBatches(fullRC *operators.RecordBatch, sortKeys []SortKey) []uint64 {
	keyColumns := make([]arrow.Array, len(sortKeys))
	for i, sk := range sortKeys {
		arr, err := Expr.EvalExpression(sk.Expr, fullRC)
		if err != nil {
			panic(fmt.Sprintf("sort batches: failed to eval sort expression: %v", err))
		}
		keyColumns[i] = arr
	}
	fmt.Printf("columns\n")
	for i, k := range keyColumns {
		fmt.Printf("%d:%v\n", i, k)
	}
	idVector := make([]uint64, fullRC.RowCount)
	for i := 0; uint64(i) < fullRC.RowCount; i++ {
		idVector[i] = uint64(i)
	}
	sortIndexVector(idVector, keyColumns, sortKeys)
	fmt.Printf("old Id Vec:%v\n", idVector)
	fmt.Printf("new ID vec: %v\n", idVector)
	return idVector
}
func toRC() []arrow.Array {
	return nil
}

func concatarr(a arrow.Array, b arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	return array.Concatenate([]arrow.Array{a, b}, mem)

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
func toDatumFormat(v []uint64, mem memory.Allocator) compute.Datum {
	// turn to array first
	b := array.NewUint64Builder(mem)
	defer b.Release()
	for _, val := range v {
		b.Append(val)
	}
	arr := b.NewArray()
	defer arr.Release()
	return compute.NewDatum(arr)
}
