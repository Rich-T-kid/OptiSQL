package join

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"opti-sql-go/operators/aggr"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var (
	ErrInvalidJoinClauseCount = func(l, r int) error {
		return fmt.Errorf("mismatched number of join expressions between left and right, left: %d vs right: %d", l, r)
	}
)

var (
	_ = (operators.Operator)(&SortMergeJoinExec{})
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	default:
		return "UNKNOWN JOIN TYPE"
	}
}

// taking in arrays of expressions allows for multiple join clauses
// Example: JOIN t2 ON t1.region = t2.region AND t1.city = t2.city
type JoinClause struct {
	leftS  []Expr.Expression
	rightS []Expr.Expression
}

func (j *JoinClause) String() string {
	var b bytes.Buffer

	// defensive: if lengths differ, print whatever pairs exist
	n := len(j.leftS)
	if len(j.rightS) < n {
		n = len(j.rightS)
	}

	for i := 0; i < n; i++ {
		b.WriteString(j.leftS[i].String())
		b.WriteString(" = ")
		b.WriteString(j.rightS[i].String())

		// add separator between pairs
		if i < n-1 {
			b.WriteString(" AND ")
		}
	}

	return b.String()
}

func NewJoinClause(leftS, rightS []Expr.Expression) JoinClause {
	return JoinClause{
		leftS:  leftS,
		rightS: rightS,
	}
}

// use sort merge join when the output needs to be sorted on the join keys
type SortMergeJoinExec struct {
	leftSource  operators.Operator
	rightSource operators.Operator
	clause      JoinClause
	joinType    JoinType
	filters     []Expr.Expression //TODO: incorpoarte
	schema      *arrow.Schema
	done        bool
	// internalState
	outputBatch []arrow.Array // intermediate storage for output arrays

}

func NewSortMergeJoinExec(left operators.Operator, right operators.Operator, clause JoinClause, joinType JoinType, filters []Expr.Expression) (*SortMergeJoinExec, error) {
	fmt.Printf("join clause: \t%v\njoin Type: \t%v\n", clause.String(), joinType)
	schema, err := joinSchemas(left.Schema(), right.Schema())
	if err != nil {
		return nil, err
	}
	// handle sorting this here. so the .Next function has less logic
	if len(clause.leftS) != len(clause.rightS) {
		return nil, ErrInvalidJoinClauseCount(len(clause.leftS), len(clause.rightS))
	}
	var Lsk []aggr.SortKey
	for i := 0; i < len(clause.leftS); i++ {
		Lsk = append(Lsk, aggr.SortKey{
			Expr:      clause.leftS[i],
			Ascending: true,
		})
	}
	var Rsk []aggr.SortKey
	for i := 0; i < len(clause.rightS); i++ {
		Rsk = append(Rsk, aggr.SortKey{
			Expr:      clause.rightS[i],
			Ascending: true,
		})
	}
	ls, err := aggr.NewSortExec(left, Lsk)
	if err != nil {
		return nil, err
	}
	rs, err := aggr.NewSortExec(right, Rsk)
	if err != nil {
		return nil, err
	}

	return &SortMergeJoinExec{
		leftSource:  rs,
		rightSource: ls,
		clause:      clause,
		joinType:    joinType,
		filters:     filters,
		schema:      schema,
		outputBatch: make([]arrow.Array, schema.NumFields()),
	}, nil
}

// TODO:

func (smj *SortMergeJoinExec) Next(n uint16) (*operators.RecordBatch, error) {
	if smj.done {
		return nil, io.EOF
	}
	return nil, nil
}
func (smj *SortMergeJoinExec) Schema() *arrow.Schema { return smj.schema }
func (smj *SortMergeJoinExec) Close() error {
	// do other clean up but for now just pass down to child
	err1 := smj.leftSource.Close()
	err2 := smj.rightSource.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

// left schema + right schema, if left and right have same column name, prefix with left_ and right_
func joinSchemas(left, right *arrow.Schema) (*arrow.Schema, error) {
	// table1 : id , name , age
	// table2 : id , dept , region
	fields := []arrow.Field{}

	leftNames := map[string]bool{}
	rightNames := map[string]bool{}

	for i := 0; i < left.NumFields(); i++ {
		leftNames[left.Field(i).Name] = true
	}
	for i := 0; i < right.NumFields(); i++ {
		rightNames[right.Field(i).Name] = true
	}
	// left side
	for i := 0; i < left.NumFields(); i++ {
		f := left.Field(i)
		name := f.Name
		if rightNames[name] {
			name = "left_" + name
		}
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata,
		})
	}

	// right side
	for i := 0; i < right.NumFields(); i++ {
		f := right.Field(i)
		name := f.Name
		if leftNames[name] {
			name = "right_" + name
		}
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata,
		})
	}

	return arrow.NewSchema(fields, nil), nil
	// produces
	// left_id ,name,age, right_id,dept,region
}

// otherwise go with hash joins
type HashJoinExec struct {
	leftSource  operators.Operator
	rightSource operators.Operator
	clause      JoinClause
	joinType    JoinType
	filters     []Expr.Expression //TODO: incorpoarte
	schema      *arrow.Schema
	done        bool
	// internalState
	outputBatch []arrow.Array // intermediate storage for output arrays

}
type hashEntry struct {
	row int
}
type joinPair struct {
	leftRow  int
	rightRow int
}

func NewHashJoinExec(left operators.Operator, right operators.Operator, clause JoinClause, joinType JoinType, filters []Expr.Expression) (*HashJoinExec, error) {
	fmt.Printf("join clause: \t%v\njoin Type: \t%v\n", clause.String(), joinType)
	schema, err := joinSchemas(left.Schema(), right.Schema())
	if err != nil {
		return nil, err
	}
	if len(clause.leftS) != len(clause.rightS) {
		return nil, ErrInvalidJoinClauseCount(len(clause.leftS), len(clause.rightS))
	}
	return &HashJoinExec{
		leftSource:  left,
		rightSource: right,
		clause:      clause,
		joinType:    joinType,
		filters:     filters,
		schema:      schema,
		outputBatch: make([]arrow.Array, schema.NumFields()),
	}, nil
}

func (hj *HashJoinExec) Next(_ uint16) (*operators.RecordBatch, error) {
	if hj.done {
		return nil, io.EOF
	}
	mem := memory.NewGoAllocator()
	leftArr, err := consumeOperator(hj.leftSource, mem)
	if err != nil {
		return nil, err
	}
	rightArr, err := consumeOperator(hj.rightSource, mem)
	if err != nil {
		return nil, err
	}
	if len(leftArr) == 0 || len(rightArr) == 0 {
		hj.done = true
		return &operators.RecordBatch{
			Schema:   hj.Schema(),
			RowCount: uint64(0),
		}, nil
	}
	leftRowCount := leftArr[0].Len()
	rightRowCount := rightArr[0].Len()
	//fmt.Printf("left:\t%v\nright:\t%v\n", leftArr, rightArr)
	leftComp, err := buildComptables(hj.clause.leftS, leftArr, hj.leftSource.Schema())
	if err != nil {
		return nil, err
	}

	rightComp, err := buildComptables(hj.clause.rightS, rightArr, hj.rightSource.Schema())
	if err != nil {
		return nil, err
	}
	fmt.Printf("left Comparission arrays:\t%v\nright Comparrission arrays:\t%v\n", leftComp, rightComp)
	ht := buildRightHashTable(rightComp, rightRowCount)
	pairs := probeJoin(leftComp, ht, leftRowCount)
	if len(pairs) == 0 {
		hj.done = true
		return &operators.RecordBatch{
			Schema:   hj.Schema(),
			Columns:  []arrow.Array{},
			RowCount: 0,
		}, nil
	}
	fmt.Printf("ht:\t%v\npairs:\t%v\n", ht, pairs)
	leftIdxArr, rightIdxArr, err := buildIndexArrays(mem, pairs)
	if err != nil {
		return nil, err
	}

	fmt.Printf("leftIDX:\t%v\nrightIDX:\t%v\n", leftIdxArr, rightIdxArr)
	outArr, err := hj.buildOutputArrays(mem, leftArr, rightArr, leftIdxArr, rightIdxArr)
	if err != nil {
		return nil, err
	}
	hj.done = true
	return &operators.RecordBatch{
		Schema:   hj.schema,
		Columns:  outArr,
		RowCount: uint64(outArr[0].Len()),
	}, nil
}
func (hj *HashJoinExec) Schema() *arrow.Schema { return hj.schema }
func (hj *HashJoinExec) Close() error {
	// do other clean up but for now just pass down to child
	err1 := hj.leftSource.Close()
	err2 := hj.rightSource.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func consumeOperator(o operators.Operator, mem memory.Allocator) ([]arrow.Array, error) {

	AllArrays := make([]arrow.Array, o.Schema().NumFields()) // concated columns
	for {
		childRecordBatch, err := o.Next(math.MaxUint16)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		for i := range childRecordBatch.Columns {
			if AllArrays[i] == nil {
				AllArrays[i] = childRecordBatch.Columns[i]
				continue
			}
			largerArray, err := array.Concatenate([]arrow.Array{AllArrays[i], childRecordBatch.Columns[i]}, mem)
			if err != nil {
				return nil, err
			}
			AllArrays[i] = largerArray
		}
	}
	return AllArrays, nil
}

func buildComptables(exprs []Expr.Expression, cols []arrow.Array, schema *arrow.Schema) ([]arrow.Array, error) {
	compArr := make([]arrow.Array, len(exprs))
	for i, expr := range exprs {
		arr, err := Expr.EvalExpression(expr, &operators.RecordBatch{
			Schema:   schema,
			Columns:  cols,
			RowCount: uint64(cols[0].Len()),
		})
		if err != nil {
			return nil, err
		}
		compArr[i] = arr
	}
	return compArr, nil

}

func buildRowKey(cols []arrow.Array, row int) string {
	var b strings.Builder
	hasNull := false

	for i, col := range cols {
		if i > 0 {
			b.WriteByte('|') // separator between cols
		}

		if col.IsNull(row) {
			hasNull = true
			// Keep a placeholder so non-null rows can’t collide with “all-null” rows.
			b.WriteString("NULL")
			continue
		}

		b.WriteString(col.ValueStr(row))
	}

	// If there were no NULLs in this row, we’re done.
	// Equal non-NULL rows on left/right will produce identical keys → join behaves as usual.
	if !hasNull {
		return b.String()
	}

	// SQL semantics: any NULL in the join key means this row should not match
	// anything from the other side. We “salt” the key with the identity of the
	// `cols` slice so left and right sides will produce *different* full keys.
	//
	// This still lets rows *on the same side* share a bucket (doesn’t hurt),
	// but probe from the other side will never see them as equal.
	b.WriteByte('#')
	b.WriteString(fmt.Sprintf("%p", cols))

	return b.String()
}

func buildRightHashTable(rightComp []arrow.Array, rowCount int) map[string][]hashEntry {
	ht := make(map[string][]hashEntry, rowCount)

	for r := 0; r < rowCount; r++ {
		key := buildRowKey(rightComp, r)
		ht[key] = append(ht[key], hashEntry{row: r})
	}
	return ht
}
func probeJoin(
	leftComp []arrow.Array,
	rightHT map[string][]hashEntry,
	leftRowCount int,
) []joinPair {
	var pairs []joinPair

	for l := 0; l < leftRowCount; l++ {
		key := buildRowKey(leftComp, l)
		matches := rightHT[key]
		if len(matches) == 0 {
			// inner join: skip if no matching right row
			continue
		}
		// emit all combinations
		for _, m := range matches {
			pairs = append(pairs, joinPair{
				leftRow:  l,
				rightRow: m.row,
			})
		}
	}

	return pairs
}

func buildIndexArrays(
	mem memory.Allocator,
	pairs []joinPair,
) (arrow.Array, arrow.Array, error) {
	// use int32 indexes (Arrow Take supports that)
	lb := array.NewInt32Builder(mem)
	rb := array.NewInt32Builder(mem)

	for _, p := range pairs {
		lb.Append(int32(p.leftRow))
		rb.Append(int32(p.rightRow))
	}

	leftIdxArr := lb.NewArray()
	rightIdxArr := rb.NewArray()
	lb.Release()
	rb.Release()

	return leftIdxArr, rightIdxArr, nil
}

func (hj *HashJoinExec) buildOutputArrays(
	mem memory.Allocator,
	leftCols []arrow.Array,
	rightCols []arrow.Array,
	leftIdxArr arrow.Array,
	rightIdxArr arrow.Array,
) ([]arrow.Array, error) {
	ctx := context.TODO()

	output := make([]arrow.Array, hj.schema.NumFields())
	for i := range len(leftCols) {
		col := leftCols[i]
		slice, err := compute.TakeArray(ctx, col, leftIdxArr)
		if err != nil {
			return nil, err
		}
		output[i] = slice
	}
	for i := range len(rightCols) {
		col := rightCols[i]
		slice, err := compute.TakeArray(ctx, col, rightIdxArr)
		if err != nil {
			return nil, err
		}
		output[i+len(leftCols)] = slice
	}
	return output, nil
}
