package aggr

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"strings"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

/*
rules for group by:
1.Every non-aggregated column in SELECT must be in GROUP BY
2.You can group by multiple columns - creates groups for each unique combination
3.Use HAVING to filter groups (WHERE filters before grouping, HAVING filters after)
*/
var (
	_ = (operators.Operator)(&GroupByExec{})
)

// place all unique elements of the group by column into a hash table, each element gets their own Accumulator instance
type GroupByExec struct {
	child       operators.Operator
	schema      *arrow.Schema
	groupExpr   []AggregateFunctions
	groupByExpr []Expr.Expression // column names

	groups map[string][]accumulator // maps group by key to its accumulator
	keys   map[string][]string      // key → original values for output
	done   bool
}

func NewGroupByExec(child operators.Operator, groupExpr []AggregateFunctions, groupBy []Expr.Expression) (*GroupByExec, error) {
	s, err := buildGroupBySchema(child.Schema(), groupBy, groupExpr)
	if err != nil {
		return nil, err
	}

	return &GroupByExec{
		child:       child,
		schema:      s,
		groupExpr:   groupExpr,
		groupByExpr: groupBy,
		keys:        make(map[string][]string),
		groups:      make(map[string][]accumulator),
	}, nil
}

/*
grab child rows
*/
func (g *GroupByExec) Next(batchSize uint16) (*operators.RecordBatch, error) {
	if g.done {
		return nil, io.EOF
	}

	for {
		childBatch, err := g.child.Next(batchSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		rowCount := int(childBatch.RowCount)

		// 1. evaluate all group-by expressions into arrays
		groupArrays := make([]arrow.Array, len(g.groupByExpr))
		for i, expr := range g.groupByExpr {
			arr, err := Expr.EvalExpression(expr, childBatch)
			if err != nil {
				return nil, err
			}
			groupArrays[i] = arr
		}

		// 2. evaluate all aggregation child expressions
		aggrArrays := make([]arrow.Array, len(g.groupExpr))
		for i, agg := range g.groupExpr {
			arr, err := Expr.EvalExpression(agg.Child, childBatch)
			if err != nil {
				return nil, err
			}
			arr, err = castArrayToFloat64(arr)
			if err != nil {
				return nil, err
			}
			aggrArrays[i] = arr
		}

		// 3. process rows
		for row := 0; row < rowCount; row++ {

			// Build group key
			keyParts := make([]string, len(groupArrays))
			for j, arr := range groupArrays {
				if arr.IsNull(row) {
					keyParts[j] = "NULL"
				} else {
					keyParts[j] = fmt.Sprintf("%v", getValue(arr, row))
				}
			}
			key := strings.Join(keyParts, "|")
			// Allocate accumulator list if new group
			if _, exists := g.groups[key]; !exists {
				g.groups[key] = make([]accumulator, len(g.groupExpr))
				for i, agg := range g.groupExpr {
					g.groups[key][i] = createAccumulator(agg.AggrFunc)
				}
				g.keys[key] = keyParts // store original values
			}

			// UPDATE accumulators
			for i, arr := range aggrArrays {
				if arr.IsNull(row) {
					continue
				}
				val := arr.(*array.Float64).Value(row)
				g.groups[key][i].Update(val)
			}
		}
	}

	// 4. Build output RecordBatch
	batch := buildGroupByOutput(g)

	g.done = true
	return batch, nil
}

func (g *GroupByExec) Schema() *arrow.Schema {
	return g.schema
}
func (g *GroupByExec) Close() error {
	return g.child.Close()
}

// handles validation and building of schema for group by
func buildGroupBySchema(childSchema *arrow.Schema, groupByExpr []Expr.Expression, aggrExprs []AggregateFunctions) (*arrow.Schema, error) {

	fields := make([]arrow.Field, 0, len(groupByExpr)+len(aggrExprs))

	// 1. Add group-by columns
	for _, expr := range groupByExpr {
		dt, err := Expr.ExprDataType(expr, childSchema)
		if err != nil {
			return nil, fmt.Errorf("group-by expr %s has invalid type: %w", expr.String(), err)
		}

		fields = append(fields, arrow.Field{
			Name:     fmt.Sprintf("group_%s", expr.String()),
			Type:     dt,
			Nullable: false,
		})
	}

	// 2. Add aggregate columns
	for _, agg := range aggrExprs {
		dt, err := Expr.ExprDataType(agg.Child, childSchema)
		if err != nil || !validAggrType(dt) {
			return nil, ErrInvalidAggrColumnType(dt)
		}
		// All aggregates produce float64
		fieldName := fmt.Sprintf("%s_%s",
			strings.ToLower(aggrToString(int(agg.AggrFunc))),
			agg.Child.String(),
		)

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: false,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

func getValue(arr arrow.Array, row int) any {
	switch col := arr.(type) {
	case *array.Int32:
		return col.Value(row)
	case *array.Int64:
		return col.Value(row)
	case *array.Float32:
		return col.Value(row)
	case *array.Float64:
		return col.Value(row)
	case *array.String:
		return col.Value(row)
	case *array.Boolean:
		return col.Value(row)
	default:
		// fallback – debug only
		return fmt.Sprintf("%v", col)
	}
}
func createAccumulator(fn AggrFunc) accumulator {
	switch fn {
	case Min:
		return newMinAggr()
	case Max:
		return newMaxAggr()
	case Sum:
		return NewSumAggr()
	case Count:
		return NewCountAggr()
	case Avg:
		return newAvgAggr()
	default:
		panic(fmt.Sprintf("unsupported aggregate function: %v", fn))
	}
}

func buildGroupByOutput(g *GroupByExec) *operators.RecordBatch {
	alloc := memory.NewGoAllocator()

	rowCount := len(g.groups)
	if rowCount == 0 {
		return &operators.RecordBatch{
			Schema:   g.schema,
			Columns:  []arrow.Array{},
			RowCount: 0,
		}
	}

	// Prepare column builders
	colBuilders := make([]arrow.Array, len(g.schema.Fields()))

	// Temporary storage for columns
	groupCols := make([][]any, len(g.groupByExpr))  // group columns
	aggrCols := make([][]float64, len(g.groupExpr)) // aggregate columns

	for i := range groupCols {
		groupCols[i] = make([]any, 0, rowCount)
	}
	for i := range aggrCols {
		aggrCols[i] = make([]float64, 0, rowCount)
	}

	// Iterate groups in stable order
	i := 0
	for key, accs := range g.groups {
		// Add group-by (dimension) values
		dims := g.keys[key]
		for j, v := range dims {
			groupCols[j] = append(groupCols[j], v)
		}

		// Add aggregated values
		for j, acc := range accs {
			aggrCols[j] = append(aggrCols[j], acc.Finalize())
		}

		i++
	}

	// Now build Arrow arrays in correct schema order
	fieldIndex := 0

	// Build group-by columns first
	for j := range g.groupByExpr {
		colBuilders[fieldIndex] = buildDynamicArray(alloc, g.schema.Field(fieldIndex).Type, groupCols[j])
		fieldIndex++
	}

	// Build aggregate columns
	for j := range g.groupExpr {
		colBuilders[fieldIndex] = buildFloatArray(alloc, aggrCols[j])
		fieldIndex++
	}

	return &operators.RecordBatch{
		Schema:   g.schema,
		Columns:  colBuilders,
		RowCount: uint64(rowCount),
	}
}
func buildDynamicArray(mem memory.Allocator, dt arrow.DataType, values []any) arrow.Array {
	switch dt.ID() {

	// ===========================
	// STRING (UTF8)
	// ===========================
	case arrow.STRING:
		sb := array.NewStringBuilder(mem)
		for _, v := range values {
			if v == nil {
				sb.AppendNull()
			} else {
				sb.Append(fmt.Sprintf("%v", v))
			}
		}
		return sb.NewArray()

	// ===========================
	// SIGNED INTEGERS
	// ===========================
	case arrow.INT8:
		b := array.NewInt8Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(int8))
			}
		}
		return b.NewArray()

	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(int16))
			}
		}
		return b.NewArray()

	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(int32))
			}
		}
		return b.NewArray()

	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(int64))
			}
		}
		return b.NewArray()

	// ===========================
	// UNSIGNED INTEGERS
	// ===========================
	case arrow.UINT8:
		b := array.NewUint8Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(uint8))
			}
		}
		return b.NewArray()

	case arrow.UINT16:
		b := array.NewUint16Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(uint16))
			}
		}
		return b.NewArray()

	case arrow.UINT32:
		b := array.NewUint32Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(uint32))
			}
		}
		return b.NewArray()

	case arrow.UINT64:
		b := array.NewUint64Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(uint64))
			}
		}
		return b.NewArray()

	// ===========================
	// FLOATS
	// ===========================
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(float32))
			}
		}
		return b.NewArray()

	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(float64))
			}
		}
		return b.NewArray()

	// ===========================
	// UNSUPPORTED TYPE
	// ===========================
	default:
		panic(fmt.Sprintf("unsupported dynamic array type: %v", dt))
	}
}

func buildFloatArray(mem memory.Allocator, values []float64) arrow.Array {
	b := array.NewFloat64Builder(mem)
	b.AppendValues(values, nil)
	return b.NewArray()
}
