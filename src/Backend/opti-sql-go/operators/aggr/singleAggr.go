package aggr

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
	ErrUnsupportedAggrFunc = func(aggr int) error {
		return fmt.Errorf("%d is an unsupported aggregate function", aggr)
	}
	ErrInvalidAggrColumnType = func(value any) error {
		return fmt.Errorf("%v of type %T cannot be cast to float64 so it is not a valid column type to aggregate on", value, value)
	}
)

// AggrFunc represents the type of aggregation function to be performed.
type AggrFunc int

const (
	Min AggrFunc = iota
	Max
	Count
	Sum
	Avg
)

var (
	_ = (accumulator)(&MinAggrAccumulator{})
	_ = (accumulator)(&MaxAggrAccumulator{})
	_ = (accumulator)(&CountAggrAccumulator{})
	_ = (accumulator)(&SumAggrAccumulator{})
	_ = (accumulator)(&AvgAggrAccumulator{})
	_ = (operators.Operator)(&AggrExec{})
)

func NewAggregateFunctions(aggrFunc AggrFunc, child Expr.Expression) AggregateFunctions {
	return AggregateFunctions{
		AggrFunc: aggrFunc,
		Child:    child,
	}
}

type AggregateFunctions struct {
	AggrFunc AggrFunc        // switch to deal with separate aggregate functions
	Child    Expr.Expression // resolves to a column generally
}
type accumulator interface {
	Update(value float64)
	Finalize() float64
}

func newMinAggr() accumulator {
	return &MinAggrAccumulator{}
}

type MinAggrAccumulator struct {
	minV       float64
	firstValue bool
}

func (m *MinAggrAccumulator) Update(value float64) {
	if !m.firstValue {
		m.minV = value
		m.firstValue = true
		return
	}
	m.minV = min(m.minV, value)

}
func (m *MinAggrAccumulator) Finalize() float64 { return m.minV }
func newMaxAggr() accumulator {
	return &MaxAggrAccumulator{}
}

type MaxAggrAccumulator struct {
	maxV       float64
	firstValue bool
}

func (m *MaxAggrAccumulator) Update(value float64) {
	if !m.firstValue {
		m.maxV = value
		m.firstValue = true
		return
	}
	m.maxV = max(m.maxV, value)
}
func (m *MaxAggrAccumulator) Finalize() float64 { return m.maxV }

func NewCountAggr() accumulator {
	return &CountAggrAccumulator{}
}

type CountAggrAccumulator struct {
	count float64
}

func (c *CountAggrAccumulator) Update(_ float64) {
	c.count++
}
func (c *CountAggrAccumulator) Finalize() float64 { return c.count }

func NewSumAggr() accumulator {
	return &SumAggrAccumulator{}
}

type SumAggrAccumulator struct {
	summation float64
}

func (s *SumAggrAccumulator) Update(value float64) {
	s.summation += value
}
func (s *SumAggrAccumulator) Finalize() float64 { return s.summation }
func newAvgAggr() accumulator {
	return &AvgAggrAccumulator{}
}

type AvgAggrAccumulator struct {
	used   bool
	values float64
	count  float64
}

func (a *AvgAggrAccumulator) Update(value float64) {
	a.used = true
	a.values += value
	a.count++
}
func (a *AvgAggrAccumulator) Finalize() float64 {
	// handles divide by zero
	if !a.used {
		return 0.0
	}
	return a.values / a.count
}

// ===================
// Aggregator Operator
// ===================
// handles global aggregations without group by
type AggrExec struct {
	child          operators.Operator   // child operator
	schema         *arrow.Schema        // output schema
	aggExpressions []AggregateFunctions // list of wanted aggregate expressions
	accumulators   []accumulator        // list of accumulators corresponding to aggExpressions, these will actually work to compute the aggregation
	done           bool                 // know when to return io.EOF
}

func NewGlobalAggrExec(child operators.Operator, aggExprs []AggregateFunctions) (*AggrExec, error) {
	accs := make([]accumulator, len(aggExprs))
	fields := make([]arrow.Field, len(aggExprs))
	for i, agg := range aggExprs {
		dt, err := Expr.ExprDataType(agg.Child, child.Schema())
		if err != nil || !validAggrType(dt) {
			return nil, ErrInvalidAggrColumnType(dt)
		}
		var fieldName string
		switch agg.AggrFunc {
		case Min:
			fieldName = fmt.Sprintf("min_%s", agg.Child.String())
			accs[i] = newMinAggr()
		case Max:
			fieldName = fmt.Sprintf("max_%s", agg.Child.String())
			accs[i] = newMaxAggr()
		case Count:
			fieldName = fmt.Sprintf("count_%s", agg.Child.String())
			accs[i] = NewCountAggr()
		case Sum:
			fieldName = fmt.Sprintf("sum_%s", agg.Child.String())
			accs[i] = NewSumAggr()
		case Avg:
			fieldName = fmt.Sprintf("avg_%s", agg.Child.String())
			accs[i] = newAvgAggr()

		default:
			return nil, ErrUnsupportedAggrFunc(int(agg.AggrFunc))
		}
		fields[i] = arrow.Field{
			Name:     fieldName,
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		}
	}
	return &AggrExec{
		child:          child,
		schema:         arrow.NewSchema(fields, nil),
		aggExpressions: aggExprs,
		accumulators:   accs,
	}, nil
}

// check for io.EOF with flag
// read in all record batches
// for each batch, run Expr.Evaluate, to get the column you want for the expression (cast to float64)
//
//	for each element of that column grab the values you want using the accumulator interface
//
// build output batch, for now its just 1 of everything straight forward
func (a *AggrExec) Next(n uint16) (*operators.RecordBatch, error) {
	if a.done {
		return nil, io.EOF
	}
	for {
		childBatch, err := a.child.Next(n)
		fmt.Printf("child batch: %v\n", childBatch)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		for i, aggExpr := range a.aggExpressions {
			agrArray, err := Expr.EvalExpression(aggExpr.Child, childBatch)
			if err != nil {
				return nil, err
			}
			agrArray, err = castArrayToFloat64(agrArray)
			if err != nil {
				return nil, err
			}
			valueArray := agrArray.(*array.Float64)
			accumulator := a.accumulators[i]
			for j := 0; j < valueArray.Len(); j++ {
				if valueArray.IsNull(j) {
					continue
				}
				accumulator.Update(valueArray.Value(j))
			}

		}
	}
	// build array with just the result of the column
	resultColumns := make([]arrow.Array, len(a.accumulators))
	for i := range a.accumulators {
		resultColumns[i] = operators.NewRecordBatchBuilder().GenFloatArray(a.accumulators[i].Finalize())
	}
	a.done = true
	return &operators.RecordBatch{
		Schema:   a.schema,
		Columns:  resultColumns,
		RowCount: 1,
	}, nil
	// this is a pipeline breaker so it will always consume all of the input which means this needs to return an io.EOF
}

func (a *AggrExec) Schema() *arrow.Schema {
	return a.schema
}
func (a *AggrExec) Close() error {
	return a.child.Close()
}

func validAggrType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func castArrayToFloat64(arr arrow.Array) (arrow.Array, error) {
	outDatum, err := compute.CastArray(context.TODO(), arr, compute.NewCastOptions(&arrow.Float64Type{}, true))
	if err != nil {
		return nil, err
	}

	return outDatum, nil
}
func aggrToString(t int) string {
	switch AggrFunc(t) {
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	case Count:
		return "COUNT"
	case Sum:
		return "SUM"
	case Avg:
		return "AVG"
	default:
		return "UNKNOWN_AGGREGATE_FUNCTION"
	}
}
