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
	_ = (accumulator)(&minAggrAccumulator{})
	_ = (accumulator)(&maxAggrAccumulator{})
	_ = (accumulator)(&countAggrAccumulator{})
	_ = (accumulator)(&sumAggrAccumulator{})
	_ = (accumulator)(&avgAggrAccumulator{})
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
	return &minAggrAccumulator{}
}

type minAggrAccumulator struct {
	minV       float64
	firstValue bool
}

func (m *minAggrAccumulator) Update(value float64) {
	if !m.firstValue {
		m.minV = value
		m.firstValue = true
		return
	}
	m.minV = min(m.minV, value)

}
func (m *minAggrAccumulator) Finalize() float64 { return m.minV }
func newMaxAggr() accumulator {
	return &maxAggrAccumulator{}
}

type maxAggrAccumulator struct {
	maxV       float64
	firstValue bool
}

func (m *maxAggrAccumulator) Update(value float64) {
	if !m.firstValue {
		m.maxV = value
		m.firstValue = true
		return
	}
	m.maxV = max(m.maxV, value)
}
func (m *maxAggrAccumulator) Finalize() float64 { return m.maxV }

func newCountAggr() accumulator {
	return &countAggrAccumulator{}
}

type countAggrAccumulator struct {
	count float64
}

func (c *countAggrAccumulator) Update(_ float64) {
	c.count++
}
func (c *countAggrAccumulator) Finalize() float64 { return c.count }

func newSumAggr() accumulator {
	return &sumAggrAccumulator{}
}

type sumAggrAccumulator struct {
	summation float64
}

func (s *sumAggrAccumulator) Update(value float64) {
	s.summation += value
}
func (s *sumAggrAccumulator) Finalize() float64 { return s.summation }
func newAvgAggr() accumulator {
	return &avgAggrAccumulator{}
}

type avgAggrAccumulator struct {
	used   bool
	values float64
	count  float64
}

func (a *avgAggrAccumulator) Update(value float64) {
	a.used = true
	a.values += value
	a.count++
}
func (a *avgAggrAccumulator) Finalize() float64 {
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
	input          operators.Operator   // child operator
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
			accs[i] = newCountAggr()
		case Sum:
			fieldName = fmt.Sprintf("sum_%s", agg.Child.String())
			accs[i] = newSumAggr()
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
		input:          child,
		schema:         arrow.NewSchema(fields, nil),
		aggExpressions: aggExprs,
		accumulators:   accs,
	}, nil
}

// Next consumes all batches from the child operator, evaluates the aggregate expressions,
// updates the accumulators for each value, and returns a single output batch containing
// the final aggregation results. It returns io.EOF after producing the result batch.
func (a *AggrExec) Next(n uint16) (*operators.RecordBatch, error) {
	if a.done {
		return nil, io.EOF
	}
	for {
		childBatch, err := a.input.Next(n)
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
		operators.ReleaseArrays(childBatch.Columns)
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
}

func (a *AggrExec) Schema() *arrow.Schema {
	return a.schema
}
func (a *AggrExec) Close() error {
	return a.input.Close()
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
