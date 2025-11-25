package aggr

import (
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
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

	groups map[string][]Accumulator // maps group by key to its accumulator
	keys   map[string][]any         // key → original values for output
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
		keys:        make(map[string][]any),
		groups:      make(map[string][]Accumulator),
	}, nil
}
func (g *GroupByExec) Next(batchSize uint16) (*operators.RecordBatch, error) {
	if g.done {
		return nil, io.EOF
	}
	return nil, nil
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

		// All aggregates produce float64 in your design
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

/*
TODO: use this in Next loop to skip boil plate creation code
func (g *GroupByExec) createAccumulators() []Accumulator {
	accumulators := make([]Accumulator, len(g.groupExpr))
	for i, expr := range g.groupExpr {
		switch expr.AggrFunc {
		case Min:
			accumulators[i] = newMinAggr()
		case Max:
			accumulators[i] = newMaxAggr()
		case Count:
			accumulators[i] = NewCountAggr()
		case Sum:
			accumulators[i] = NewSumAggr()
		case Avg:
			accumulators[i] = newAvgAggr()
		}
	}
	return accumulators
}
*/
