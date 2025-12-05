package aggr

import (
	"errors"
	"io"
	"strings"
	"testing"

	"opti-sql-go/Expr"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func TestHavingExec_OnGroupBy(t *testing.T) {

	// =============================================================
	// 1) HAVING SUM(salary) > 600000
	// =============================================================
	t.Run("having_sum_salary_gt_600k", func(t *testing.T) {

		child := groupByProject()

		groupBy := []Expr.Expression{col("department")}
		aggs := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("salary")},
		}

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected GroupBy error: %v", err)
		}

		sumCol := "sum_Column(salary)"

		// SUM(salary) > 600000
		havingExpr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve(sumCol),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 600000.0),
		)

		having, err := NewHavingExec(gb, havingExpr)
		if err != nil {
			t.Fatalf("unexpected HavingExec init error: %v", err)
		}

		batch, err := having.Next(1024)
		if err != nil {
			t.Fatalf("unexpected error running Next: %v", err)
		}
		t.Logf("batch : %v\n", batch.PrettyPrint())
		sumValues := batch.Columns[1].(*array.Float64)
		for i := 0; i < sumValues.Len(); i++ {
			if sumValues.Value(i) <= 600000 {
				t.Fatalf("expected sum(salary) > 600000, got %f", sumValues.Value(i))
			}
		}

	})

	// =============================================================
	// 2) HAVING COUNT(id) >= 10
	// =============================================================
	t.Run("having_count_id_ge_10", func(t *testing.T) {

		child := groupByProject()

		groupBy := []Expr.Expression{col("region")}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: col("id")},
		}

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected GroupBy err: %v", err)
		}

		countCol := "count_Column(id)"

		havingExpr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve(countCol),
			Expr.GreaterThanOrEqual,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 10.0),
		)

		having, err := NewHavingExec(gb, havingExpr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		batch, err := having.Next(200)
		if err != nil {
			t.Fatalf("unexpected Next error: %v", err)
		}

		if batch.RowCount != 3 { // North, South, West ≥ 10
			t.Fatalf("expected 3 regions with >=10 rows, got %d", batch.RowCount)
		}
	})

	// =============================================================
	// 3) HAVING filters all groups out
	// =============================================================
	t.Run("having_filters_all", func(t *testing.T) {

		child := groupByProject()

		groupBy := []Expr.Expression{col("department")}
		aggs := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("salary")},
		}

		gb, _ := NewGroupByExec(child, aggs, groupBy)

		sumCol := "sum_Column(salary)"

		// Impossible condition
		havingExpr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve(sumCol),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 1_000_000_000.0),
		)

		having, _ := NewHavingExec(gb, havingExpr)

		batch, err := having.Next(1024)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if batch.RowCount != 0 {
			t.Fatalf("expected all rows to be filtered out, got %d", batch.RowCount)
		}
	})

	// =============================================================
	// 4) Non-boolean predicate → error
	// =============================================================
	t.Run("having_non_boolean_predicate", func(t *testing.T) {

		child := groupByProject()
		groupBy := []Expr.Expression{col("department")}
		aggs := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("salary")},
		}

		gb, _ := NewGroupByExec(child, aggs, groupBy)

		// invalid: resolves to float, not boolean
		invalidExpr := Expr.NewColumnResolve("sum_Column(salary)")

		having, _ := NewHavingExec(gb, invalidExpr)

		_, err := having.Next(100)
		if err == nil {
			t.Fatalf("expected non-boolean error, got nil")
		}
		if !strings.Contains(err.Error(), "boolean") {
			t.Fatalf("expected boolean error, got: %v", err)
		}
	})

	// =============================================================
	// 5) done = true returns EOF
	// =============================================================
	t.Run("done_returns_eof", func(t *testing.T) {

		child := groupByProject()

		groupBy := []Expr.Expression{col("region")}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: col("id")},
		}

		gb, _ := NewGroupByExec(child, aggs, groupBy)

		countCol := "count_Column(id)"

		havingExpr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve(countCol),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 0.0),
		)

		h, _ := NewHavingExec(gb, havingExpr)
		h.done = true

		_, err := h.Next(10)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got: %v", err)
		}
	})

	// =============================================================
	// 6) Close forwards to child.Close()
	// =============================================================
	t.Run("close_propagates", func(t *testing.T) {

		child := groupByProject()

		gb, _ := NewGroupByExec(child, []AggregateFunctions{
			{AggrFunc: Count, Child: col("id")},
		}, []Expr.Expression{col("region")})

		h, _ := NewHavingExec(gb, Expr.NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true))

		if err := h.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		t.Log(h.Schema())
	})
}
