package aggr

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators/project"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
)

func generateGroupByTestColumns() ([]string, []any) {
	names := []string{
		"id",
		"name",
		"department",
		"region",
		"seniority",
		"salary",
		"age",
	}

	// 40 IDs
	ids := make([]int32, 40)
	for i := range ids {
		ids[i] = int32(i + 1)
	}

	// Names – 40 names
	namesArr := []string{
		"Alice", "Bob", "Charlie", "David", "Eve",
		"Frank", "Grace", "Hannah", "Ivy", "Jake",
		"Karen", "Leo", "Mona", "Nate", "Olive",
		"Paul", "Quinn", "Rita", "Sam", "Tina",
		"Uma", "Victor", "Wendy", "Xavier", "Yara",
		"Zane", "Becky", "Carlos", "Dora", "Elias",
		"Fiona", "Gabe", "Helena", "Isaac", "Julia",
		"Kevin", "Lara", "Miles", "Nora", "Owen",
	}

	// Randomized but balanced departments (5 groups)
	departments := []string{
		"Engineering", "HR", "Sales", "Engineering", "Finance",
		"Support", "Sales", "Engineering", "Support", "Finance",
		"HR", "Engineering", "Sales", "Support", "Finance",
		"Engineering", "Sales", "HR", "Support", "Engineering",
		"Finance", "Sales", "Engineering", "Support", "HR",
		"Support", "Engineering", "Finance", "Sales", "HR",
		"Engineering", "Support", "Finance", "Sales", "Engineering",
		"HR", "Finance", "Support", "Engineering", "Sales",
	}

	// Randomized but balanced regions (4 groups)
	regions := []string{
		"North", "East", "South", "West", "South",
		"North", "West", "East", "North", "South",
		"West", "East", "North", "South", "West",
		"North", "East", "West", "South", "North",
		"East", "West", "South", "North", "East",
		"South", "North", "West", "East", "South",
		"West", "North", "East", "South", "West",
		"North", "South", "East", "West", "North",
	}

	// Randomized seniority (3 groups)
	seniority := []string{
		"Junior", "Senior", "Mid", "Junior", "Mid",
		"Senior", "Junior", "Mid", "Senior", "Junior",
		"Mid", "Senior", "Junior", "Mid", "Senior",
		"Junior", "Mid", "Senior", "Junior", "Mid",
		"Senior", "Junior", "Mid", "Senior", "Junior",
		"Mid", "Senior", "Junior", "Mid", "Senior",
		"Junior", "Mid", "Senior", "Junior", "Mid",
		"Senior", "Junior", "Mid", "Senior", "Junior",
	}

	// Salaries (same as before)
	salaries := []float64{
		70000, 82000, 54000, 91000, 60000,
		75000, 66000, 88000, 45000, 99000,
		72000, 81000, 53000, 86000, 64000,
		93000, 68000, 76000, 89000, 71000,
		83000, 94000, 55000, 87000, 91500,
		72000, 69000, 58000, 84000, 79000,
		81000, 78000, 62000, 97000, 82000,
		95000, 76000, 88000, 91000, 64000,
	}

	// Ages with some repetition
	ages := []int32{
		28, 34, 45, 22, 31,
		29, 40, 36, 50, 26,
		33, 41, 27, 38, 24,
		46, 30, 35, 43, 32,
		39, 48, 29, 37, 42,
		28, 34, 45, 22, 31,
		29, 40, 36, 50, 26,
		39, 48, 29, 37, 42,
	}

	columns := []any{
		ids,
		namesArr,
		departments,
		regions,
		seniority,
		salaries,
		ages,
	}

	return names, columns
}

func groupByProject() *project.InMemorySource {
	names, cols := generateGroupByTestColumns()
	p, _ := project.NewInMemoryProjectExec(names, cols)
	return p
}

func TestGroupByInit(t *testing.T) {
	p := groupByProject()
	rc, _ := p.Next(12)
	fmt.Printf("rc:%v \n", rc)
}

func TestNewGroupByExecAndSchema(t *testing.T) {
	// convenience builder
	col := func(name string) Expr.Expression {
		return Expr.NewColumnResolve(name)
	}

	t.Run("single group-by single aggregate", func(t *testing.T) {
		child := groupByProject()

		groupBy := []Expr.Expression{col("department")}
		aggs := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("salary")},
		}

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		schema := gb.Schema()
		if schema == nil {
			t.Fatalf("schema should not be nil")
		}
		fmt.Println(schema)

		// group-by + 1 agg = 2 fields
		if got, want := schema.NumFields(), 2; got != want {
			t.Fatalf("expected %d fields, got %d", want, got)
		}

		// group field
		f0 := schema.Field(0)
		expName := "group_" + groupBy[0].String()
		if f0.Name != expName {
			t.Fatalf("expected group field name %q, got %q", expName, f0.Name)
		}

		// aggregate field
		f1 := schema.Field(1)
		properAggName := fmt.Sprintf("%s_%s",
			strings.ToLower(aggrToString(int(aggs[0].AggrFunc))),
			aggs[0].Child.String(),
		)
		if f1.Name != properAggName {
			t.Fatalf("expected agg field %q, got %q", properAggName, f1.Name)
		}

		if gb.groups == nil {
			t.Fatalf("groups map not initialized")
		}
		if gb.keys == nil {
			t.Fatalf("keys map not initialized")
		}
	})

	t.Run("multiple group-by and multiple aggregates", func(t *testing.T) {
		child := groupByProject()

		groupBy := []Expr.Expression{col("region"), col("seniority")}
		aggs := []AggregateFunctions{
			{AggrFunc: Min, Child: col("age")},
			{AggrFunc: Max, Child: col("salary")},
			{AggrFunc: Count, Child: col("id")},
		}

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		schema := gb.Schema()
		fmt.Printf("schema: %v\n", schema)
		wantFields := len(groupBy) + len(aggs)
		if schema.NumFields() != wantFields {
			t.Fatalf("expected %d fields, got %d", wantFields, schema.NumFields())
		}

		// group fields first
		for i, gexpr := range groupBy {
			f := schema.Field(i)
			exp := "group_" + gexpr.String()
			if f.Name != exp {
				t.Fatalf("group field[%d] mismatch: want %q got %q", i, exp, f.Name)
			}
		}

		// aggregate fields next
		offset := len(groupBy)
		for j, agg := range aggs {
			f := schema.Field(offset + j)
			expAggName := fmt.Sprintf("%s_%s",
				strings.ToLower(aggrToString(int(agg.AggrFunc))),
				agg.Child.String(),
			)
			if f.Name != expAggName {
				t.Fatalf("agg field name mismatch: want %q got %q", expAggName, f.Name)
			}
		}
	})

	t.Run("invalid group-by column triggers error", func(t *testing.T) {
		child := groupByProject()

		invalidGB := []Expr.Expression{col("not_a_col")}
		aggs := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("salary")},
		}

		// direct schema builder test
		_, err := buildGroupBySchema(child.Schema(), invalidGB, aggs)
		if err == nil {
			t.Fatalf("expected error for invalid group-by expr")
		}

		// NewGroupByExec should also fail
		if _, err := NewGroupByExec(child, aggs, invalidGB); err == nil {
			t.Fatalf("expected NewGroupByExec error for invalid group-by")
		}
	})

	t.Run("no aggregates - schema should only contain group-by columns", func(t *testing.T) {
		child := groupByProject()

		groupBy := []Expr.Expression{col("region")}
		var aggs []AggregateFunctions

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		schema := gb.Schema()

		if schema.NumFields() != 1 {
			t.Fatalf("expected 1 field, got %d", schema.NumFields())
		}

		f := schema.Field(0)
		exp := "group_" + groupBy[0].String()
		if f.Name != exp {
			t.Fatalf("wrong group field name: want %q got %q", exp, f.Name)
		}
	})

	t.Run("multiple aggregates produce float64 regardless of source type", func(t *testing.T) {
		child := groupByProject()

		groupBy := []Expr.Expression{col("department")}
		aggs := []AggregateFunctions{
			{AggrFunc: Avg, Child: col("age")},    // int32 → float64
			{AggrFunc: Sum, Child: col("salary")}, // float64 → float64
		}

		gb, err := NewGroupByExec(child, aggs, groupBy)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		schema := gb.Schema()

		// group-by + 2 aggregates = 3
		if schema.NumFields() != 3 {
			t.Fatalf("expected 3 fields, got %d", schema.NumFields())
		}

		for idx := 1; idx < 3; idx++ {
			f := schema.Field(idx)
			if f.Type.ID() != arrow.FLOAT64 {
				t.Fatalf("expected field[%d] to be float64, got %v", idx, f.Type)
			}
		}
	})

	t.Run("schema names must match exact string() output of expressions", func(t *testing.T) {
		child := groupByProject()

		gbExpr := []Expr.Expression{
			Expr.NewColumnResolve("seniority"),
			Expr.NewColumnResolve("region"),
		}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, err := NewGroupByExec(child, aggs, gbExpr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		schema := gb.Schema()

		expected0 := "group_" + gbExpr[0].String() // group_Column(seniority)
		expected1 := "group_" + gbExpr[1].String() // group_Column(region)

		if schema.Field(0).Name != expected0 {
			t.Fatalf("wrong field[0] name: want %q got %q", expected0, schema.Field(0).Name)
		}
		if schema.Field(1).Name != expected1 {
			t.Fatalf("wrong field[1] name: want %q got %q", expected1, schema.Field(1).Name)
		}

		// count column
		expectedAgg := "count_" + aggs[0].Child.String()
		if schema.Field(2).Name != expectedAgg {
			t.Fatalf("wrong agg field name: want %q got %q", expectedAgg, schema.Field(2).Name)
		}
	})
	t.Run("basic close check", func(t *testing.T) {
		child := groupByProject()

		gbExpr := []Expr.Expression{
			Expr.NewColumnResolve("seniority"),
			Expr.NewColumnResolve("region"),
		}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, err := NewGroupByExec(child, aggs, gbExpr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if gb.Close() != nil {
			t.Fatalf("unexpected error on close")
		}

	})
}
func TestBasicOperatorCasesGroupBy(t *testing.T) {

	t.Run("basic close check", func(t *testing.T) {
		child := groupByProject()

		gbExpr := []Expr.Expression{
			Expr.NewColumnResolve("seniority"),
			Expr.NewColumnResolve("region"),
		}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, err := NewGroupByExec(child, aggs, gbExpr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if gb.Close() != nil {
			t.Fatalf("unexpected error on close")
		}

	})
	t.Run("done case", func(t *testing.T) {
		child := groupByProject()

		gbExpr := []Expr.Expression{
			Expr.NewColumnResolve("seniority"),
			Expr.NewColumnResolve("region"),
		}
		aggs := []AggregateFunctions{
			{AggrFunc: Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, err := NewGroupByExec(child, aggs, gbExpr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		gb.done = true
		_, err = gb.Next(100)
		if err == nil || !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF but recieved %v", err)
		}

	})
}
