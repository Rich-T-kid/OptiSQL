package test

import (
	"errors"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	join "opti-sql-go/operators/Join"
	"opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/project"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// test for all operators together
// using in memory format at first
func generateIntegrationDataset1(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{
		"id", "first_name", "last_name", "age", "salary", "department", "region",
	}

	// id
	idB := array.NewInt32Builder(mem)
	idB.AppendValues(
		[]int32{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		},
		[]bool{
			true, true, true, true, true, true, true, true, true, true,
			true, true, true, true, true, true, true, true, true, true,
		},
	)
	idArr := idB.NewArray()

	// first_name
	fnB := array.NewStringBuilder(mem)
	fnB.AppendValues([]string{
		"Alice", "Bob", "Charlie", "Diana", "Eve",
		"Frank", "Grace", "Hank", "Ivy", "Jake",
		"Karen", "Leo", "Mona", "Nate", "Olivia",
		"Paul", "Quinn", "Ruth", "Steve", "Tina",
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	fnArr := fnB.NewArray()

	// last_name
	lnB := array.NewStringBuilder(mem)
	lnB.AppendValues([]string{
		"Smith", "Jones", "Stone", "Lopez", "King",
		"Hall", "Young", "Wright", "Hill", "Green",
		"Adams", "Clark", "Allen", "Baker", "Cox",
		"Diaz", "Evans", "Ford", "Gray", "Hart",
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	lnArr := lnB.NewArray()

	// age
	ageB := array.NewInt32Builder(mem)
	ageB.AppendValues([]int32{
		29, 34, 41, 26, 33,
		45, 38, 28, 52, 31,
		27, 49, 36, 42, 30,
		40, 50, 39, 55, 25,
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	ageArr := ageB.NewArray()

	// salary
	salB := array.NewFloat64Builder(mem)
	salB.AppendValues([]float64{
		70000, 80000, 65000, 72000, 59000,
		82000, 91000, 54000, 68000, 60000,
		75000, 88000, 56000, 69000, 62000,
		93000, 97000, 58000, 89000, 61000,
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	salArr := salB.NewArray()

	// department (some NULLs)
	deptB := array.NewStringBuilder(mem)
	deptB.AppendValues([]string{
		"HR", "Engineering", "Sales", "Finance", "HR",
		"Engineering", "Sales", "Finance", "HR", "Engineering",
		"Sales", "Finance", "HR", "Engineering", "Sales",
		"Finance", "HR", "Engineering", "Sales", "Finance",
	}, []bool{
		true, true, true, false, true,
		true, true, true, true, true,
		true, true, true, true, true,
		true, true, true, true, true,
	})
	deptArr := deptB.NewArray()

	// region (with NULLs)
	regB := array.NewStringBuilder(mem)
	regB.AppendValues([]string{
		"US", "EU", "US", "APAC", "LATAM",
		"US", "EU", "APAC", "LATAM", "US",
		"EU", "US", "LATAM", "EU", "APAC",
		"US", "EU", "LATAM", "US", "EU",
	}, []bool{
		true, true, true, true, true,
		true, true, false, true, true,
		true, true, true, true, true,
		true, true, true, true, false,
	})
	regArr := regB.NewArray()

	return names, []arrow.Array{idArr, fnArr, lnArr, ageArr, salArr, deptArr, regArr}
}

func generateIntegrationDataset2(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"dept_id", "department", "region", "budget", "manager"}

	// dept_id
	idB := array.NewInt32Builder(mem)
	idB.AppendValues([]int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	idArr := idB.NewArray()

	// department
	deptB := array.NewStringBuilder(mem)
	deptB.AppendValues([]string{
		"HR", "Engineering", "Sales", "Finance", "Marketing",
		"Support", "Research", "Security", "Legal", "Operations",
		"HR", "Engineering", "Sales", "Finance", "Marketing",
		"Support", "Research", "Security", "Legal", "Operations",
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	deptArr := deptB.NewArray()

	// region (with NULLs)
	regB := array.NewStringBuilder(mem)
	regB.AppendValues([]string{
		"US", "EU", "LATAM", "APAC", "US",
		"EU", "LATAM", "APAC", "US", "EU",
		"LATAM", "US", "EU", "APAC", "US",
		"LATAM", "US", "EU", "APAC", "US",
	}, []bool{
		true, true, true, true, true,
		true, true, true, true, true,
		true, true, true, true, false,
		true, true, true, true, true,
	})
	regArr := regB.NewArray()

	// budget
	budB := array.NewFloat64Builder(mem)
	budB.AppendValues([]float64{
		1e6, 2e6, 3e6, 1.5e6, 1.2e6,
		900000, 850000, 780000, 950000, 1100000,
		1e6, 2e6, 3e6, 1.5e6, 1.2e6,
		900000, 850000, 780000, 950000, 1100000,
	}, []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true,
	})
	budArr := budB.NewArray()

	// manager (with NULLs)
	manB := array.NewStringBuilder(mem)
	manB.AppendValues([]string{
		"Anna", "Ben", "Chris", "Dana", "Eli",
		"Faye", "George", "Holly", "Ian", "Jane",
		"Karl", "Lilly", "Mason", "Nora", "Owen",
		"Pam", "Quinn", "Rose", "Sam", "Tara",
	}, []bool{
		true, true, true, true, true,
		true, true, true, false, true,
		true, true, true, true, true,
		true, true, true, true, true,
	})
	manArr := manB.NewArray()

	return names, []arrow.Array{idArr, deptArr, regArr, budArr, manArr}
}
func NewIntegrationSource1(mem memory.Allocator) (*project.InMemorySource, error) {
	names, cols := generateIntegrationDataset1(mem)
	return project.NewInMemoryProjectExecFromArrays(names, cols)
}

func NewIntegrationSource2(mem memory.Allocator) (*project.InMemorySource, error) {
	names, cols := generateIntegrationDataset2(mem)
	return project.NewInMemoryProjectExecFromArrays(names, cols)
}

/*
============================================================================
Project tests
============================================================================
*/
func TestProjectExec(t *testing.T) {
	t.Run("integration_project_exec", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		src, err := NewIntegrationSource1(mem)
		if err != nil {
			t.Fatalf("failed to create integration source: %v", err)
		}
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewAlias(Expr.NewColumnResolve("age"), "age"),
			Expr.NewColumnResolve("salary"),
			Expr.NewColumnResolve("department"),
		)
		basicProj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("unexpected error\t%v\n", basicProj)
		}
		//t.Logf("%v\n", basicProj.Schema())
		rc, err := basicProj.Next(100)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error %v\n", err)
			}
		}
		if rc.RowCount != 20 {
			t.Fatalf("expected 20 rows, got %d", rc.RowCount)
		}
	})
	t.Run("projection_with_alias", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		src, _ := NewIntegrationSource1(mem)

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewAlias(Expr.NewColumnResolve("salary"), "emp_salary"),
		)

		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		batch, _ := proj.Next(50)

		// verify alias appears in schema
		if batch.Schema.Fields()[1].Name != "emp_salary" {
			t.Fatalf("expected alias emp_salary, got %s", batch.Schema.Fields()[1].Name)
		}
	})
	t.Run("projection_expression_math", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		src, _ := NewIntegrationSource1(mem)

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewAlias(
				Expr.NewBinaryExpr(
					Expr.NewColumnResolve("salary"),
					Expr.Multiplication,
					Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(1.10)),
				),
				"adjusted_salary",
			),
		)

		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		batch, _ := proj.Next(50)

		adjCol := batch.Columns[1].(*array.Float64)
		_, origin := generateIntegrationDataset1(mem)
		sal := origin[4].(*array.Float64)
		// check: for a non-null salary (row 0 = 50000)
		if adjCol.Len() != sal.Len() {
			t.Fatalf("expected adjusted salary length %d, got %d", sal.Len(), adjCol.Len())
		}
		for i := 0; i < adjCol.Len(); i++ {
			if !sal.IsNull(i) {
				expected := sal.Value(i) * 1.10
				if adjCol.Value(i) != expected {
					t.Fatalf("row %d: expected adjusted salary %f, got %f", i, expected, adjCol.Value(i))
				}
			}
		}
	})
	t.Run("projection_upper_first_name", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		src, err := NewIntegrationSource1(mem)
		if err != nil {
			t.Fatalf("failed to create integration source: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewAlias(
				Expr.NewScalarFunction(Expr.Upper, Expr.NewColumnResolve("first_name")),
				"first_name_upper",
			),
		)

		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("unexpected project exec error: %v", err)
		}

		batch, err := proj.Next(100) // pull all rows at once
		if err != nil {
			t.Fatalf("unexpected error on Next: %v", err)
		}
		if batch == nil {
			t.Fatalf("expected a batch but got nil")
		}

		// ---- get projected column (index 0) ----
		upperCol := batch.Columns[0].(*array.String)

		// ---- get original dataset to compare ----
		_, originCols := generateIntegrationDataset1(mem)
		firstNameCol := originCols[1].(*array.String) // index 1 is first_name

		if upperCol.Len() != firstNameCol.Len() {
			t.Fatalf("length mismatch: expected %d got %d",
				firstNameCol.Len(), upperCol.Len())
		}

		// ---- validate uppercase projection ----
		for i := 0; i < upperCol.Len(); i++ {
			if firstNameCol.IsNull(i) {
				if !upperCol.IsNull(i) {
					t.Fatalf("row %d: expected NULL but got value", i)
				}
				continue
			}

			expected := strings.ToUpper(firstNameCol.Value(i))
			got := upperCol.Value(i)

			if expected != got {
				t.Fatalf("row %d: expected %q, got %q", i, expected, got)
			}
		}
	})

}

/*
============================================================================
Filter tests
============================================================================
*/
func TestFilterExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	// ----------------------------------------------------------------------
	t.Run("filter_age_gt_30", func(t *testing.T) {
		names, cols := generateIntegrationDataset1(mem)
		src, err := project.NewInMemoryProjectExecFromArrays(names, cols)
		if err != nil {
			t.Fatalf("failed to create in-memory source: %v", err)
		}
		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(30)),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		batch, err := filt.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		if batch == nil {
			t.Fatalf("expected rows, got nil batch")
		}
		ageCol, _ := batch.ColumnByName("age")
		for i := 0; i < ageCol.Len(); i++ {
			ageValue := ageCol.(*array.Int32).Value(i)
			if ageValue <= 30 {
				t.Fatalf("expected age > 30, got %d", ageValue)
			}
		}

	})

	// ----------------------------------------------------------------------
	t.Run("filter_engineering_and_salary_gt_70000", func(t *testing.T) {
		names, cols := generateIntegrationDataset1(mem)
		src, err := project.NewInMemoryProjectExecFromArrays(names, cols)
		if err != nil {
			t.Fatalf("failed to create in-memory source: %v", err)
		}
		pred := Expr.NewBinaryExpr(
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("department"),
				Expr.Equal,
				Expr.NewLiteralResolve(arrow.BinaryTypes.String, "Engineering"),
			),
			Expr.And,
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("salary"),
				Expr.GreaterThan,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(70000)),
			),
		)
		// department = 'Engineering' AND salary > 70000

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		batch, err := filt.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Fatalf("expected non-nil batch")
		}

		// validate
		deptCol, _ := batch.ColumnByName("department")
		salCol, _ := batch.ColumnByName("salary")
		depColumn, _ := deptCol.(*array.String)
		salColumn, _ := salCol.(*array.Float64)
		for i := 0; i < int(batch.RowCount); i++ {
			if depColumn.Value(i) != "Engineering" {
				t.Fatalf("expected department 'Engineering', got %s", depColumn.Value(i))
			}
			if salColumn.Value(i) <= 70000 {
				t.Fatalf("expected salary > 70000, got %f", salColumn.Value(i))
			}
		}
	})

	// ----------------------------------------------------------------------
	t.Run("filter_region_is_null", func(t *testing.T) {
		names, cols := generateIntegrationDataset1(mem)
		src, err := project.NewInMemoryProjectExecFromArrays(names, cols)
		if err != nil {
			t.Fatalf("failed to create in-memory source: %v", err)
		}
		// We're filtering region IS NULL
		pred := Expr.NewNullCheckExpr(Expr.NewColumnResolve("region"))

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		batch, err := filt.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		if batch == nil {
			// possible: no NULLS
			t.Fatalf("expected atleast one null")
			return
		}
		t.Logf("batch: \t%v\n", batch.PrettyPrint())
		// validate
		regionCol, _ := batch.ColumnByName("region")
		regionArr := regionCol.(*array.String)
		for i := 0; i < int(batch.RowCount); i++ {
			if regionArr.IsNull(i) {
				t.Fatalf("expected NULL region but got value=%s", regionArr.Value(i))
			}
		}
	})

}

/*
============================================================================
Sort tests
============================================================================
*/
func TestSortTest(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("sort_salary_ascending", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		sortKeys := []aggr.SortKey{
			{Expr: Expr.NewColumnResolve("salary"), Ascending: true},
		}

		sortExec, err := aggr.NewSortExec(src, sortKeys)
		if err != nil {
			t.Fatalf("failed to create sort exec: %v", err)
		}

		batch, err := sortExec.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		salaryArr := batch.Columns[4].(*array.Float64)

		for i := 1; i < salaryArr.Len(); i++ {
			if salaryArr.IsNull(i-1) || salaryArr.IsNull(i) {
				continue
			}
			if salaryArr.Value(i) < salaryArr.Value(i-1) {
				t.Fatalf("salary not sorted ASC at row %d: %f < %f",
					i, salaryArr.Value(i), salaryArr.Value(i-1))
			}
		}

	})

	// ─────────────────────────────────────────────────────────────

	t.Run("sort_lastname_descending", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		sortKeys := []aggr.SortKey{
			{Expr: Expr.NewColumnResolve("last_name"), Ascending: false},
		}

		sortExec, err := aggr.NewSortExec(src, sortKeys)
		if err != nil {
			t.Fatalf("failed to create sort exec: %v", err)
		}

		batch, err := sortExec.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		lastArr := batch.Columns[2].(*array.String)

		for i := 1; i < lastArr.Len(); i++ {
			if lastArr.IsNull(i-1) || lastArr.IsNull(i) {
				continue
			}

			// descending → current <= previous
			if lastArr.Value(i) > lastArr.Value(i-1) {
				t.Fatalf("last_name not sorted DESC at %d: %s > %s",
					i, lastArr.Value(i), lastArr.Value(i-1))
			}
		}
	})

	// ─────────────────────────────────────────────────────────────

	t.Run("sort_department_then_salary_desc", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		sortKeys := []aggr.SortKey{
			{Expr: Expr.NewColumnResolve("department"), Ascending: true}, // asc
			{Expr: Expr.NewColumnResolve("salary"), Ascending: false},    // desc
		}

		sortExec, err := aggr.NewSortExec(src, sortKeys)
		if err != nil {
			t.Fatalf("failed to create sort exec: %v", err)
		}

		batch, err := sortExec.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		deptArr := batch.Columns[5].(*array.String)
		salaryArr := batch.Columns[4].(*array.Float64)

		for i := 1; i < deptArr.Len(); i++ {
			if deptArr.IsNull(i) || deptArr.IsNull(i-1) {
				continue
			}

			prevDept := deptArr.Value(i - 1)
			currDept := deptArr.Value(i)

			// department ascending grouping
			if currDept < prevDept {
				t.Fatalf("department not sorted ASC at %d: %s < %s",
					i, currDept, prevDept)
			}

			// if same department → salary must be descending
			if currDept == prevDept {
				if !salaryArr.IsNull(i) && !salaryArr.IsNull(i-1) {
					if salaryArr.Value(i) > salaryArr.Value(i-1) {
						t.Fatalf("salary not DESC within department '%s' at row %d",
							currDept, i)
					}
				}
			}
		}
	})
}

/*
============================================================================
Aggregations tests
============================================================================
*/
func TestIntegrationAggregations(t *testing.T) {
	t.Run("sum_avg_min_max_salary", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		// Load integration dataset
		_, cols := generateIntegrationDataset1(mem)
		salaryArr := cols[4].(*array.Float64)

		// Expected values
		var sum float64
		min := math.MaxFloat64
		max := -math.MaxFloat64
		count := 0

		for i := 0; i < salaryArr.Len(); i++ {
			if salaryArr.IsNull(i) {
				continue
			}
			v := salaryArr.Value(i)
			sum += v
			count++
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		avg := sum / float64(count)

		// Build aggregation operator
		src, _ := NewIntegrationSource1(mem)

		salCol := Expr.NewColumnResolve("salary")

		agg, err := aggr.NewGlobalAggrExec(src,
			[]aggr.AggregateFunctions{aggr.NewAggregateFunctions(aggr.Sum, salCol),
				aggr.NewAggregateFunctions(aggr.Avg, salCol),
				aggr.NewAggregateFunctions(aggr.Min, salCol),
				aggr.NewAggregateFunctions(aggr.Max, salCol)})
		if err != nil {
			t.Fatalf("aggregation init failed: %v", err)
		}

		batch, err := agg.Next(100)
		if err != nil {
			t.Fatalf("aggregation next failed: %v", err)
		}

		// Extract columns from result
		sumArr := batch.Columns[0].(*array.Float64)
		avgArr := batch.Columns[1].(*array.Float64)
		minArr := batch.Columns[2].(*array.Float64)
		maxArr := batch.Columns[3].(*array.Float64)

		if sumArr.Value(0) != sum {
			t.Fatalf("SUM mismatch: expected %f, got %f", sum, sumArr.Value(0))
		}
		if avgArr.Value(0) != avg {
			t.Fatalf("AVG mismatch: expected %f, got %f", avg, avgArr.Value(0))
		}
		if minArr.Value(0) != min {
			t.Fatalf("MIN mismatch: expected %f, got %f", min, minArr.Value(0))
		}
		if maxArr.Value(0) != max {
			t.Fatalf("MAX mismatch: expected %f, got %f", max, maxArr.Value(0))
		}
	})

	// ─────────────────────────────────────────────────────────────

	t.Run("sum_age", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		_, cols := generateIntegrationDataset1(mem)
		ageArr := cols[3].(*array.Int32)

		// Expected SUM(age)
		var sum int32
		for i := 0; i < ageArr.Len(); i++ {
			if !ageArr.IsNull(i) {
				sum += ageArr.Value(i)
			}
		}

		src, _ := NewIntegrationSource1(mem)

		agg, err := aggr.NewGlobalAggrExec(
			src,
			[]aggr.AggregateFunctions{
				aggr.NewAggregateFunctions(
					aggr.Sum, Expr.NewColumnResolve("age")),
			},
		)
		if err != nil {
			t.Fatalf("agg init failed: %v", err)
		}

		batch, _ := agg.Next(100)
		sumArr := batch.Columns[0].(*array.Float64) // SUM(int32) -> int64

		if sumArr.Value(0) != float64(sum) {
			t.Fatalf("SUM(age) mismatch: expected %v, got %v", sum, sumArr.Value(0))
		}
	})

	// ─────────────────────────────────────────────────────────────

	t.Run("min_max_age", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		_, cols := generateIntegrationDataset1(mem)
		ageArr := cols[3].(*array.Int32)

		min := int32(math.MaxInt32)
		max := int32(math.MinInt32)

		for i := 0; i < ageArr.Len(); i++ {
			if ageArr.IsNull(i) {
				continue
			}
			v := ageArr.Value(i)
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}

		src, _ := NewIntegrationSource1(mem)

		agg, err := aggr.NewGlobalAggrExec(src,
			[]aggr.AggregateFunctions{
				aggr.NewAggregateFunctions(aggr.Min, Expr.NewColumnResolve("age")),
				aggr.NewAggregateFunctions(aggr.Max, Expr.NewColumnResolve("age")),
			})
		if err != nil {
			t.Fatalf("agg init failed: %v", err)
		}

		batch, _ := agg.Next(100)

		minArr := batch.Columns[0].(*array.Float64)
		maxArr := batch.Columns[1].(*array.Float64)

		if minArr.Value(0) != float64(min) {
			t.Fatalf("MIN(age) mismatch: expected %v, got %v", min, minArr.Value(0))
		}
		if maxArr.Value(0) != float64(max) {
			t.Fatalf("MAX(age) mismatch: expected %v, got %v", max, maxArr.Value(0))
		}
	})
}

/*
============================================================================
Group-by tests
============================================================================
*/

func TestGroupByExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Utility helper to get origin dataset quickly
	_, originCols := generateIntegrationDataset1(mem)

	// ------------------------------------------------------------
	t.Run("group_by_department_count", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		dept := Expr.NewColumnResolve("department")

		groupByExpr := []Expr.Expression{dept}
		aggs := []aggr.AggregateFunctions{
			{AggrFunc: aggr.Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, err := aggr.NewGroupByExec(src, aggs, groupByExpr)
		if err != nil {
			t.Fatalf("gb init failed: %v", err)
		}

		batch, err := gb.Next(1024)
		if err != nil {
			t.Fatalf("group by Next failed: %v", err)
		}

		deptCol := batch.Columns[0].(*array.String)
		countCol := batch.Columns[1].(*array.Float64) // count returns float64 in your impl

		// Validate counts by manually counting departments
		origDept := originCols[5].(*array.String)
		expected := make(map[string]int)

		for i := 0; i < origDept.Len(); i++ {
			if origDept.IsNull(i) {
				expected["NULL"]++
			} else {
				expected[origDept.Value(i)]++
			}
		}

		for i := 0; i < int(batch.RowCount); i++ {
			key := "NULL"
			if !deptCol.IsNull(i) {
				key = deptCol.Value(i)
			}
			got := int(countCol.Value(i))
			want := expected[key]

			if got != want {
				t.Fatalf("group %s: expected %d, got %d", key, want, got)
			}
		}
	})

	// ------------------------------------------------------------
	t.Run("group_by_department_region_sum_salary", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		dept := Expr.NewColumnResolve("department")
		region := Expr.NewColumnResolve("region")

		groupByExpr := []Expr.Expression{dept, region}
		aggs := []aggr.AggregateFunctions{
			{AggrFunc: aggr.Sum, Child: Expr.NewColumnResolve("salary")},
		}

		gb, err := aggr.NewGroupByExec(src, aggs, groupByExpr)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}

		batch, err := gb.Next(1024)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		deptCol := batch.Columns[0].(*array.String)
		regionCol := batch.Columns[1].(*array.String)
		sumCol := batch.Columns[2].(*array.Float64)

		origDept := originCols[5].(*array.String)
		origRegion := originCols[6].(*array.String)
		origSalary := originCols[4].(*array.Float64)

		expected := make(map[string]float64)

		for i := 0; i < origSalary.Len(); i++ {
			d := "NULL"
			if !origDept.IsNull(i) {
				d = origDept.Value(i)
			}

			r := "NULL"
			if !origRegion.IsNull(i) {
				r = origRegion.Value(i)
			}

			key := d + "|" + r
			expected[key] += origSalary.Value(i)
		}

		for i := 0; i < int(batch.RowCount); i++ {
			d := "NULL"
			if !deptCol.IsNull(i) {
				d = deptCol.Value(i)
			}

			r := "NULL"
			if !regionCol.IsNull(i) {
				r = regionCol.Value(i)
			}

			key := d + "|" + r
			got := sumCol.Value(i)
			want := expected[key]

			if got != want {
				t.Fatalf("(%s,%s): expected sum=%f, got %f", d, r, want, got)
			}
		}
	})

	// ------------------------------------------------------------
	t.Run("group_by_with_null_keys", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		src, _ := NewIntegrationSource1(mem)

		region := Expr.NewColumnResolve("region")

		groupByExpr := []Expr.Expression{region}
		aggs := []aggr.AggregateFunctions{
			{AggrFunc: aggr.Count, Child: Expr.NewColumnResolve("id")},
		}

		gb, _ := aggr.NewGroupByExec(src, aggs, groupByExpr)

		batch, err := gb.Next(1024)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		regionCol := batch.Columns[0].(*array.String)
		countCol := batch.Columns[1].(*array.Float64)

		origRegion := originCols[6].(*array.String)
		expected := make(map[string]int)

		for i := 0; i < origRegion.Len(); i++ {
			key := "NULL"
			if !origRegion.IsNull(i) {
				key = origRegion.Value(i)
			}
			expected[key]++
		}

		for i := 0; i < int(batch.RowCount); i++ {
			k := "NULL"
			if !regionCol.IsNull(i) {
				k = regionCol.Value(i)
			}

			got := int(countCol.Value(i))
			want := expected[k]

			if got != want {
				t.Fatalf("region=%s expected %d got %d", k, want, got)
			}
		}
	})
}

/*
============================================================================
Having tests
============================================================================
*/
func TestHavingExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	// helper — build group by department avg salary
	buildDeptAvg := func() operators.Operator {
		src, _ := NewIntegrationSource1(mem)

		aggs := []aggr.AggregateFunctions{
			{AggrFunc: aggr.Avg, Child: Expr.NewColumnResolve("salary")},
		}

		gb, _ := aggr.NewGroupByExec(src, aggs,
			[]Expr.Expression{Expr.NewColumnResolve("department")},
		)
		return gb
	}

	// ------------------------------------------------------------
	t.Run("having_avg_salary_gt_75000", func(t *testing.T) {
		gb := buildDeptAvg()

		having := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("avg_Column(salary)"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(75000)),
		)

		hv, _ := aggr.NewHavingExec(gb, having)
		fmt.Printf("\t%v\n", hv.Schema())
		batch, err := hv.Next(500)
		if err != nil {
			t.Fatalf("having next failed: %v", err)
		}
		t.Logf("batch:\t%v\n", batch.PrettyPrint())

		deptCol := batch.Columns[0].(*array.String)
		avgCol := batch.Columns[1].(*array.Float64)

		for i := 0; i < int(batch.RowCount); i++ {
			if avgCol.Value(i) <= 75000 {
				t.Fatalf("expected avg > 75k, got %f for dept %s",
					avgCol.Value(i), deptCol.Value(i))
			}
		}
	})

	// ------------------------------------------------------------
	t.Run("having_no_group_passes", func(t *testing.T) {
		gb := buildDeptAvg()

		having := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("avg_Column(salary)"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(999999)),
		)

		hv, _ := aggr.NewHavingExec(gb, having)
		batch, _ := hv.Next(100)

		if batch.RowCount != 0 {
			t.Fatalf("expected empty result")
		}
	})

	// ------------------------------------------------------------
	t.Run("having_everything_passes", func(t *testing.T) {
		gb := buildDeptAvg()

		having := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("avg_Column(salary)"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(0)),
		)

		hv, _ := aggr.NewHavingExec(gb, having)
		batch, _ := hv.Next(1000)

		if batch.RowCount == 0 {
			t.Fatalf("expected some rows")
		}
	})
}

/*
============================================================================
Distinct tests
============================================================================
*/
func TestDistinctExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Utility: load dataset
	names, cols := generateIntegrationDataset1(mem)
	src, err := project.NewInMemoryProjectExecFromArrays(names, cols)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}

	// -------------------------------
	// 1) DISTINCT on department
	// -------------------------------
	t.Run("distinct_department", func(t *testing.T) {
		expr := Expr.NewExpressions(
			Expr.NewColumnResolve("department"),
		)

		de, err := filter.NewDistinctExec(src, expr)
		if err != nil {
			t.Fatalf("distinct init failed: %v", err)
		}

		batch, err := de.Next(100)
		if err != nil {
			t.Fatalf("distinct next failed: %v", err)
		}

		//deptArr := batch.Columns[5].(*array.String)

		// get expected unique departments from original dataset
		origDept := cols[5].(*array.String)
		expected := make(map[string]struct{})
		for i := 0; i < origDept.Len(); i++ {
			if origDept.IsNull(i) {
				expected["NULL"] = struct{}{}
			} else {
				expected[origDept.Value(i)] = struct{}{}
			}
		}

		if int(batch.RowCount) != len(expected) {
			t.Fatalf("expected %d distinct departments, got %d",
				len(expected), batch.RowCount)
		}
	})

	// -------------------------------
	// 2) DISTINCT on region
	// -------------------------------
	t.Run("distinct_region", func(t *testing.T) {
		// reload source (distinct consumes input)
		src2, _ := project.NewInMemoryProjectExecFromArrays(names, cols)

		expr := Expr.NewExpressions(
			Expr.NewColumnResolve("region"),
		)

		de, err := filter.NewDistinctExec(src2, expr)
		if err != nil {
			t.Fatalf("distinct init failed: %v", err)
		}

		batch, err := de.Next(100)
		if err != nil {
			t.Fatalf("distinct next failed: %v", err)
		}

		regionArr := batch.Columns[6].(*array.String)

		orig := cols[6].(*array.String)
		expected := make(map[string]struct{})
		for i := 0; i < orig.Len(); i++ {
			if orig.IsNull(i) {
				expected["NULL"] = struct{}{}
			} else {
				expected[orig.Value(i)] = struct{}{}
			}
		}

		if int(regionArr.Len()) != len(expected) {
			t.Fatalf("expected %d distinct regions, got %d",
				len(expected), regionArr.Len())
		}
	})

	// -------------------------------
	// 3) DISTINCT(id) → should return all 20 rows
	// -------------------------------
	t.Run("distinct_id_all_unique", func(t *testing.T) {
		src3, _ := project.NewInMemoryProjectExecFromArrays(names, cols)

		expr := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
		)

		de, err := filter.NewDistinctExec(src3, expr)
		if err != nil {
			t.Fatalf("distinct init failed: %v", err)
		}

		batch, err := de.Next(100)
		if err != nil {
			t.Fatalf("distinct next failed: %v", err)
		}

		if batch.RowCount != 20 {
			t.Fatalf("expected 20 distinct id rows, got %d", batch.RowCount)
		}
	})
}

/*
============================================================================
Limit tests
============================================================================
*/
func TestLimitExec(t *testing.T) {
	mem := memory.NewGoAllocator()
	names, cols := generateIntegrationDataset1(mem)

	// ----------------------------------
	// 1) LIMIT 5
	// ----------------------------------
	t.Run("limit_5", func(t *testing.T) {
		src, _ := project.NewInMemoryProjectExecFromArrays(names, cols)

		lim, err := filter.NewLimitExec(src, 5)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil {
			t.Fatalf("limit next error: %v", err)
		}

		if batch.RowCount != 5 {
			t.Fatalf("expected 5 rows, got %d", batch.RowCount)
		}

		// verify first 5 IDs match original dataset
		idArr := batch.Columns[0].(*array.Int32)
		origID := cols[0].(*array.Int32)

		for i := 0; i < 5; i++ {
			if idArr.Value(i) != origID.Value(i) {
				t.Fatalf("row %d: expected id=%d, got id=%d",
					i, origID.Value(i), idArr.Value(i))
			}
		}
	})

	// ----------------------------------
	// 2) LIMIT EXACT = 20
	// ----------------------------------
	t.Run("limit_exact", func(t *testing.T) {
		src, _ := project.NewInMemoryProjectExecFromArrays(names, cols)

		lim, err := filter.NewLimitExec(src, 20)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil {
			t.Fatalf("limit error: %v", err)
		}

		if batch.RowCount != 20 {
			t.Fatalf("expected 20 rows, got %d", batch.RowCount)
		}
	})

	// ----------------------------------
	// 3) LIMIT larger than dataset
	// ----------------------------------
	t.Run("limit_too_large", func(t *testing.T) {
		src, _ := project.NewInMemoryProjectExecFromArrays(names, cols)

		lim, err := filter.NewLimitExec(src, 50)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil {
			t.Fatalf("limit next failed: %v", err)
		}

		if batch.RowCount != 20 {
			t.Fatalf("expected 20 rows when limit > dataset size, got %d", batch.RowCount)
		}
	})
}

/*
============================================================================
Scalar function tests
============================================================================
*/
func TestScalarStringFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()

	// We will run: SELECT department, UPPER(department), LOWER(department)
	// Using ScalarFunction(Upper, col("department"))
	// And ScalarFunction(Lower, col("department"))

	t.Run("UpperFunction", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)
		colDept := Expr.NewColumnResolve("department")

		upperExpr := Expr.NewScalarFunction(Expr.Upper, colDept)

		// Evaluate: UPPER(department)
		batch, err := src.Next(100)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		arr, err := Expr.EvalScalarFunction(upperExpr, batch)
		if err != nil {
			t.Fatalf("upper eval failed: %v", err)
		}

		out := arr.(*array.String)

		// Compare with strings.ToUpper
		deptCol, _ := Expr.EvalExpression(colDept, batch)
		deptArr := deptCol.(*array.String)

		for i := 0; i < int(out.Len()); i++ {
			if deptArr.IsNull(i) {
				if !out.IsNull(i) {
					t.Fatalf("expected null at %d", i)
				}
				continue
			}
			expected := strings.ToUpper(deptArr.Value(i))
			if out.Value(i) != expected {
				t.Fatalf("UPPER mismatch at row %d: got %s, expected %s",
					i, out.Value(i), expected)
			}
		}
	})

	t.Run("LowerFunction", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)
		colDept := Expr.NewColumnResolve("department")

		lowerExpr := Expr.NewScalarFunction(Expr.Lower, colDept)

		// Evaluate: LOWER(department)
		batch, err := src.Next(100)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		arr, err := Expr.EvalScalarFunction(lowerExpr, batch)
		if err != nil {
			t.Fatalf("lower eval failed: %v", err)
		}

		out := arr.(*array.String)

		deptCol, _ := Expr.EvalExpression(colDept, batch)
		deptArr := deptCol.(*array.String)

		for i := 0; i < int(out.Len()); i++ {
			if deptArr.IsNull(i) {
				if !out.IsNull(i) {
					t.Fatalf("expected null at %d", i)
				}
				continue
			}
			expected := strings.ToLower(deptArr.Value(i))
			if out.Value(i) != expected {
				t.Fatalf("LOWER mismatch at row %d: got %s, expected %s",
					i, out.Value(i), expected)
			}
		}
	})
	t.Run("Abs", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)

		fn := Expr.NewScalarFunction(Expr.Abs, Expr.NewColumnResolve("salary"))
		exec, err := project.NewProjectExec(src, []Expr.Expression{fn})
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := exec.Next(50)
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}

		out := batch.Columns[0].(*array.Float64)

		for i := 0; i < out.Len(); i++ {
			val := out.Value(i)
			if val < 0 {
				t.Fatalf("abs result should never be negative, got %v", val)
			}
		}
	})

	// ─────────────────────────────────────────────
	// ROUND(salary)
	// ─────────────────────────────────────────────
	t.Run("Round", func(t *testing.T) {
		src, _ := NewIntegrationSource1(mem)
		_, col := generateIntegrationDataset1(mem)

		fn := Expr.NewScalarFunction(Expr.Round, Expr.NewColumnResolve("salary"))
		exec, err := project.NewProjectExec(src, []Expr.Expression{fn})
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := exec.Next(50)
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}

		out := batch.Columns[0].(*array.Float64)
		orig := col[4].(*array.Float64) // salary column

		for i := 0; i < out.Len(); i++ {
			expected := math.Round(orig.Value(i))
			got := out.Value(i)

			if expected != got {
				t.Fatalf("round mismatch at %d: expected=%v got=%v", i, expected, got)
			}
		}
	})
}

/*
============================================================================
Hash join tests
============================================================================
*/
func TestHashJoinExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("InnerJoin_SimpleDept", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("department")},
			[]Expr.Expression{Expr.NewColumnResolve("department")},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("inner join init failed: %v", err)
		}

		batch, err := j.Next(1000)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		if batch.RowCount == 0 {
			t.Fatalf("inner join returned zero rows (expected matches)")
		}
	})

	t.Run("LeftJoin_AllLeftPreserved", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("region")},
			[]Expr.Expression{Expr.NewColumnResolve("region")},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.LeftJoin, nil)
		if err != nil {
			t.Fatalf("left join init failed: %v", err)
		}

		batch, err := j.Next(1000)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		if batch.RowCount < 20 {
			t.Fatalf("left join should preserve all 20 left rows, got %d", batch.RowCount)
		}
	})

	t.Run("RightJoin_AllRightPreserved", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("region")},
			[]Expr.Expression{Expr.NewColumnResolve("region")},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.RightJoin, nil)
		if err != nil {
			t.Fatalf("right join init failed: %v", err)
		}

		batch, err := j.Next(1000)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		if batch.RowCount < 20 {
			t.Fatalf("right join should preserve all 20 right rows, got %d", batch.RowCount)
		}
	})

	t.Run("InnerJoin_NoMatches", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		// Join on unrelated keys → expect zero matches
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("age")},
			[]Expr.Expression{Expr.NewColumnResolve("dept_id")},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("inner join init failed: %v", err)
		}

		batch, err := j.Next(1000)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		if batch.RowCount != 0 {
			t.Fatalf("expected zero matches, got %d", batch.RowCount)
		}
	})

	t.Run("MultiColumnJoin", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		clause := join.NewJoinClause(
			[]Expr.Expression{
				Expr.NewColumnResolve("department"),
				Expr.NewColumnResolve("region"),
			},
			[]Expr.Expression{
				Expr.NewColumnResolve("department"),
				Expr.NewColumnResolve("region"),
			},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("multi-col join init failed: %v", err)
		}

		batch, err := j.Next(1000)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		if batch.RowCount == 0 {
			t.Fatalf("multi-column join should match some rows")
		}
	})

	t.Run("InnerJoin_CheckSchemaPrefixed", func(t *testing.T) {
		src1, _ := NewIntegrationSource1(mem)
		src2, _ := NewIntegrationSource2(mem)

		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("department")},
			[]Expr.Expression{Expr.NewColumnResolve("department")},
		)

		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}

		schema := j.Schema()

		// Check prefixing (department exists on both sides)
		foundLeft := false
		foundRight := false

		for _, f := range schema.Fields() {
			if f.Name == "left_department" {
				foundLeft = true
			}
			if f.Name == "right_department" {
				foundRight = true
			}
		}

		if !foundLeft || !foundRight {
			t.Fatalf("schema prefixing failed: left_department=%v right_department=%v", foundLeft, foundRight)
		}
	})
}
