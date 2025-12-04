package test

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
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
func runAll(t *testing.T, op operators.Operator) *operators.RecordBatch {
	t.Helper()

	b, err := op.Next(1000)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		t.Fatalf("unexpected err from Next: %v", err)
	}
	return b
}

/*
============================================================================
Project tests
============================================================================
*/
func TestIntegrationProjectExec(t *testing.T) {
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
func TestIntegrationFilterExec(t *testing.T) {
	mem := memory.NewGoAllocator()

	// ----- load dataset -----

	// convenience handles to original cols for expected-value validation
	//ageArr := cols[3].(*array.Int32)
	//salaryArr := cols[4].(*array.Float64)
	//deptArr := cols[5].(*array.String)
	//regionArr := cols[6].(*array.String)

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
