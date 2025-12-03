package join

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"opti-sql-go/operators/project"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func generateDataset1WithNulls(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"id", "name", "age", "salary"}

	// ----- id (int32) -----
	idB := array.NewInt32Builder(mem)
	idVals := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	idValid := []bool{
		true, true, false, true, true,
		false, true, true, true, false,
	}
	idB.AppendValues(idVals, idValid)
	idArr := idB.NewArray()

	// ----- name (string) -----
	nameB := array.NewStringBuilder(mem)
	nameVals := []string{
		"Alice", "Bob", "Charlie", "David", "Eve",
		"Frank", "Grace", "Hannah", "Ivy", "Jake",
	}
	nameValid := []bool{
		true, true, true, false, true,
		true, true, true, false, true,
	}
	nameB.AppendValues(nameVals, nameValid)
	nameArr := nameB.NewArray()

	// ----- age (int32) -----
	ageB := array.NewInt32Builder(mem)
	ageVals := []int32{28, 34, 45, 22, 31, 29, 40, 36, 50, 26}
	ageValid := []bool{
		true, false, true, true, true,
		true, false, true, true, true,
	}
	ageB.AppendValues(ageVals, ageValid)
	ageArr := ageB.NewArray()

	// ----- salary (float64) -----
	salB := array.NewFloat64Builder(mem)
	salVals := []float64{
		70000, 82000, 54000, 91000, 60000,
		75000, 66000, 0, 45000, 99000,
	}
	salaryValid := []bool{
		true, true, true, true, true,
		true, true, false, true, true,
	}
	salB.AppendValues(salVals, salaryValid)
	salaryArr := salB.NewArray()

	return names, []arrow.Array{idArr, nameArr, ageArr, salaryArr}
}
func generateJoinDataset2(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"id", "department", "region"}

	// ---- id (int32) ----
	// overlap on: 1,2,4,5
	// unique to dataset2: 11,12,13,14
	// and one null
	idB := array.NewInt32Builder(mem)
	idB.AppendValues(
		[]int32{1, 2, 4, 5, 11, 12, 13, 14, 3, 0},
		[]bool{true, true, true, true, true, true, true, true, false, false}, // null at idx 8 and 9
	)
	idArr := idB.NewArray()

	// ---- department (string) ----
	deptB := array.NewStringBuilder(mem)
	deptB.AppendValues(
		[]string{"HR", "Engineering", "Sales", "Finance", "Marketing",
			"Support", "Research", "Security", "Unknown", "Unknown"},
		[]bool{true, true, true, true, true, true, true, false, true, false}, // some nulls
	)
	deptArr := deptB.NewArray()

	// ---- region (string) ----
	regionB := array.NewStringBuilder(mem)
	regionB.AppendValues(
		[]string{"US", "EU", "EU", "APAC", "US", "US", "LATAM", "EU", "N/A", "N/A"},
		[]bool{true, true, true, true, true, true, false, true, true, false},
	)
	regionArr := regionB.NewArray()

	return names, []arrow.Array{idArr, deptArr, regionArr}
}
func newSources() (*project.InMemorySource, *project.InMemorySource) {
	mem := memory.NewGoAllocator()
	leftNames, leftCols := generateDataset1WithNulls(mem)
	rightNames, rightCols := generateJoinDataset2(mem)

	leftSource, _ := project.NewInMemoryProjectExecFromArrays(leftNames, leftCols)
	rightSource, _ := project.NewInMemoryProjectExecFromArrays(rightNames, rightCols)
	return leftSource, rightSource
}

func TestJoinSchemas(t *testing.T) {

	makeField := func(name string, dt arrow.DataType) arrow.Field {
		return arrow.Field{Name: name, Type: dt, Nullable: true}
	}

	tests := []struct {
		name       string
		left       *arrow.Schema
		right      *arrow.Schema
		wantFields []string
	}{
		{
			name: "No duplicate fields",
			left: arrow.NewSchema([]arrow.Field{
				makeField("id", arrow.PrimitiveTypes.Int32),
				makeField("name", arrow.BinaryTypes.String),
			}, nil),
			right: arrow.NewSchema([]arrow.Field{
				makeField("dept", arrow.BinaryTypes.String),
				makeField("region", arrow.BinaryTypes.String),
			}, nil),
			wantFields: []string{"id", "name", "dept", "region"},
		},
		{
			name: "Single duplicate (id)",
			left: arrow.NewSchema([]arrow.Field{
				makeField("id", arrow.PrimitiveTypes.Int32),
				makeField("name", arrow.BinaryTypes.String),
				makeField("age", arrow.PrimitiveTypes.Int32),
			}, nil),
			right: arrow.NewSchema([]arrow.Field{
				makeField("id", arrow.PrimitiveTypes.Int32),
				makeField("dept", arrow.BinaryTypes.String),
			}, nil),
			wantFields: []string{"left_id", "name", "age", "right_id", "dept"},
		},
		{
			name: "Multiple duplicates",
			left: arrow.NewSchema([]arrow.Field{
				makeField("id", arrow.PrimitiveTypes.Int32),
				makeField("name", arrow.BinaryTypes.String),
			}, nil),
			right: arrow.NewSchema([]arrow.Field{
				makeField("id", arrow.PrimitiveTypes.Int32),
				makeField("name", arrow.BinaryTypes.String),
				makeField("salary", arrow.PrimitiveTypes.Float64),
			}, nil),
			wantFields: []string{"left_id", "left_name", "right_id", "right_name", "salary"},
		},
		{
			name: "Nullable metadata preserved",
			left: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			}, nil),
			right: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			}, nil),
			wantFields: []string{"left_id", "right_id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := joinSchemas(tt.left, tt.right)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.NumFields() != len(tt.wantFields) {
				t.Fatalf("wrong number of fields: got %d want %d",
					got.NumFields(), len(tt.wantFields))
			}

			for i, wantName := range tt.wantFields {
				gotName := got.Field(i).Name
				if gotName != wantName {
					t.Fatalf("field %d mismatch: got %s want %s", i, gotName, wantName)
				}
			}
		})
	}
}

func TestHashJoin1(t *testing.T) {
	t.Run("playground", func(t *testing.T) {
		left, right := newSources()
		joinPred := NewJoinClause(Expr.NewExpressions(Expr.NewColumnResolve("id")), Expr.NewExpressions(Expr.NewColumnResolve("id")))
		smjExec, err := NewHashJoinExec(left, right, joinPred, InnerJoin, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v, _ := smjExec.Next(5)
		t.Logf("expected schema:\t\n%v\n\n", smjExec.Schema())
		t.Logf("recieved schema:\t\n%v\n\n", v.Schema)
		t.Logf("\t\n\n\t%+v\n", v.PrettyPrint())

	})
}

// collectAllRows drains an operator into a slice of *operators.RecordBatch.
func collectAllRows(t *testing.T, op operators.Operator) []*operators.RecordBatch {
	t.Helper()

	var batches []*operators.RecordBatch
	for {
		b, err := op.Next(1024)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error from Next: %v", err)
		}
		if b == nil || b.RowCount == 0 {
			continue
		}
		batches = append(batches, b)
	}
	return batches
}

// flattenRowCount sums total rows across all batches.
func flattenRowCount(batches []*operators.RecordBatch) int {
	total := 0
	for _, b := range batches {
		total += int(b.RowCount)
	}
	return total
}

// evalInt32Slice evaluates an expression to an Int32 array and returns values + validity bitmap.
func evalInt32Slice(t *testing.T, expr Expr.Expression, batch *operators.RecordBatch) ([]int32, []bool) {
	t.Helper()

	arr, err := Expr.EvalExpression(expr, batch)
	if err != nil {
		t.Fatalf("EvalExpression failed: %v", err)
	}
	defer arr.Release()

	intArr, ok := arr.(*array.Int32)
	if !ok {
		t.Fatalf("expected Int32 array, got %T", arr)
	}

	n := intArr.Len()
	values := make([]int32, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if intArr.IsNull(i) {
			valid[i] = false
			continue
		}
		valid[i] = true
		values[i] = intArr.Value(i)
	}
	return values, valid
}

//
// Simple helpers to get your left/right sources for the id-join dataset.
//

//
// Multi-attribute dataset: first_name + last_name join.
//

func generateMultiAttrLeft(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"first_name", "last_name", "emp_id"}

	fnB := array.NewStringBuilder(mem)
	fnB.AppendValues(
		[]string{"Alice", "Bob", "Charlie", "Diana"},
		[]bool{true, true, true, true},
	)
	fnArr := fnB.NewArray()

	lnB := array.NewStringBuilder(mem)
	lnB.AppendValues(
		[]string{"Smith", "Jones", "Stone", "Lopez"},
		[]bool{true, true, true, true},
	)
	lnArr := lnB.NewArray()

	empB := array.NewInt32Builder(mem)
	empB.AppendValues(
		[]int32{1, 2, 3, 4},
		[]bool{true, true, true, true},
	)
	empArr := empB.NewArray()

	return names, []arrow.Array{fnArr, lnArr, empArr}
}

func generateMultiAttrRight(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"first_name", "last_name", "department"}

	fnB := array.NewStringBuilder(mem)
	fnB.AppendValues(
		[]string{"Alice", "Charlie", "Evan"},
		[]bool{true, true, true},
	)
	fnArr := fnB.NewArray()

	lnB := array.NewStringBuilder(mem)
	lnB.AppendValues(
		[]string{"Smith", "Stone", "Miller"},
		[]bool{true, true, true},
	)
	lnArr := lnB.NewArray()

	deptB := array.NewStringBuilder(mem)
	deptB.AppendValues(
		[]string{"HR", "Engineering", "Sales"},
		[]bool{true, true, true},
	)
	deptArr := deptB.NewArray()

	return names, []arrow.Array{fnArr, lnArr, deptArr}
}

//
// "Computed" key dataset: we simulate a computed join key by precomputing a normalized field.
//

func generateEmailLeft(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"id", "email_lower"}

	idB := array.NewInt32Builder(mem)
	idB.AppendValues([]int32{1, 2, 3}, []bool{true, true, true})
	idArr := idB.NewArray()

	emailB := array.NewStringBuilder(mem)
	emailB.AppendValues(
		[]string{"alice@example.com", "bob@example.com", "charlie@example.com"},
		[]bool{true, true, true},
	)
	emailArr := emailB.NewArray()

	return names, []arrow.Array{idArr, emailArr}
}

func generateEmailRight(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"id", "email_lower", "group"}

	idB := array.NewInt32Builder(mem)
	idB.AppendValues([]int32{10, 20, 30}, []bool{true, true, true})
	idArr := idB.NewArray()

	emailB := array.NewStringBuilder(mem)
	emailB.AppendValues(
		[]string{"alice@example.com", "notused@example.com", "charlie@example.com"},
		[]bool{true, true, true},
	)
	emailArr := emailB.NewArray()

	groupB := array.NewStringBuilder(mem)
	groupB.AppendValues([]string{"A", "B", "C"}, []bool{true, true, true})
	groupArr := groupB.NewArray()

	return names, []arrow.Array{idArr, emailArr, groupArr}
}

//
// ---------- (1) Simple id join tests ----------
//

func TestHashJoin_OnSimpleKey(t *testing.T) {
	t.Run("inner join on id with SQL NULL semantics", func(t *testing.T) {
		left, right := newSources()

		leftExpr := Expr.NewExpressions(Expr.NewColumnResolve("id"))
		rightExpr := Expr.NewExpressions(Expr.NewColumnResolve("id"))
		clause := NewJoinClause(leftExpr, rightExpr)

		hj, err := NewHashJoinExec(left, right, clause, InnerJoin, nil)
		if err != nil {
			t.Fatalf("NewHashJoinExec failed: %v", err)
		}
		defer func() {
			if err := hj.Close(); err != nil {
				t.Fatalf("HashJoinExec Close failed: %v", err)
			}

		}()

		batches := collectAllRows(t, hj)
		totalRows := flattenRowCount(batches)

		// Overlap on non-NULL ids is: 1, 2, 4, 5 => 4 rows for inner join.
		if totalRows != 4 {
			t.Fatalf("expected 4 joined rows, got %d", totalRows)
		}

		if len(batches) == 0 {
			t.Fatal("expected at least one output batch")
		}
		first := batches[0]

		leftIDExpr := Expr.NewColumnResolve("left_id")
		rightIDExpr := Expr.NewColumnResolve("right_id")

		leftVals, leftValid := evalInt32Slice(t, leftIDExpr, first)
		rightVals, rightValid := evalInt32Slice(t, rightIDExpr, first)

		for i := range leftVals {
			if !leftValid[i] || !rightValid[i] {
				t.Fatalf("unexpected NULL id in joined row %d", i)
			}
			if leftVals[i] != rightVals[i] {
				t.Fatalf("mismatched ids at row %d: left=%d right=%d",
					i, leftVals[i], rightVals[i])
			}
		}
	})

	t.Run("constructor error on mismatched join clause length", func(t *testing.T) {
		left, right := newSources()

		// left has 1 expression, right has 2 → must error
		leftExpr := Expr.NewExpressions(Expr.NewColumnResolve("id"))
		rightExpr := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("department"),
		)

		clause := NewJoinClause(leftExpr, rightExpr)
		_, err := NewHashJoinExec(left, right, clause, InnerJoin, nil)
		if err == nil {
			t.Fatal("expected error due to mismatched join expression counts, got nil")
		}
		if !strings.Contains(err.Error(), "mismatched number of join expressions") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

//
// ---------- (2) Multi-attribute join tests ----------
//

func TestHashJoin_MultiAttributeKey(t *testing.T) {
	mem := memory.NewGoAllocator()
	leftNames, leftCols := generateMultiAttrLeft(mem)
	rightNames, rightCols := generateMultiAttrRight(mem)

	leftSource, _ := project.NewInMemoryProjectExecFromArrays(leftNames, leftCols)
	rightSource, _ := project.NewInMemoryProjectExecFromArrays(rightNames, rightCols)

	leftExprs := Expr.NewExpressions(
		Expr.NewColumnResolve("first_name"),
		Expr.NewColumnResolve("last_name"),
	)
	rightExprs := Expr.NewExpressions(
		Expr.NewColumnResolve("first_name"),
		Expr.NewColumnResolve("last_name"),
	)
	clause := NewJoinClause(leftExprs, rightExprs)

	hj, err := NewHashJoinExec(leftSource, rightSource, clause, InnerJoin, nil)
	if err != nil {
		t.Fatalf("NewHashJoinExec failed: %v", err)
	}

	defer func() {
		if err := hj.Close(); err != nil {
			t.Fatalf("HashJoinExec Close failed: %v", err)
		}

	}()

	batches := collectAllRows(t, hj)
	totalRows := flattenRowCount(batches)

	// Matches: ("Alice","Smith") and ("Charlie","Stone") => 2 rows.
	if totalRows != 2 {
		t.Fatalf("expected 2 rows from multi-attribute join, got %d", totalRows)
	}

	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	first := batches[0]

	deptExpr := Expr.NewColumnResolve("department")
	arr, err := Expr.EvalExpression(deptExpr, first)
	if err != nil {
		t.Fatalf("EvalExpression department failed: %v", err)
	}
	defer arr.Release()

	strArr := arr.(*array.String)
	if strArr.Len() != totalRows {
		t.Fatalf("expected department array len %d, got %d", totalRows, strArr.Len())
	}
	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			t.Fatalf("expected non-null department at row %d", i)
		}
	}
}

//
// ---------- (3) "Computed" key join tests ----------
//

func TestHashJoin_ComputedKeySimulation(t *testing.T) {
	mem := memory.NewGoAllocator()
	leftNames, leftCols := generateEmailLeft(mem)
	rightNames, rightCols := generateEmailRight(mem)

	leftSource, _ := project.NewInMemoryProjectExecFromArrays(leftNames, leftCols)
	rightSource, _ := project.NewInMemoryProjectExecFromArrays(rightNames, rightCols)

	leftExprs := Expr.NewExpressions(Expr.NewColumnResolve("email_lower"))
	rightExprs := Expr.NewExpressions(Expr.NewColumnResolve("email_lower"))
	clause := NewJoinClause(leftExprs, rightExprs)

	hj, err := NewHashJoinExec(leftSource, rightSource, clause, InnerJoin, nil)
	if err != nil {
		t.Fatalf("NewHashJoinExec failed: %v", err)
	}

	defer func() {
		if err := hj.Close(); err != nil {
			t.Fatalf("HashJoinExec Close failed: %v", err)
		}

	}()
	batches := collectAllRows(t, hj)
	totalRows := flattenRowCount(batches)

	// Overlap on email_lower: alice + charlie => 2 rows.
	if totalRows != 2 {
		t.Fatalf("expected 2 joined rows on email_lower, got %d", totalRows)
	}

	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	first := batches[0]

	leftEmailExpr := Expr.NewColumnResolve("left_email_lower")
	rightEmailExpr := Expr.NewColumnResolve("right_email_lower")

	leftArr, err := Expr.EvalExpression(leftEmailExpr, first)
	if err != nil {
		t.Fatalf("EvalExpression left_email_lower failed: %v", err)
	}
	defer leftArr.Release()

	rightArr, err := Expr.EvalExpression(rightEmailExpr, first)
	if err != nil {
		t.Fatalf("EvalExpression right_email_lower failed: %v", err)
	}
	defer rightArr.Release()

	lStr := leftArr.(*array.String)
	rStr := rightArr.(*array.String)

	if lStr.Len() != rStr.Len() {
		t.Fatalf("expected same length for left/right email arrays, got %d vs %d",
			lStr.Len(), rStr.Len())
	}
	for i := 0; i < lStr.Len(); i++ {
		if lStr.IsNull(i) || rStr.IsNull(i) {
			t.Fatalf("unexpected NULL email at row %d", i)
		}
	}
}
