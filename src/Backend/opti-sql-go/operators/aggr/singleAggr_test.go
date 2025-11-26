package aggr

import (
	"errors"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators/project"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func generateAggTestColumns() ([]string, []any) {
	names := []string{
		"id",
		"name",
		"age",
		"salary",
	}

	columns := []any{
		// id: 1 to 25
		[]int32{
			1, 2, 3, 4, 5,
			6, 7, 8, 9, 10,
			11, 12, 13, 14, 15,
			16, 17, 18, 19, 20,
			21, 22, 23, 24, 25,
		},

		// name: 25 people
		[]string{
			"Alice", "Bob", "Charlie", "David", "Eve",
			"Frank", "Grace", "Hannah", "Ivy", "Jake",
			"Karen", "Leo", "Mona", "Nate", "Olive",
			"Paul", "Quinn", "Rita", "Sam", "Tina",
			"Uma", "Victor", "Wendy", "Xavier", "Yara",
		},

		// age: 25 numeric values
		[]int32{
			28, 34, 45, 22, 31,
			29, 40, 36, 50, 26,
			33, 41, 27, 38, 24,
			46, 30, 35, 43, 32,
			39, 48, 29, 37, 42,
		},

		// salary: 25 numeric values
		[]float64{
			70000.0, 82000.5, 54000.0, 91000.0, 60000.0,
			75000.0, 66000.0, 88000.0, 45000.0, 99000.0,
			72000.0, 81000.0, 53000.0, 86000.0, 64000.0,
			93000.0, 68000.0, 76000.0, 89000.0, 71000.0,
			83000.0, 94000.0, 55000.0, 87000.0, 91500.0,
		},
	}

	return names, columns
}
func generateAggTestColumnsWithNulls(mem memory.Allocator) ([]string, []arrow.Array) {
	names := []string{"id", "name", "age", "salary"}

	// -------------------------
	// id column (int32)
	// -------------------------
	idB := array.NewInt32Builder(mem)
	idVals := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	idValid := []bool{
		true, true, false, true, true,
		false, true, true, true, false,
	}
	idB.AppendValues(idVals, idValid)
	idArr := idB.NewArray()

	// -------------------------
	// name column (string)
	// -------------------------
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

	// -------------------------
	// age column (int32)
	// -------------------------
	ageB := array.NewInt32Builder(mem)
	ageVals := []int32{28, 34, 45, 22, 31, 29, 40, 36, 50, 26}
	ageValid := []bool{
		true, false, true, true, true,
		true, false, true, true, true,
	}
	ageB.AppendValues(ageVals, ageValid)
	ageArr := ageB.NewArray()

	// -------------------------
	// salary column (float64)
	// -------------------------
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

func aggProject() *project.InMemorySource {
	names, cols := generateAggTestColumns()
	p, _ := project.NewInMemoryProjectExec(names, cols)
	return p
}

func aggProjectNull() *project.InMemorySource {
	names, arr := generateAggTestColumnsWithNulls(memory.NewGoAllocator())
	p, _ := project.NewInMemoryProjectExecFromArrays(names, arr)
	return p
}

func col(name string) Expr.Expression {
	return Expr.NewColumnResolve(name)
}

func TestNewAggrExec(t *testing.T) {

	// -----------------------------------------------------------------
	t.Run("valid_single_min", func(t *testing.T) {
		child := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("age")},
		}

		exec, err := NewGlobalAggrExec(child, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if exec.Schema().NumFields() != 1 {
			t.Fatalf("expected 1 schema field, got %d", exec.Schema().NumFields())
		}

		expectedName := "min_Column(age)"
		if exec.Schema().Field(0).Name != expectedName {
			t.Fatalf("expected name %s, got %s",
				expectedName, exec.Schema().Field(0).Name)
		}
	})

	// -----------------------------------------------------------------
	t.Run("multiple_aggregations_schema_names", func(t *testing.T) {
		child := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("id")},
			{AggrFunc: Max, Child: col("salary")},
			{AggrFunc: Avg, Child: col("age")},
		}

		exec, err := NewGlobalAggrExec(child, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		schema := exec.Schema()

		expected := []string{
			"min_Column(id)",
			"max_Column(salary)",
			"avg_Column(age)",
		}

		for i, f := range schema.Fields() {
			if f.Name != expected[i] {
				t.Fatalf("expected field %s, got %s", expected[i], f.Name)
			}
		}
	})

	// -----------------------------------------------------------------
	t.Run("invalid_type_detection_string_column", func(t *testing.T) {
		child := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("name")}, // "name" is string → invalid
		}

		_, err := NewGlobalAggrExec(child, agg)
		if err == nil {
			t.Fatalf("expected type error, got nil")
		}
		t.Logf("================\n invalid column err %v \n ============", err)
	})

	// -----------------------------------------------------------------
	t.Run("unsupported_aggregate_function", func(t *testing.T) {
		child := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: 9999, Child: col("age")},
		}

		_, err := NewGlobalAggrExec(child, agg)
		if err == nil {
			t.Fatalf("expected unsupported aggr error")
		}
	})

	// -----------------------------------------------------------------
	t.Run("schema_type_float64_for_all_numeric_aggs", func(t *testing.T) {
		child := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("id")},
			{AggrFunc: Max, Child: col("salary")},
			{AggrFunc: Sum, Child: col("age")},
			{AggrFunc: Avg, Child: col("salary")},
			{AggrFunc: Count, Child: col("age")},
		}

		exec, err := NewGlobalAggrExec(child, agg)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		for _, f := range exec.Schema().Fields() {
			if f.Type.ID() != arrow.FLOAT64 {
				t.Fatalf("expected float64 output type, got %s", f.Type)
			}
		}
		if err := exec.Close(); err != nil {
			t.Fatalf("unexpected close error: %v", err)
		}
	})

	// -----------------------------------------------------------------
	t.Run("check_all_valid_numeric_types_pass", func(t *testing.T) {

		// all numeric arrow types accepted by validAggrType()
		validTypes := []arrow.DataType{
			arrow.PrimitiveTypes.Uint8,
			arrow.PrimitiveTypes.Uint16,
			arrow.PrimitiveTypes.Uint32,
			arrow.PrimitiveTypes.Uint64,
			arrow.PrimitiveTypes.Int8,
			arrow.PrimitiveTypes.Int16,
			arrow.PrimitiveTypes.Int32,
			arrow.PrimitiveTypes.Int64,
			arrow.PrimitiveTypes.Float32,
			arrow.PrimitiveTypes.Float64,
		}

		fieldNames := make([]string, len(validTypes))
		colData := make([]any, len(validTypes))

		for i, dt := range validTypes {
			name := fmt.Sprintf("col_%d", i)
			fieldNames[i] = name

			switch dt.ID() {
			case arrow.UINT8:
				colData[i] = []uint8{1}
			case arrow.UINT16:
				colData[i] = []uint16{1}
			case arrow.UINT32:
				colData[i] = []uint32{1}
			case arrow.UINT64:
				colData[i] = []uint64{1}
			case arrow.INT8:
				colData[i] = []int8{1}
			case arrow.INT16:
				colData[i] = []int16{1}
			case arrow.INT32:
				colData[i] = []int32{1}
			case arrow.INT64:
				colData[i] = []int64{1}
			case arrow.FLOAT16:
				// float16 stored as float32 in Go
				colData[i] = []float32{1}
			case arrow.FLOAT32:
				colData[i] = []float32{1}
			case arrow.FLOAT64:
				colData[i] = []float64{1}
			}
		}

		src, _ := project.NewInMemoryProjectExec(fieldNames, colData)

		for i := range fieldNames {
			agg := []AggregateFunctions{
				{AggrFunc: Sum, Child: col(fieldNames[i])},
			}

			_, err := NewGlobalAggrExec(src, agg)
			if err != nil {
				t.Fatalf("unexpected error for type %s: %v", validTypes[i], err)
			}
		}
	})
}

func TestCastArrayToFloat64(t *testing.T) {

	alloc := memory.NewGoAllocator

	// --------------------------------------------------------
	t.Run("cast_int32_to_float64", func(t *testing.T) {
		b := array.NewInt32Builder(alloc())
		b.AppendValues([]int32{1, 2, 3, 4}, nil)
		arr := b.NewArray()

		out, err := castArrayToFloat64(arr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		farr, ok := out.(*array.Float64)
		if !ok {
			t.Fatalf("expected Float64 array, got %T", out)
		}

		expected := []float64{1, 2, 3, 4}
		for i := range expected {
			if farr.Value(i) != expected[i] {
				t.Fatalf("expected %v at %d, got %v", expected[i], i, farr.Value(i))
			}
		}
	})

	// --------------------------------------------------------
	t.Run("cast_float32_to_float64", func(t *testing.T) {
		b := array.NewFloat32Builder(alloc())
		b.AppendValues([]float32{10.5, 20.5, 30.5}, nil)
		arr := b.NewArray()

		out, err := castArrayToFloat64(arr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		farr, ok := out.(*array.Float64)
		if !ok {
			t.Fatalf("expected Float64 array, got %T", out)
		}

		expected := []float64{10.5, 20.5, 30.5}
		for i := range expected {
			if farr.Value(i) != expected[i] {
				t.Fatalf("expected %v at %d, got %v", expected[i], i, farr.Value(i))
			}
		}
	})

	// --------------------------------------------------------
	t.Run("invalid_string_cast", func(t *testing.T) {
		b := array.NewStringBuilder(alloc())
		b.AppendValues([]string{"a", "b", "c"}, nil)
		arr := b.NewArray()

		_, err := castArrayToFloat64(arr)
		if err == nil {
			t.Fatalf("expected error when casting string array to float64")
		}
	})

	// --------------------------------------------------------
	t.Run("empty_array_cast", func(t *testing.T) {
		b := array.NewInt32Builder(alloc())
		// no values appended
		arr := b.NewArray()

		out, err := castArrayToFloat64(arr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, ok := out.(*array.Float64)
		if !ok {
			t.Fatalf("expected Float64 array for empty cast, got %T", out)
		}

		if out.Len() != 0 {
			t.Fatalf("expected empty array, got length %d", out.Len())
		}
	})

}

func TestAggregateExecNext(t *testing.T) {
	t.Run("validating done case early", func(t *testing.T) {
		proj := aggProject()
		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("id")}}
		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		aggrExec.done = true
		_, err = aggrExec.Next(10)
		if err == nil || !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF error, got nil")
		}
	})
	t.Run("Aggr minimum value on age", func(t *testing.T) {
		proj := aggProject()
		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("age")}}
		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resultBatch, _ := aggrExec.Next(100)
		t.Logf("record batch: %v\n", resultBatch)
		if resultBatch.Columns[0].(*array.Float64).Value(0) != 22 {
			t.Fatalf("expected minimum age 22, got %v", resultBatch.Columns[0].(*array.Float64).Value(0))
		}

	})
	t.Run("Aggr maximum salary", func(t *testing.T) {
		proj := aggProject()
		agg := []AggregateFunctions{
			{AggrFunc: Max, Child: col("salary")},
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		resultBatch, _ := aggrExec.Next(100)

		maxSalary := resultBatch.Columns[0].(*array.Float64).Value(0)
		if maxSalary != 99000.0 && maxSalary != 94000.0 && maxSalary != 93000.0 {
			// Real max is 99000 (Jake has 99000)
			t.Fatalf("expected max salary 99000, got %v", maxSalary)
		}
	})
	t.Run("Aggr sum of id column", func(t *testing.T) {
		proj := aggProject()
		agg := []AggregateFunctions{
			{AggrFunc: Sum, Child: col("id")},
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		resultBatch, _ := aggrExec.Next(200)

		sumIDs := resultBatch.Columns[0].(*array.Float64).Value(0)
		expected := float64((25 * 26) / 2) // sum(1..25) = 325
		if sumIDs != expected {
			t.Fatalf("expected sum 325, got %v", sumIDs)
		}
	})
	t.Run("Aggr count of age column", func(t *testing.T) {
		proj := aggProject()
		agg := []AggregateFunctions{
			NewAggregateFunctions(Count, col("age")),
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		resultBatch, _ := aggrExec.Next(300)

		count := resultBatch.Columns[0].(*array.Float64).Value(0)
		if count != 25 {
			t.Fatalf("expected count 25, got %v", count)
		}
	})
	t.Run("Aggr average of salary ", func(t *testing.T) {
		proj := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Avg, Child: col("salary")},
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		resultBatch, _ := aggrExec.Next(500)

		avg := resultBatch.Columns[0].(*array.Float64).Value(0)
		expected := 75740.02

		if math.Abs(avg-expected) > 0.001 {
			t.Fatalf("expected avg %v, got %v", expected, avg)
		}

	})
	t.Run("Multiple aggregators in a single request", func(t *testing.T) {
		proj := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("age")},
			{AggrFunc: Max, Child: col("salary")},
			{AggrFunc: Count, Child: col("id")},
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		resultBatch, _ := aggrExec.Next(1000)

		minAge := resultBatch.Columns[0].(*array.Float64).Value(0)
		maxSalary := resultBatch.Columns[1].(*array.Float64).Value(0)
		countIDs := resultBatch.Columns[2].(*array.Float64).Value(0)

		if minAge != 22 {
			t.Fatalf("expected min age 22, got %v", minAge)
		}
		if maxSalary != 99000.0 {
			t.Fatalf("expected max salary 99000, got %v", maxSalary)
		}
		if countIDs != 25 {
			t.Fatalf("expected count 25, got %v", countIDs)
		}
	})

	// ==========================================================
	t.Run("Schema correctness for multiple aggregates", func(t *testing.T) {
		proj := aggProject()

		agg := []AggregateFunctions{
			{AggrFunc: Min, Child: col("id")},
			{AggrFunc: Sum, Child: col("age")},
			{AggrFunc: Count, Child: col("salary")},
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		s := aggrExec.Schema()

		expectedNames := []string{
			"min_Column(id)",
			"sum_Column(age)",
			"count_Column(salary)",
		}

		for i, f := range s.Fields() {
			if f.Name != expectedNames[i] {
				t.Fatalf("expected field %s, got %s", expectedNames[i], f.Name)
			}
			if f.Type.ID() != arrow.FLOAT64 {
				t.Fatalf("expected float64 fields only")
			}
		}
	})
}

func TestAggregateExecNull(t *testing.T) {

	t.Run("Aggr count of age column", func(t *testing.T) {
		proj := aggProjectNull()
		agg := []AggregateFunctions{
			NewAggregateFunctions(Count, col("age")),
			NewAggregateFunctions(Sum, col("id")),
		}

		aggrExec, err := NewGlobalAggrExec(proj, agg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resultBatch, _ := aggrExec.Next(100)
		t.Logf("rb:%v\n", resultBatch)
		count := resultBatch.Columns[0].(*array.Float64).Value(0)
		if count != 8 {
			t.Fatalf("expected count 7, got %v", count)
		}
		sumIDs := resultBatch.Columns[1].(*array.Float64).Value(0)
		expectedSum := float64(1 + 2 + 4 + 5 + 7 + 8 + 9) // only non-null ids
		if sumIDs != expectedSum {
			t.Fatalf("expected sum %v, got %v", expectedSum, sumIDs)
		}
	})
}
