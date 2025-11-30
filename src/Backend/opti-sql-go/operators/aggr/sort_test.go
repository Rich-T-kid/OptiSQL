package aggr

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators/project"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/go-jose/go-jose/v4/testutils/require"
)

func TestSortInit(t *testing.T) {
	// Simple passing test
	t.Run("sort Exec init", func(t *testing.T) {
		proj := aggProject()
		sortExec, err := NewSortExec(proj, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !sortExec.Schema().Equal(proj.Schema()) {
			t.Fatalf("expected schema %v, got %v", proj.Schema(), sortExec.schema)
		}
		sortExec.done = true
		_, err = sortExec.Next(100)
		if err != io.EOF {
			t.Fatalf("expected io.EOF error on done sortExec but got %v", err)
		}
		if sortExec.Close() != nil {
			t.Fatalf("expected nil error on close but got %v", sortExec.Close())
		}

	})
	t.Run("SortKey options", func(t *testing.T) {
		proj := aggProject()
		_, err := NewSortExec(proj, []SortKey{*NewSortKey(col("-"), false, false)})
		if err != nil {
			t.Fatal(err)
		}

	})
	t.Run("tok k sort exec init", func(t *testing.T) {
		proj := aggProject()
		topKVal := 5
		topK, err := NewTopKSortExec(proj, nil, uint16(topKVal))
		if err != nil {
			t.Fatal(err)
		}
		if !topK.Schema().Equal(proj.Schema()) {
			t.Fatalf("expected schema %v, got %v", proj.Schema(), topK.schema)
		}
		if topK.k != 5 {
			t.Fatalf("expected %v for top k but got %v", topKVal, topK.k)
		}
		topK.done = true
		_, err = topK.Next(100)
		if err != io.EOF {
			t.Fatalf("expected io.EOF error on done topK but got %v", err)
		}
		if topK.Close() != nil {
			t.Fatalf("expected nil error on close but got %v", topK.Close())
		}

	})
}

func TestBasicSortExpr(t *testing.T) {
	t.Run("Sort", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		_, err := NewSortExec(proj, CombineSortKeys(nameSK, ageSK))
		if err != nil {
			t.Fatalf("unexpected error from NewSortExec : %v\n", err)
		}
		//t.Logf("%v\n", sortExec)
	})
	t.Run("Basic Next operation", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		sortExec, err := NewSortExec(proj, CombineSortKeys(ageSK, nameSK))
		if err != nil {
			t.Fatalf("unexpected error from NewSortExec : %v\n", err)
		}
		for {
			sortedBatch, err := sortExec.Next(5)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("unexpected error from sortExec Next : %v\n", err)
			}
			fmt.Println(sortedBatch.PrettyPrint())
		}
	})
}
func TestFullSortOverNetwork(t *testing.T) {
	t.Run("Full Sort of large file", func(t *testing.T) {
		const fileName = "country_full.csv"
		nr, err := project.NewStreamReader(fileName)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		pj, err := project.NewProjectCSVLeaf(nr.Stream())
		if err != nil {
			t.Fatalf("failed to create csv project source from s3 object: %v", err)
		}
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		sortExec, err := NewSortExec(pj, CombineSortKeys(nameSK))
		if err != nil {
			t.Fatalf("unexpected error %v\n", err)
		}
		rc, err := sortExec.Next(10)
		if err != nil {
			t.Fatalf("unexpected error %v\n", err)
		}
		fmt.Println(rc.PrettyPrint())

	})

}

func TestFullSortExec_Next(t *testing.T) {
	t.Parallel()

	t.Run("sort_age_DESC", func(t *testing.T) {
		proj := aggProject()

		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false) // DESC

		sortExec, err := NewSortExec(proj, CombineSortKeys(ageSK))
		require.NoError(t, err)

		batch, err := sortExec.Next(5)
		require.NoError(t, err)
		require.Equal(t, uint64(5), batch.RowCount)

		ages := batch.Columns[2].(*array.Int32)
		got := []int32{
			ages.Value(0),
			ages.Value(1),
			ages.Value(2),
			ages.Value(3),
			ages.Value(4),
		}

		expected := []int32{50, 48, 46, 45, 43}
		for i, v := range expected {
			if got[i] != v {
				t.Fatalf("expected %v at index %d, but got %v", v, i, got[i])
			}
		}
	})

	t.Run("sort_name_ASC", func(t *testing.T) {
		proj := aggProject()

		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)

		sortExec, err := NewSortExec(proj, CombineSortKeys(nameSK))
		require.NoError(t, err)

		batch, err := sortExec.Next(3)
		require.NoError(t, err)

		names := batch.Columns[1].(*array.String)
		got := []string{
			names.Value(0),
			names.Value(1),
			names.Value(2),
		}

		expected := []string{"Alice", "Bob", "Charlie"}
		for i, v := range expected {
			if got[i] != v {
				t.Fatalf("expected %v at index %d, but got %v", v, i, got[i])
			}
		}
	})
}

// -----------------------------------------------------------------------------
//  TEST 2: sortIndexVector()
// -----------------------------------------------------------------------------

func TestSortIndexVector(t *testing.T) {
	t.Parallel()

	mem := memory.NewGoAllocator()

	t.Run("single_key_int", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		b.AppendValues([]int32{30, 10, 20}, nil)
		arr := b.NewArray()
		defer arr.Release()

		keys := []arrow.Array{arr}
		idVec := []uint64{0, 1, 2}

		sks := []SortKey{
			{Expr: nil, Ascending: true},
		}

		sortIndexVector(idVec, keys, sks)

		expected := []uint64{1, 2, 0}
		for i, v := range expected {
			if idVec[i] != v {
				t.Fatalf("expected %v at index %d, but got %v", v, i, idVec[i])
			}
		}
	})

	t.Run("single_key_string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		b.AppendValues([]string{"Charlie", "Alice", "Bob"}, nil)
		arr := b.NewArray()
		defer arr.Release()

		keys := []arrow.Array{arr}
		idVec := []uint64{0, 1, 2}

		sks := []SortKey{{Ascending: true}}

		sortIndexVector(idVec, keys, sks)

		expected := []uint64{1, 2, 0}
		for i, v := range expected {
			if idVec[i] != v {
				t.Fatalf("expected %v at index %d, but got %v", v, i, idVec[i])
			}
		}
	})
}

// -----------------------------------------------------------------------------
//  TEST 3: compareArrowValues()
// -----------------------------------------------------------------------------

func TestCompareArrowValues(t *testing.T) {
	t.Parallel()

	mem := memory.NewGoAllocator()

	t.Run("int", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		b.AppendValues([]int32{10, 20}, nil)
		arr := b.NewArray()
		defer arr.Release()

		require.Equal(t, -1, compareArrowValues(arr, 0, 1))
		require.Equal(t, 1, compareArrowValues(arr, 1, 0))
		require.Equal(t, 0, compareArrowValues(arr, 0, 0))
	})

	t.Run("uint", func(t *testing.T) {
		b := array.NewUint32Builder(mem)
		b.AppendValues([]uint32{5, 7}, nil)
		arr := b.NewArray()
		defer arr.Release()

		require.Equal(t, -1, compareArrowValues(arr, 0, 1))
		require.Equal(t, 1, compareArrowValues(arr, 1, 0))
	})

	t.Run("float", func(t *testing.T) {
		b := array.NewFloat64Builder(mem)
		b.AppendValues([]float64{1.5, 1.7}, nil)
		arr := b.NewArray()
		defer arr.Release()

		require.Equal(t, -1, compareArrowValues(arr, 0, 1))
		require.Equal(t, 1, compareArrowValues(arr, 1, 0))
	})

	t.Run("string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		b.AppendValues([]string{"a", "b"}, nil)
		arr := b.NewArray()
		defer arr.Release()

		require.Equal(t, -1, compareArrowValues(arr, 0, 1))
		require.Equal(t, 1, compareArrowValues(arr, 1, 0))
	})

	t.Run("bool", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		b.AppendValues([]bool{false, true}, nil)
		arr := b.NewArray()
		defer arr.Release()

		require.Equal(t, -1, compareArrowValues(arr, 0, 1))
		require.Equal(t, 1, compareArrowValues(arr, 1, 0))
	})
}
func TestCompareArrowValues_AllTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	// helper to assert cmp result
	assert := func(name string, got, want int) {
		if got != want {
			t.Fatalf("%s: expected %d, got %d", name, want, got)
		}
	}

	// ---- STRING ----
	strB := array.NewStringBuilder(mem)
	strB.Append("apple")
	strB.Append("banana")
	strArr := strB.NewArray().(*array.String)

	assert("string lt", compareArrowValues(strArr, 0, 1), -1)
	assert("string gt", compareArrowValues(strArr, 1, 0), 1)
	assert("string eq", compareArrowValues(strArr, 0, 0), 0)

	strArr.Release()
	strB.Release()

	// ---- INT TYPES ----
	int8Arr := buildInt8(mem, []int8{1, 3})
	assert("int8 lt", compareArrowValues(int8Arr, 0, 1), -1)
	assert("int8 gt", compareArrowValues(int8Arr, 1, 0), 1)
	assert("int8 eq", compareArrowValues(int8Arr, 0, 0), 0)
	int8Arr.Release()

	int16Arr := buildInt16(mem, []int16{5, 2})
	assert("int16 gt", compareArrowValues(int16Arr, 0, 1), 1)
	int16Arr.Release()

	int32Arr := buildInt32(mem, []int32{10, 10})
	assert("int32 eq", compareArrowValues(int32Arr, 0, 1), 0)
	int32Arr.Release()

	int64Arr := buildInt64(mem, []int64{-5, 7})
	assert("int64 lt", compareArrowValues(int64Arr, 0, 1), -1)
	int64Arr.Release()

	// ---- UINT TYPES ----
	u8Arr := buildUint8(mem, []uint8{9, 3})
	assert("uint8 gt", compareArrowValues(u8Arr, 0, 1), 1)
	u8Arr.Release()

	u16Arr := buildUint16(mem, []uint16{3, 3})
	assert("uint16 eq", compareArrowValues(u16Arr, 0, 1), 0)
	u16Arr.Release()

	u32Arr := buildUint32(mem, []uint32{3, 10})
	assert("uint32 lt", compareArrowValues(u32Arr, 0, 1), -1)
	u32Arr.Release()

	u64Arr := buildUint64(mem, []uint64{100, 2})
	assert("uint64 gt", compareArrowValues(u64Arr, 0, 1), 1)
	u64Arr.Release()

	// ---- FLOAT TYPES ----
	f32Arr := buildFloat32(mem, []float32{1.5, 1.5})
	assert("float32 eq", compareArrowValues(f32Arr, 0, 1), 0)
	f32Arr.Release()

	f64Arr := buildFloat64(mem, []float64{-1.0, 2.3})
	assert("float64 lt", compareArrowValues(f64Arr, 0, 1), -1)
	f64Arr.Release()

	// ---- BOOLEAN ----
	boolArr := buildBool(mem, []bool{false, true})
	assert("bool lt", compareArrowValues(boolArr, 0, 1), -1)
	assert("bool gt", compareArrowValues(boolArr, 1, 0), 1)
	assert("bool eq", compareArrowValues(boolArr, 1, 1), 0)
	boolArr.Release()

	// ---- NULL CASES ----
	nullB := array.NewInt32Builder(mem)
	nullB.AppendNull()
	nullB.Append(10)
	nullArr := nullB.NewArray().(*array.Int32)

	assert("null < value", compareArrowValues(nullArr, 0, 1), -1)
	assert("value > null", compareArrowValues(nullArr, 1, 0), 1)
	assert("null == null", compareArrowValues(nullArr, 0, 0), 0)

	nullArr.Release()
	nullB.Release()

	// ---- UNSUPPORTED TYPE PANIC ----
	// Build a fixed-size binary array to trigger panic
	fsb := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 2})
	fsb.Append([]byte{1, 2})
	fsb.Append([]byte{3, 4})
	fsArr := fsb.NewArray()

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		_ = compareArrowValues(fsArr, 0, 1)
	}()
	if !didPanic {
		t.Fatalf("expected panic for unsupported Arrow type")
	}

	fsArr.Release()
	fsb.Release()
}

// Top-K sort tests kept simple and grouped into two test functions
func buildInt8(mem memory.Allocator, vals []int8) *array.Int8 {
	b := array.NewInt8Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Int8)
	b.Release()
	return arr
}

func buildInt16(mem memory.Allocator, vals []int16) *array.Int16 {
	b := array.NewInt16Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Int16)
	b.Release()
	return arr
}

func buildInt32(mem memory.Allocator, vals []int32) *array.Int32 {
	b := array.NewInt32Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Int32)
	b.Release()
	return arr
}

func buildInt64(mem memory.Allocator, vals []int64) *array.Int64 {
	b := array.NewInt64Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Int64)
	b.Release()
	return arr
}

func buildUint8(mem memory.Allocator, vals []uint8) *array.Uint8 {
	b := array.NewUint8Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Uint8)
	b.Release()
	return arr
}

func buildUint16(mem memory.Allocator, vals []uint16) *array.Uint16 {
	b := array.NewUint16Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Uint16)
	b.Release()
	return arr
}

func buildUint32(mem memory.Allocator, vals []uint32) *array.Uint32 {
	b := array.NewUint32Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Uint32)
	b.Release()
	return arr
}

func buildUint64(mem memory.Allocator, vals []uint64) *array.Uint64 {
	b := array.NewUint64Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Uint64)
	b.Release()
	return arr
}

func buildFloat32(mem memory.Allocator, vals []float32) *array.Float32 {
	b := array.NewFloat32Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Float32)
	b.Release()
	return arr
}

func buildFloat64(mem memory.Allocator, vals []float64) *array.Float64 {
	b := array.NewFloat64Builder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Float64)
	b.Release()
	return arr
}

func buildBool(mem memory.Allocator, vals []bool) *array.Boolean {
	b := array.NewBooleanBuilder(mem)
	for _, v := range vals {
		b.Append(v)
	}
	arr := b.NewArray().(*array.Boolean)
	b.Release()
	return arr
}

// Consolidated TopK tests: two functions with multiple subtests, placed at file bottom.
func TestTopKSort_BasicAndValues(t *testing.T) {
	t.Run("AgeDesc_Top5", func(t *testing.T) {
		proj := aggProject()
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)

		sortExec, err := NewTopKSortExec(proj, CombineSortKeys(ageSK), 5)
		if err != nil {
			t.Fatalf("NewTopKSortExec error: %v", err)
		}
		rb, err := sortExec.Next(5)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if rb.RowCount != 5 {
			t.Fatalf("expected 5 rows, got %d", rb.RowCount)
		}
		ages := rb.Columns[2].(*array.Int32)
		expected := []int32{50, 48, 46, 45, 43}
		for i := range expected {
			if ages.Value(i) != expected[i] {
				t.Fatalf("age mismatch at %d: expected %v got %v", i, expected[i], ages.Value(i))
			}
		}
		for _, c := range rb.Columns {
			c.Release()
		}
		if err := sortExec.Close(); err != nil {
			t.Fatalf("close error: %v", err)
		}
	})

	t.Run("KGreaterThanRows_ReturnsAll", func(t *testing.T) {
		proj := aggProject()
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		sortExec, err := NewTopKSortExec(proj, CombineSortKeys(ageSK), 100)
		if err != nil {
			t.Fatalf("NewTopKSortExec error: %v", err)
		}
		rb, err := sortExec.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("Next error: %v", err)
		}
		if rb.RowCount == 0 {
			t.Fatalf("expected rows when K > total rows")
		}
		for _, c := range rb.Columns {
			c.Release()
		}
		if err := sortExec.Close(); err != nil {
			t.Fatalf("close error: %v", err)
		}
	})
}

func TestTopKSort_CombinedAndPagination(t *testing.T) {
	t.Run("CombinedKeys_Pagination_TotalMatchesK", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		sortExec, err := NewTopKSortExec(proj, CombineSortKeys(ageSK, nameSK), 7)
		if err != nil {
			t.Fatalf("NewTopKSortExec error: %v", err)
		}
		total := uint64(0)
		for _, sz := range []uint16{3, 3, 3} {
			rb, err := sortExec.Next(sz)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("Next error: %v", err)
			}
			total += rb.RowCount
			for _, c := range rb.Columns {
				c.Release()
			}
			if errors.Is(err, io.EOF) {
				break
			}
		}
		if total != 7 {
			t.Fatalf("expected total 7 rows, got %d", total)
		}
		if err := sortExec.Close(); err != nil {
			t.Fatalf("close error: %v", err)
		}
	})
}
