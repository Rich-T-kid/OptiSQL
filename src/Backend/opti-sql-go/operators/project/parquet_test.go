package project

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const ParquetTestDatafile = "../../../test_data/parquet/capitals_clean.parquet"

func getTestParuqetFile() *os.File {
	file, err := os.Open(ParquetTestDatafile)
	if err != nil {
		panic(err)
	}
	return file
}

/*
schema:

	fields: 5
	  - country: type=utf8, nullable
	       metadata: ["PARQUET:field_id": "-1"]
	  - country_alpha2: type=utf8, nullable
	              metadata: ["PARQUET:field_id": "-1"]
	  - capital: type=utf8, nullable
	       metadata: ["PARQUET:field_id": "-1"]
	  - lat: type=float64, nullable
	   metadata: ["PARQUET:field_id": "-1"]
	  - lon: type=float64, nullable
*/
// TODO: more to their own files later down the line
func existIn(str string, arr []string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}
func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func TestParquetInit(t *testing.T) {
	t.Run("Test No names pass in", func(t *testing.T) {
		f := getTestParuqetFile()

		_, err := NewParquetSourcePushDown(f, []string{}, nil)
		if err == nil {
			t.Errorf("Expected error when no columns are passed in, but got nil")
		}
	})

	t.Run("Test invalid names are passed in", func(t *testing.T) {
		f := getTestParuqetFile()
		_, err := NewParquetSourcePushDown(f, []string{"non_existent_column"}, nil)
		if err == nil {
			t.Errorf("Expected error when invalid column names are passed in, but got nil")
		}
	})

	t.Run("Test correct schema is returned", func(t *testing.T) {
		f := getTestParuqetFile()
		columns := []string{"country", "capital", "lat"}
		source, err := NewParquetSourcePushDown(f, columns, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		schema := source.Schema()
		if len(schema.Fields()) != len(columns) {
			t.Errorf("Expected schema to have %d fields, got %d", len(columns), len(schema.Fields()))
		}
		for _, field := range schema.Fields() {
			if !existIn(field.Name, columns) {
				t.Errorf("Field %s not found in expected columns %v", field.Name, columns)
			}
		}

	})

	t.Run("Test input columns and filters were passed back out", func(t *testing.T) {
		f := getTestParuqetFile()
		columns := []string{"country", "capital", "lat"}
		source, err := NewParquetSourcePushDown(f, columns, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(source.projectionPushDown) != len(columns) {
			t.Errorf("Expected projectionPushDown to have %d columns, got %d", len(columns), len(source.projectionPushDown))
		}
		if !sameStringSlice(source.projectionPushDown, columns) || source.predicatePushDown != nil {
			t.Errorf("Expected projectionPushDown to be %v and predicatePushDown to be nil, got %v and %v", columns, source.projectionPushDown, source.predicatePushDown)
		}
	})

	t.Run("Check reader isnt null", func(t *testing.T) {

		f := getTestParuqetFile()
		columns := []string{"country", "capital", "lat"}
		source, err := NewParquetSourcePushDown(f, columns, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if source.reader == nil {
			t.Errorf("Expected reader to be initialized, but got nil")
		}

	})

}
func TestParquetClose(t *testing.T) {
	f := getTestParuqetFile()
	columns := []string{"country", "capital", "lat"}
	source, err := NewParquetSourcePushDown(f, columns, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	err = source.Close()
	if err != nil {
		t.Errorf("Unexpected error on Close: %v", err)
	}
	if source.reader != nil {
		t.Errorf("Expected reader to be nil after Close, but it is not")
	}
	_, err = source.Next(1)
	if err != io.EOF {
		t.Error("expected reader to return io.EOF")
	}

}
func TestRunToEnd(t *testing.T) {
	f := getTestParuqetFile()
	columns := []string{"country", "capital", "lat"}
	source, err := NewParquetSourcePushDown(f, columns, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for {
		rc, err := source.Next(1024 * 8)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Unexpected error on Next: %v", err)
		}
		t.Log("RecordBatch: ", rc)
	}
}

func TestParquetRead(t *testing.T) {
	f := getTestParuqetFile()
	columns := []string{"country", "capital", "lat"}
	source, err := NewParquetSourcePushDown(f, columns, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	//	batchSize := uint16(10)
	rc, err := source.Next(uint16(15))
	if err != nil {
		t.Fatalf("Unexpected error on Next: %v", err)
	}
	if rc == nil {
		t.Fatalf("Expected RecordBatch, got nil")
	}
	if len(rc.Columns) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(rc.Columns))
	}
	if rc.Schema.NumFields() != len(columns) {
		t.Errorf("Expected schema to have %d fields, got %d", len(columns), rc.Schema.NumFields())
	}
	fmt.Printf("columns:%v\n", rc.Columns)
	fmt.Printf("count:%d\n", rc.RowCount)
}

// CombineArray tests: cover primitive, uint, float, bool, string, binary and nil-handling
func TestCombineArray_Cases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("INT8", func(t *testing.T) {
		ib1 := array.NewInt8Builder(mem)
		ib1.Append(1)
		ib1.AppendNull()
		a1 := ib1.NewArray().(*array.Int8)
		ib2 := array.NewInt8Builder(mem)
		ib2.Append(2)
		ib2.Append(3)
		a2 := ib2.NewArray().(*array.Int8)
		comb := CombineArray(a1, a2).(*array.Int8)
		if comb.Len() != a1.Len()+a2.Len() {
			t.Fatalf("int8 combined length wrong")
		}
		if comb.Value(0) != 1 || !comb.IsNull(1) || comb.Value(2) != 2 {
			t.Fatalf("int8 values unexpected")
		}
		a1.Release()
		a2.Release()
		comb.Release()
	})

	t.Run("INT16", func(t *testing.T) {
		i16b1 := array.NewInt16Builder(mem)
		i16b1.Append(10)
		i16b1.Append(20)
		ia1 := i16b1.NewArray().(*array.Int16)
		i16b2 := array.NewInt16Builder(mem)
		i16b2.Append(30)
		ia2 := i16b2.NewArray().(*array.Int16)
		i16c := CombineArray(ia1, ia2).(*array.Int16)
		if i16c.Len() != ia1.Len()+ia2.Len() {
			t.Fatalf("int16 combined length")
		}
		ia1.Release()
		ia2.Release()
		i16c.Release()
	})

	t.Run("INT32", func(t *testing.T) {
		i32b1 := array.NewInt32Builder(mem)
		i32b1.Append(1)
		ia32_1 := i32b1.NewArray().(*array.Int32)
		i32b2 := array.NewInt32Builder(mem)
		i32b2.Append(2)
		ia32_2 := i32b2.NewArray().(*array.Int32)
		i32c := CombineArray(ia32_1, ia32_2).(*array.Int32)
		if i32c.Len() != 2 {
			t.Fatalf("int32 combined length")
		}
		ia32_1.Release()
		ia32_2.Release()
		i32c.Release()
	})

	t.Run("INT64", func(t *testing.T) {
		i64b1 := array.NewInt64Builder(mem)
		i64b1.Append(100)
		ia64_1 := i64b1.NewArray().(*array.Int64)
		i64b2 := array.NewInt64Builder(mem)
		i64b2.Append(200)
		ia64_2 := i64b2.NewArray().(*array.Int64)
		i64c := CombineArray(ia64_1, ia64_2).(*array.Int64)
		if i64c.Len() != 2 {
			t.Fatalf("int64 combined length")
		}
		ia64_1.Release()
		ia64_2.Release()
		i64c.Release()
	})

	t.Run("UINT8", func(t *testing.T) {
		u8b1 := array.NewUint8Builder(mem)
		u8b1.Append(8)
		ua8_1 := u8b1.NewArray().(*array.Uint8)
		u8b2 := array.NewUint8Builder(mem)
		u8b2.Append(9)
		ua8_2 := u8b2.NewArray().(*array.Uint8)
		u8c := CombineArray(ua8_1, ua8_2).(*array.Uint8)
		if u8c.Len() != 2 {
			t.Fatalf("uint8 combined length")
		}
		ua8_1.Release()
		ua8_2.Release()
		u8c.Release()
	})

	t.Run("UINT16", func(t *testing.T) {
		u16b1 := array.NewUint16Builder(mem)
		u16b1.Append(16)
		ua16_1 := u16b1.NewArray().(*array.Uint16)
		u16b2 := array.NewUint16Builder(mem)
		u16b2.Append(32)
		ua16_2 := u16b2.NewArray().(*array.Uint16)
		u16c := CombineArray(ua16_1, ua16_2).(*array.Uint16)
		if u16c.Len() != 2 {
			t.Fatalf("uint16 combined length")
		}
		ua16_1.Release()
		ua16_2.Release()
		u16c.Release()
	})

	t.Run("UINT32", func(t *testing.T) {
		u32b1 := array.NewUint32Builder(mem)
		u32b1.Append(1000)
		ua32_1 := u32b1.NewArray().(*array.Uint32)
		u32b2 := array.NewUint32Builder(mem)
		u32b2.Append(2000)
		ua32_2 := u32b2.NewArray().(*array.Uint32)
		u32c := CombineArray(ua32_1, ua32_2).(*array.Uint32)
		if u32c.Len() != 2 {
			t.Fatalf("uint32 combined length")
		}
		ua32_1.Release()
		ua32_2.Release()
		u32c.Release()
	})

	t.Run("UINT64", func(t *testing.T) {
		u64b1 := array.NewUint64Builder(mem)
		u64b1.Append(10000)
		ua64_1 := u64b1.NewArray().(*array.Uint64)
		u64b2 := array.NewUint64Builder(mem)
		u64b2.Append(20000)
		ua64_2 := u64b2.NewArray().(*array.Uint64)
		u64c := CombineArray(ua64_1, ua64_2).(*array.Uint64)
		if u64c.Len() != 2 {
			t.Fatalf("uint64 combined length")
		}
		ua64_1.Release()
		ua64_2.Release()
		u64c.Release()
	})

	t.Run("FLOAT32", func(t *testing.T) {
		f32b1 := array.NewFloat32Builder(mem)
		f32b1.Append(1.25)
		fa32_1 := f32b1.NewArray().(*array.Float32)
		f32b2 := array.NewFloat32Builder(mem)
		f32b2.Append(2.5)
		fa32_2 := f32b2.NewArray().(*array.Float32)
		f32c := CombineArray(fa32_1, fa32_2).(*array.Float32)
		if f32c.Len() != 2 {
			t.Fatalf("float32 combined length")
		}
		fa32_1.Release()
		fa32_2.Release()
		f32c.Release()
	})

	t.Run("FLOAT64", func(t *testing.T) {
		f64b1 := array.NewFloat64Builder(mem)
		f64b1.Append(3.14)
		fa64_1 := f64b1.NewArray().(*array.Float64)
		f64b2 := array.NewFloat64Builder(mem)
		f64b2.Append(6.28)
		fa64_2 := f64b2.NewArray().(*array.Float64)
		f64c := CombineArray(fa64_1, fa64_2).(*array.Float64)
		if f64c.Len() != 2 {
			t.Fatalf("float64 combined length")
		}
		fa64_1.Release()
		fa64_2.Release()
		f64c.Release()
	})

	t.Run("BOOL", func(t *testing.T) {
		bb1 := array.NewBooleanBuilder(mem)
		bb1.Append(true)
		bb1.AppendNull()
		ba1 := bb1.NewArray().(*array.Boolean)
		bb2 := array.NewBooleanBuilder(mem)
		bb2.Append(false)
		ba2 := bb2.NewArray().(*array.Boolean)
		bc := CombineArray(ba1, ba2).(*array.Boolean)
		if bc.Len() != ba1.Len()+ba2.Len() {
			t.Fatalf("bool combined length")
		}
		ba1.Release()
		ba2.Release()
		bc.Release()
	})

	t.Run("STRING", func(t *testing.T) {
		sb1 := array.NewStringBuilder(mem)
		sb1.Append("one")
		sb1.AppendNull()
		sa1 := sb1.NewArray().(*array.String)
		sb2 := array.NewStringBuilder(mem)
		sb2.Append("two")
		sa2 := sb2.NewArray().(*array.String)
		sc := CombineArray(sa1, sa2).(*array.String)
		if sc.Len() != sa1.Len()+sa2.Len() {
			t.Fatalf("string combined length")
		}
		sa1.Release()
		sa2.Release()
		sc.Release()
	})

	t.Run("LARGE_STRING", func(t *testing.T) {
		lsb1 := array.NewLargeStringBuilder(mem)
		lsb1.Append("big1")
		la1 := lsb1.NewArray().(*array.LargeString)
		lsb2 := array.NewLargeStringBuilder(mem)
		lsb2.Append("big2")
		la2 := lsb2.NewArray().(*array.LargeString)
		lc := CombineArray(la1, la2).(*array.LargeString)
		if lc.Len() != la1.Len()+la2.Len() {
			t.Fatalf("large string combined length")
		}
		la1.Release()
		la2.Release()
		lc.Release()
	})

	t.Run("BINARY", func(t *testing.T) {
		bbld := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		bbld.Append([]byte("a"))
		baBb1 := bbld.NewArray().(*array.Binary)
		bbld2 := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		bbld2.Append([]byte("b"))
		baBb2 := bbld2.NewArray().(*array.Binary)
		bcbin := CombineArray(baBb1, baBb2).(*array.Binary)
		if bcbin.Len() != baBb1.Len()+baBb2.Len() {
			t.Fatalf("binary combined length")
		}
		baBb1.Release()
		baBb2.Release()
		bcbin.Release()
	})

	t.Run("NIL_A1", func(t *testing.T) {
		// build a small binary array to pass as second
		bbld := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		bbld.Append([]byte("z"))
		sec := bbld.NewArray().(*array.Binary)
		got := CombineArray(nil, sec)
		if got == nil {
			t.Fatalf("expected non-nil when a1 is nil")
		}
		if got != sec { // CombineArray will return sec directly when a1 is nil
			got.Release()
		}
		sec.Release()
	})

	t.Run("NIL_A2", func(t *testing.T) {
		bbld := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		bbld.Append([]byte("y"))
		first := bbld.NewArray().(*array.Binary)
		got := CombineArray(first, nil)
		if got == nil {
			t.Fatalf("expected non-nil when a2 is nil")
		}
		if got != first { // CombineArray will return first directly when a2 is nil
			got.Release()
		}
		first.Release()
	})
}

// includes null values so append* helpers take the AppendNull branch.
func TestCombineArray_PerTypeNulls(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("AppendUint16_nulls", func(t *testing.T) {
		b1 := array.NewUint16Builder(mem)
		b1.Append(11)
		b1.AppendNull()
		b1.Append(13)
		a1 := b1.NewArray().(*array.Uint16)

		b2 := array.NewUint16Builder(mem)
		b2.AppendNull()
		b2.Append(15)
		a2 := b2.NewArray().(*array.Uint16)

		out := CombineArray(a1, a2).(*array.Uint16)
		if out.Len() != 5 {
			t.Fatalf("uint16 expected len 5 got %d", out.Len())
		}
		if !out.IsNull(1) || !out.IsNull(3) {
			t.Fatalf("uint16 nulls not preserved")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendInt16_nulls", func(t *testing.T) {
		b1 := array.NewInt16Builder(mem)
		b1.Append(21)
		b1.AppendNull()
		a1 := b1.NewArray().(*array.Int16)
		b2 := array.NewInt16Builder(mem)
		b2.AppendNull()
		b2.Append(23)
		a2 := b2.NewArray().(*array.Int16)
		out := CombineArray(a1, a2).(*array.Int16)
		if out.Len() != 4 {
			t.Fatalf("int16 expected len 4 got %d", out.Len())
		}
		if !out.IsNull(1) || !out.IsNull(2) {
			t.Fatalf("int16 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendInt32_nulls", func(t *testing.T) {
		b1 := array.NewInt32Builder(mem)
		b1.Append(31)
		b1.AppendNull()
		a1 := b1.NewArray().(*array.Int32)
		b2 := array.NewInt32Builder(mem)
		b2.AppendNull()
		b2.Append(33)
		a2 := b2.NewArray().(*array.Int32)
		out := CombineArray(a1, a2).(*array.Int32)
		if !out.IsNull(1) || !out.IsNull(2) {
			t.Fatalf("int32 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})
	t.Run("AppendUint32_nulls", func(t *testing.T) {
		b1 := array.NewUint32Builder(mem)
		b1.AppendNull()
		b1.Append(22)
		a1 := b1.NewArray().(*array.Uint32)
		b2 := array.NewUint32Builder(mem)
		b2.Append(23)
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Uint32)
		out := CombineArray(a1, a2).(*array.Uint32)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("uint32 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendInt64_nulls", func(t *testing.T) {
		b1 := array.NewInt64Builder(mem)
		b1.AppendNull()
		b1.Append(41)
		a1 := b1.NewArray().(*array.Int64)
		b2 := array.NewInt64Builder(mem)
		b2.Append(42)
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Int64)
		out := CombineArray(a1, a2).(*array.Int64)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("int64 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendUint64_nulls", func(t *testing.T) {
		b1 := array.NewUint64Builder(mem)
		b1.AppendNull()
		b1.Append(41)
		a1 := b1.NewArray().(*array.Uint64)
		b2 := array.NewUint64Builder(mem)
		b2.Append(42)
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Uint64)
		out := CombineArray(a1, a2).(*array.Uint64)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("Uint64 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()

	})

	t.Run("AppendUint8_nulls", func(t *testing.T) {
		b1 := array.NewUint8Builder(mem)
		b1.AppendNull()
		b1.Append(2)
		a1 := b1.NewArray().(*array.Uint8)
		b2 := array.NewUint8Builder(mem)
		b2.Append(3)
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Uint8)
		out := CombineArray(a1, a2).(*array.Uint8)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("uint8 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendFloat32_nulls", func(t *testing.T) {
		b1 := array.NewFloat32Builder(mem)
		b1.Append(1.5)
		b1.AppendNull()
		a1 := b1.NewArray().(*array.Float32)
		b2 := array.NewFloat32Builder(mem)
		b2.AppendNull()
		b2.Append(2.5)
		a2 := b2.NewArray().(*array.Float32)
		out := CombineArray(a1, a2).(*array.Float32)
		if !out.IsNull(1) || !out.IsNull(2) {
			t.Fatalf("float32 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendFloat64_nulls", func(t *testing.T) {
		b1 := array.NewFloat64Builder(mem)
		b1.AppendNull()
		b1.Append(3.14)
		a1 := b1.NewArray().(*array.Float64)
		b2 := array.NewFloat64Builder(mem)
		b2.Append(4.14)
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Float64)
		out := CombineArray(a1, a2).(*array.Float64)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("float64 nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendBool_nulls", func(t *testing.T) {
		b1 := array.NewBooleanBuilder(mem)
		b1.Append(true)
		b1.AppendNull()
		a1 := b1.NewArray().(*array.Boolean)
		b2 := array.NewBooleanBuilder(mem)
		b2.AppendNull()
		b2.Append(false)
		a2 := b2.NewArray().(*array.Boolean)
		out := CombineArray(a1, a2).(*array.Boolean)
		if !out.IsNull(1) || !out.IsNull(2) {
			t.Fatalf("bool nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendString_nulls", func(t *testing.T) {
		b1 := array.NewStringBuilder(mem)
		b1.Append("s1")
		b1.AppendNull()
		a1 := b1.NewArray().(*array.String)
		b2 := array.NewStringBuilder(mem)
		b2.AppendNull()
		b2.Append("s2")
		a2 := b2.NewArray().(*array.String)
		out := CombineArray(a1, a2).(*array.String)
		if !out.IsNull(1) || !out.IsNull(2) {
			t.Fatalf("string nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendLargeString_nulls", func(t *testing.T) {
		b1 := array.NewLargeStringBuilder(mem)
		b1.AppendNull()
		b1.Append("L1")
		a1 := b1.NewArray().(*array.LargeString)
		b2 := array.NewLargeStringBuilder(mem)
		b2.Append("L2")
		b2.AppendNull()
		a2 := b2.NewArray().(*array.LargeString)
		out := CombineArray(a1, a2).(*array.LargeString)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("large string nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

	t.Run("AppendBinary_nulls", func(t *testing.T) {
		b1 := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		b1.AppendNull()
		b1.Append([]byte("bb1"))
		a1 := b1.NewArray().(*array.Binary)
		b2 := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		b2.Append([]byte("bb2"))
		b2.AppendNull()
		a2 := b2.NewArray().(*array.Binary)
		out := CombineArray(a1, a2).(*array.Binary)
		if !out.IsNull(0) || !out.IsNull(3) {
			t.Fatalf("binary nulls not present")
		}
		a1.Release()
		a2.Release()
		out.Release()
	})

}
func TestCombineArray_UnsupportedType(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Build a FixedSizeBinary array (NOT supported in your switch)
	dt := &arrow.FixedSizeBinaryType{ByteWidth: 4}
	b := array.NewFixedSizeBinaryBuilder(mem, dt)
	b.Append([]byte{0, 1, 2, 3})
	b.Append([]byte{4, 5, 6, 7})
	arr := b.NewArray()
	b.Release()

	defer arr.Release()

	// Expect panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for unsupported datatype")
		}
	}()

	// Call CombineArray with unsupported type
	_ = CombineArray(arr, arr)
}
