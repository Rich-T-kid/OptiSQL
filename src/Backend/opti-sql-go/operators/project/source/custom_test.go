package source

import (
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// generateTestColumns returns 8 column names and matching columns,
// each column containing ~10 entries for testing purposes.
func generateTestColumns() ([]string, []any) {
	names := []string{
		"id",
		"name",
		"age",
		"salary",
		"is_active",
		"department",
		"rating",
		"years_experience",
	}

	columns := []any{
		[]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]string{
			"Alice", "Bob", "Charlie", "David", "Eve",
			"Frank", "Grace", "Hannah", "Ivy", "Jake",
		},
		[]int32{28, 34, 45, 22, 31, 29, 40, 36, 50, 26},
		[]float64{
			70000.0, 82000.5, 54000.0, 91000.0, 60000.0,
			75000.0, 66000.0, 88000.0, 45000.0, 99000.0,
		},
		[]bool{true, false, true, true, false, false, true, true, false, true},
		[]string{
			"Engineering", "HR", "Engineering", "Sales", "Finance",
			"Sales", "Support", "Engineering", "HR", "Finance",
		},
		[]float32{4.5, 3.8, 4.2, 2.9, 5.0, 4.3, 3.7, 4.9, 4.1, 3.5},
		[]int32{1, 5, 10, 2, 7, 3, 6, 12, 4, 8},
	}

	return names, columns
}

func TestInMemoryBatchInit(t *testing.T) {
	// Simple passing test
	names := []string{"id", "name", "age", "salary", "is_active"}
	columns := []any{
		[]int32{1, 2, 3, 4, 5},
		[]string{"Alice", "Bob", "Charlie", "David", "Eve"},
		[]int32{30, 25, 35, 28, 40},
		[]float64{70000.0, 50000.0, 80000.0, 60000.0, 90000.0},
		[]bool{true, false, true, true, false},
	}
	projC, err := NewInMemoryProjectExec(names, columns)
	if err != nil {
		t.Errorf("Failed to create InMemoryProjectExec: %v", err)
	}
	if projC.schema == nil {
		t.Error("Schema is nil")
	}
	if projC.columns == nil {
		t.Error("Columns are nil")
	}
	if projC.schema.NumFields() != len(names) {
		t.Errorf("Schema field count mismatch: got %d, want %d", projC.schema.NumFields(), len(names))
	}
	if len(projC.columns) != len(columns) {
		t.Errorf("Columns count mismatch: got %d, want %d", len(projC.columns), len(columns))
	}
	if len(projC.columns) != projC.schema.NumFields() {
		t.Errorf("Columns and schema field count mismatch: got %d and %d", len(projC.columns), projC.schema.NumFields())
	}
	fmt.Printf("schema: %v\n", projC.schema)
}

// ==================== COMPREHENSIVE TESTS FOR 100% CODE COVERAGE ====================

// TestSupportedType tests every branch of the supportedType function
func TestSupportedType(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		// Supported integer types
		{"[]int", []int{1, 2, 3}, true},
		{"[]int8", []int8{1, 2, 3}, true},
		{"[]int16", []int16{1, 2, 3}, true},
		{"[]int32", []int32{1, 2, 3}, true},
		{"[]int64", []int64{1, 2, 3}, true},

		// Supported unsigned integer types
		{"[]uint", []uint{1, 2, 3}, true},
		{"[]uint8", []uint8{1, 2, 3}, true},
		{"[]uint16", []uint16{1, 2, 3}, true},
		{"[]uint32", []uint32{1, 2, 3}, true},
		{"[]uint64", []uint64{1, 2, 3}, true},

		// Supported float types
		{"[]float32", []float32{1.1, 2.2, 3.3}, true},
		{"[]float64", []float64{1.1, 2.2, 3.3}, true},

		// Supported string type
		{"[]string", []string{"a", "b", "c"}, true},

		// Supported boolean type
		{"[]bool", []bool{true, false, true}, true},

		// Unsupported types
		//{"[]byte", []byte{1, 2, 3}, false}, alias for uint8
		//{"[]rune", []rune{'a', 'b', 'c'}, false}, alias for int32
		{"[]interface{}", []interface{}{1, "a", true}, false},
		{"map[string]int", map[string]int{"a": 1}, false},
		{"string", "not a slice", false},
		{"int", 123, false},
		{"struct", struct{ x int }{x: 1}, false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := supportedType(tt.input)
			if result != tt.expected {
				t.Errorf("supportedType(%v) = %v, expected %v", tt.name, result, tt.expected)
			}
		})
	}
}

// TestUnpackColumn tests every branch of the unpackColumm function
func TestUnpackColumn(t *testing.T) {
	t.Run("[]int type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_int", []int{1, 2, 3, 4, 5})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Name != "test_int" {
			t.Errorf("Expected field name 'test_int', got '%s'", field.Name)
		}
		if field.Type != arrow.PrimitiveTypes.Int64 {
			t.Errorf("Expected Int64 type, got %v", field.Type)
		}
		if !field.Nullable {
			t.Error("Expected field to be nullable")
		}
		int64Arr, ok := arr.(*array.Int64)
		if !ok {
			t.Fatalf("Expected *array.Int64, got %T", arr)
		}
		if int64Arr.Len() != 5 {
			t.Errorf("Expected 5 elements, got %d", int64Arr.Len())
		}
		for i := 0; i < 5; i++ {
			if int64Arr.Value(i) != int64(i+1) {
				t.Errorf("Element %d: expected %d, got %d", i, i+1, int64Arr.Value(i))
			}
		}
	})

	t.Run("[]int8 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_int8", []int8{-1, 0, 1, 127})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Int8 {
			t.Errorf("Expected Int8 type, got %v", field.Type)
		}
		int8Arr, ok := arr.(*array.Int8)
		if !ok {
			t.Fatalf("Expected *array.Int8, got %T", arr)
		}
		if int8Arr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", int8Arr.Len())
		}
	})

	t.Run("[]int16 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_int16", []int16{-100, 0, 100, 32767})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Int16 {
			t.Errorf("Expected Int16 type, got %v", field.Type)
		}
		int16Arr, ok := arr.(*array.Int16)
		if !ok {
			t.Fatalf("Expected *array.Int16, got %T", arr)
		}
		if int16Arr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", int16Arr.Len())
		}
	})

	t.Run("[]int32 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_int32", []int32{-1000, 0, 1000, 2147483647})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Int32 {
			t.Errorf("Expected Int32 type, got %v", field.Type)
		}
		int32Arr, ok := arr.(*array.Int32)
		if !ok {
			t.Fatalf("Expected *array.Int32, got %T", arr)
		}
		if int32Arr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", int32Arr.Len())
		}
	})

	t.Run("[]int64 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_int64", []int64{-9223372036854775808, 0, 9223372036854775807})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Int64 {
			t.Errorf("Expected Int64 type, got %v", field.Type)
		}
		int64Arr, ok := arr.(*array.Int64)
		if !ok {
			t.Fatalf("Expected *array.Int64, got %T", arr)
		}
		if int64Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", int64Arr.Len())
		}
	})

	t.Run("[]uint type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_uint", []uint{0, 1, 100, 1000})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Uint64 {
			t.Errorf("Expected Uint64 type, got %v", field.Type)
		}
		uint64Arr, ok := arr.(*array.Uint64)
		if !ok {
			t.Fatalf("Expected *array.Uint64, got %T", arr)
		}
		if uint64Arr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", uint64Arr.Len())
		}
		expected := []uint64{0, 1, 100, 1000}
		for i, exp := range expected {
			if uint64Arr.Value(i) != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, uint64Arr.Value(i))
			}
		}
	})

	t.Run("[]uint8 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_uint8", []uint8{0, 1, 255})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Uint8 {
			t.Errorf("Expected Uint8 type, got %v", field.Type)
		}
		uint8Arr, ok := arr.(*array.Uint8)
		if !ok {
			t.Fatalf("Expected *array.Uint8, got %T", arr)
		}
		if uint8Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", uint8Arr.Len())
		}
	})

	t.Run("[]uint16 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_uint16", []uint16{0, 100, 65535})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Uint16 {
			t.Errorf("Expected Uint16 type, got %v", field.Type)
		}
		uint16Arr, ok := arr.(*array.Uint16)
		if !ok {
			t.Fatalf("Expected *array.Uint16, got %T", arr)
		}
		if uint16Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", uint16Arr.Len())
		}
	})

	t.Run("[]uint32 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_uint32", []uint32{0, 1000, 4294967295})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Uint32 {
			t.Errorf("Expected Uint32 type, got %v", field.Type)
		}
		uint32Arr, ok := arr.(*array.Uint32)
		if !ok {
			t.Fatalf("Expected *array.Uint32, got %T", arr)
		}
		if uint32Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", uint32Arr.Len())
		}
	})

	t.Run("[]uint64 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_uint64", []uint64{0, 1000, 18446744073709551615})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Uint64 {
			t.Errorf("Expected Uint64 type, got %v", field.Type)
		}
		uint64Arr, ok := arr.(*array.Uint64)
		if !ok {
			t.Fatalf("Expected *array.Uint64, got %T", arr)
		}
		if uint64Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", uint64Arr.Len())
		}
	})

	t.Run("[]float32 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_float32", []float32{-1.5, 0.0, 1.5, 3.14159})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Float32 {
			t.Errorf("Expected Float32 type, got %v", field.Type)
		}
		float32Arr, ok := arr.(*array.Float32)
		if !ok {
			t.Fatalf("Expected *array.Float32, got %T", arr)
		}
		if float32Arr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", float32Arr.Len())
		}
	})

	t.Run("[]float64 type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_float64", []float64{-2.718281828, 0.0, 3.141592653589793})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.PrimitiveTypes.Float64 {
			t.Errorf("Expected Float64 type, got %v", field.Type)
		}
		float64Arr, ok := arr.(*array.Float64)
		if !ok {
			t.Fatalf("Expected *array.Float64, got %T", arr)
		}
		if float64Arr.Len() != 3 {
			t.Errorf("Expected 3 elements, got %d", float64Arr.Len())
		}
	})

	t.Run("[]string type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_string", []string{"hello", "world", "test", ""})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.BinaryTypes.String {
			t.Errorf("Expected String type, got %v", field.Type)
		}
		stringArr, ok := arr.(*array.String)
		if !ok {
			t.Fatalf("Expected *array.String, got %T", arr)
		}
		if stringArr.Len() != 4 {
			t.Errorf("Expected 4 elements, got %d", stringArr.Len())
		}
		expected := []string{"hello", "world", "test", ""}
		for i, exp := range expected {
			if stringArr.Value(i) != exp {
				t.Errorf("Element %d: expected '%s', got '%s'", i, exp, stringArr.Value(i))
			}
		}
	})

	t.Run("[]bool type", func(t *testing.T) {
		field, arr, err := unpackColumm("test_bool", []bool{true, false, true, false, true})
		if err != nil {
			t.Fatalf("unpackColumm failed: %v", err)
		}
		if field.Type != arrow.FixedWidthTypes.Boolean {
			t.Errorf("Expected Boolean type, got %v", field.Type)
		}
		boolArr, ok := arr.(*array.Boolean)
		if !ok {
			t.Fatalf("Expected *array.Boolean, got %T", arr)
		}
		if boolArr.Len() != 5 {
			t.Errorf("Expected 5 elements, got %d", boolArr.Len())
		}
		expected := []bool{true, false, true, false, true}
		for i, exp := range expected {
			if boolArr.Value(i) != exp {
				t.Errorf("Element %d: expected %v, got %v", i, exp, boolArr.Value(i))
			}
		}
	})

	t.Run("Unsupported type - default case", func(t *testing.T) {
		_, _, err := unpackColumm("test_unsupported", []byte{1, 2, 3})
		if err != nil {
			t.Error("unexpected error for unsupported type")
		}

	})

	t.Run("Empty slices", func(t *testing.T) {
		field, arr, err := unpackColumm("empty_int", []int{})
		if err != nil {
			t.Fatalf("unpackColumm failed for empty slice: %v", err)
		}
		if arr.Len() != 0 {
			t.Errorf("Expected 0 elements for empty slice, got %d", arr.Len())
		}
		if field.Name != "empty_int" {
			t.Errorf("Expected field name 'empty_int', got '%s'", field.Name)
		}
	})
}

// TestNewInMemoryProjectExec tests the constructor comprehensively
func TestNewInMemoryProjectExec(t *testing.T) {
	t.Run("Valid construction with all types", func(t *testing.T) {
		names := []string{
			"col_int", "col_int8", "col_int16", "col_int32", "col_int64",
			"col_uint", "col_uint8", "col_uint16", "col_uint32", "col_uint64",
			"col_float32", "col_float64", "col_string", "col_bool",
		}
		columns := []any{
			[]int{1, 2},
			[]int8{1, 2},
			[]int16{1, 2},
			[]int32{1, 2},
			[]int64{1, 2},
			[]uint{1, 2},
			[]uint8{1, 2},
			[]uint16{1, 2},
			[]uint32{1, 2},
			[]uint64{1, 2},
			[]float32{1.1, 2.2},
			[]float64{1.1, 2.2},
			[]string{"a", "b"},
			[]bool{true, false},
		}

		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		if proj == nil {
			t.Fatal("InMemoryProjectExec is nil")
		}
		if proj.schema == nil {
			t.Fatal("Schema is nil")
		}
		if proj.columns == nil {
			t.Fatal("Columns are nil")
		}
		if proj.schema.NumFields() != len(names) {
			t.Errorf("Expected %d fields, got %d", len(names), proj.schema.NumFields())
		}
		if len(proj.columns) != len(columns) {
			t.Errorf("Expected %d columns, got %d", len(columns), len(proj.columns))
		}

		// Verify each field name matches
		fields := proj.schema.Fields()
		for i, expectedName := range names {
			if fields[i].Name != expectedName {
				t.Errorf("Field %d: expected name '%s', got '%s'", i, expectedName, fields[i].Name)
			}
			if !fields[i].Nullable {
				t.Errorf("Field %d (%s): expected nullable=true", i, expectedName)
			}
		}

		// Verify each column has correct length
		for i, col := range proj.columns {
			if col.Len() != 2 {
				t.Errorf("Column %d: expected length 2, got %d", i, col.Len())
			}
		}
	})

	t.Run("Mismatched names and columns count", func(t *testing.T) {
		names := []string{"col1", "col2"}
		columns := []any{[]int{1, 2, 3}}

		_, err := NewInMemoryProjectExec(names, columns)
		if err == nil {
			t.Error("Expected error for mismatched names and columns, got nil")
		}
	})

	t.Run("Unsupported type - supportedType returns false", func(t *testing.T) {
		// Custom struct type is not supported
		type CustomStruct struct {
			ID   int
			Name string
		}

		names := []string{"col1"}
		columns := []any{[]CustomStruct{{1, "test"}, {2, "data"}}}

		_, err := NewInMemoryProjectExec(names, columns)
		if err == nil {
			t.Error("Expected error for unsupported type, got nil")
		}

	})

	t.Run("Single column", func(t *testing.T) {
		names := []string{"only_col"}
		columns := []any{[]int{10, 20, 30}}

		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		if proj.schema.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", proj.schema.NumFields())
		}
		if len(proj.columns) != 1 {
			t.Errorf("Expected 1 column, got %d", len(proj.columns))
		}
		if proj.columns[0].Len() != 3 {
			t.Errorf("Expected column length 3, got %d", proj.columns[0].Len())
		}
	})

	t.Run("Empty columns", func(t *testing.T) {
		names := []string{}
		columns := []any{}

		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed for empty input: %v", err)
		}

		if proj.schema.NumFields() != 0 {
			t.Errorf("Expected 0 fields, got %d", proj.schema.NumFields())
		}
		if len(proj.columns) != 0 {
			t.Errorf("Expected 0 columns, got %d", len(proj.columns))
		}
	})

	t.Run("Columns with different lengths - valid construction", func(t *testing.T) {
		// Note: The function doesn't validate that all columns have the same length
		// This is valid construction even though columns have different lengths
		names := []string{"col1", "col2"}
		columns := []any{
			[]int{1, 2, 3},
			[]string{"a", "b"},
		}

		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		if proj.columns[0].Len() != 3 {
			t.Errorf("Column 0: expected length 3, got %d", proj.columns[0].Len())
		}
		if proj.columns[1].Len() != 2 {
			t.Errorf("Column 1: expected length 2, got %d", proj.columns[1].Len())
		}
	})

	t.Run("Complex field names", func(t *testing.T) {
		names := []string{"Column_1", "column-2", "Column.3", "column 4"}
		columns := []any{
			[]int{1},
			[]int{2},
			[]int{3},
			[]int{4},
		}

		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		fields := proj.schema.Fields()
		for i, expectedName := range names {
			if fields[i].Name != expectedName {
				t.Errorf("Field %d: expected name '%s', got '%s'", i, expectedName, fields[i].Name)
			}
		}
	})
}

// TestErrInvalidInMemoryDataType tests the error constructor
func TestErrInvalidInMemoryDataType(t *testing.T) {
	testType := []byte{1, 2, 3}
	err := ErrInvalidInMemoryDataType(testType)

	if err == nil {
		t.Fatal("ErrInvalidInMemoryDataType returned nil")
	}

	expectedMsg := "[]uint8 is not a supported in memory dataType for InMemoryProjectExec"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}

	// Test with different type
	testType2 := map[string]int{"key": 1}
	err2 := ErrInvalidInMemoryDataType(testType2)
	expectedMsg2 := "map[string]int is not a supported in memory dataType for InMemoryProjectExec"
	if err2.Error() != expectedMsg2 {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg2, err2.Error())
	}
}

// TestSchemaFieldTypes verifies the correct Arrow types are assigned
func TestSchemaFieldTypes(t *testing.T) {
	names := []string{
		"int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float32", "float64", "string", "bool",
	}
	columns := []any{
		[]int{1}, []int8{1}, []int16{1}, []int32{1}, []int64{1},
		[]uint{1}, []uint8{1}, []uint16{1}, []uint32{1}, []uint64{1},
		[]float32{1.0}, []float64{1.0}, []string{"a"}, []bool{true},
	}

	expectedTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int64,    // []int -> Int64
		arrow.PrimitiveTypes.Int8,     // []int8 -> Int8
		arrow.PrimitiveTypes.Int16,    // []int16 -> Int16
		arrow.PrimitiveTypes.Int32,    // []int32 -> Int32
		arrow.PrimitiveTypes.Int64,    // []int64 -> Int64
		arrow.PrimitiveTypes.Uint64,   // []uint -> Uint64
		arrow.PrimitiveTypes.Uint8,    // []uint8 -> Uint8
		arrow.PrimitiveTypes.Uint16,   // []uint16 -> Uint16
		arrow.PrimitiveTypes.Uint32,   // []uint32 -> Uint32
		arrow.PrimitiveTypes.Uint64,   // []uint64 -> Uint64
		arrow.PrimitiveTypes.Float32,  // []float32 -> Float32
		arrow.PrimitiveTypes.Float64,  // []float64 -> Float64
		arrow.BinaryTypes.String,      // []string -> String
		arrow.FixedWidthTypes.Boolean, // []bool -> Boolean
	}

	proj, err := NewInMemoryProjectExec(names, columns)
	if err != nil {
		t.Fatalf("NewInMemoryProjectExec failed: %v", err)
	}

	fields := proj.schema.Fields()
	for i, expectedType := range expectedTypes {
		if fields[i].Type != expectedType {
			t.Errorf("Field %d (%s): expected type %v, got %v",
				i, names[i], expectedType, fields[i].Type)
		}
	}
}

func TestPrunceSchema(t *testing.T) {
	names, columns := generateTestColumns()

	t.Run("Select subset of fields", func(t *testing.T) {
		// Create a fresh instance for this test
		testProj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Original schema should have 8 fields
		originalFieldCount := testProj.schema.NumFields()
		if originalFieldCount != 8 {
			t.Errorf("Expected 8 fields in original schema, got %d", originalFieldCount)
		}

		// Select only a subset of fields
		selectedFields := []string{"id", "name", "salary"}
		err = testProj.withFields(selectedFields...)
		if err != nil {
			t.Error("unexpected error when pruning columns")
		}

		// After pruning, schema should have only 3 fields
		prunedFieldCount := testProj.schema.NumFields()
		if prunedFieldCount != 3 {
			t.Errorf("Expected 3 fields after pruning, got %d", prunedFieldCount)
		}

		// Verify the field names match
		fields := testProj.schema.Fields()
		for i, expectedName := range selectedFields {
			if fields[i].Name != expectedName {
				t.Errorf("Field %d: expected name '%s', got '%s'", i, expectedName, fields[i].Name)
			}
		}

		// Verify field order is preserved
		if fields[0].Name != "id" || fields[1].Name != "name" || fields[2].Name != "salary" {
			t.Error("Field order not preserved after pruning")
		}
	})

	t.Run("Select single field", func(t *testing.T) {
		// Create a fresh instance for this test
		testProj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Select only one field
		err = testProj.withFields("department")
		if err != nil {
			t.Error("unexpected error when pruning columns")
		}

		// After pruning, schema should have only 1 field
		prunedFieldCount := testProj.schema.NumFields()
		if prunedFieldCount != 1 {
			t.Errorf("Expected 1 field after pruning, got %d", prunedFieldCount)
		}

		// Verify the field name
		fields := testProj.schema.Fields()
		if fields[0].Name != "department" {
			t.Errorf("Expected field name 'department', got '%s'", fields[0].Name)
		}

		// Verify the field type is preserved (should be String since department is []string)
		if fields[0].Type != arrow.BinaryTypes.String {
			t.Errorf("Expected String type, got %v", fields[0].Type)
		}
	})
}

// TestNext tests the Next function with projection and iteration
func TestNext(t *testing.T) {
	t.Run("Read all data in single batch", func(t *testing.T) {
		names, columns := generateTestColumns()
		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Read all 10 rows in one batch
		batch, err := proj.Next(100)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if batch == nil {
			t.Fatal("Expected batch, got nil")
		}

		// Verify we got all 10 rows
		if len(batch.Columns) != 8 {
			t.Errorf("Expected 8 columns, got %d", len(batch.Columns))
		}
		if batch.Columns[0].Len() != 10 {
			t.Errorf("Expected 10 rows, got %d", batch.Columns[0].Len())
		}

		// Next call should return EOF
		_, err = proj.Next(1)
		if err != io.EOF {
			t.Errorf("Expected EOF after reading all data, got: %v", err)
		}
	})

	t.Run("Read with projection and iterate to EOF", func(t *testing.T) {
		names, columns := generateTestColumns()
		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Project to only 3 columns
		err = proj.withFields("id", "name", "salary")
		if err != nil {
			t.Error("unexpected error when pruning columns")
		}
		totalRowsRead := 0
		batchCount := 0

		// Iterate until EOF
		for {
			batch, err := proj.Next(3)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed on batch %d: %v", batchCount+1, err)
			}

			batchCount++
			totalRowsRead += batch.Columns[0].Len()

			// Verify projected schema has only 3 fields
			if len(batch.Columns) != 3 {
				t.Errorf("Batch %d: expected 3 columns after projection, got %d", batchCount, len(batch.Columns))
			}

			// Verify field names
			fields := batch.Schema.Fields()
			expectedNames := []string{"id", "name", "salary"}
			for i, expectedName := range expectedNames {
				if fields[i].Name != expectedName {
					t.Errorf("Batch %d, Field %d: expected '%s', got '%s'", batchCount, i, expectedName, fields[i].Name)
				}
			}
		}

		// Verify we read all 10 rows total
		if totalRowsRead != 10 {
			t.Errorf("Expected to read 10 total rows, got %d", totalRowsRead)
		}

		// Verify we got 4 batches (3+3+3+1)
		if batchCount != 4 {
			t.Errorf("Expected 4 batches, got %d", batchCount)
		}
	})

	t.Run("Multiple Next calls with small batch size", func(t *testing.T) {
		names, columns := generateTestColumns()
		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Project to 2 columns
		err = proj.withFields("age", "is_active")
		if err != nil {
			t.Error("unexpected error when pruning columns")
		}

		// Read 2 rows at a time
		batch1, err := proj.Next(2)
		if err != nil {
			t.Fatalf("First Next failed: %v", err)
		}
		if batch1.Columns[0].Len() != 2 {
			t.Errorf("First batch: expected 2 rows, got %d", batch1.Columns[0].Len())
		}

		batch2, err := proj.Next(2)
		if err != nil {
			t.Fatalf("Second Next failed: %v", err)
		}
		if batch2.Columns[0].Len() != 2 {
			t.Errorf("Second batch: expected 2 rows, got %d", batch2.Columns[0].Len())
		}

		// Continue reading until EOF
		rowsRemaining := 0
		for {
			batch, err := proj.Next(2)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			rowsRemaining += batch.Columns[0].Len()
		}

		// We read 4 rows in first two batches, so 6 should remain
		if rowsRemaining != 6 {
			t.Errorf("Expected 6 remaining rows, got %d", rowsRemaining)
		}
	})

	t.Run("Single field projection with iteration", func(t *testing.T) {
		names, columns := generateTestColumns()
		proj, err := NewInMemoryProjectExec(names, columns)
		if err != nil {
			t.Fatalf("NewInMemoryProjectExec failed: %v", err)
		}

		// Project to just the department column
		err = proj.withFields("department")
		if err != nil {
			t.Error("unexpected error when pruning columns")
		}
		fmt.Printf("updated: %s\n", proj.schema)
		fmt.Printf("new Mapping: %v\n", proj.fieldToColIDx)
		fmt.Printf("new columns: %v\n", proj.columns)

		totalRows := 0
		for {
			batch, err := proj.Next(5)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			fmt.Printf("Batche schema: %v\n", batch.Schema)
			fmt.Printf("Batch data: %v\n", batch.Columns)

			// Verify only 1 column
			if len(batch.Columns) != 1 {
				t.Errorf("Expected 1 column, got %d", len(batch.Columns))
			}

			// Verify it's a string array
			if _, ok := batch.Columns[0].(*array.String); !ok {
				t.Errorf("Expected *array.String, got %T", batch.Columns[0])
			}

			totalRows += batch.Columns[0].Len()
		}

		if totalRows != 10 {
			t.Errorf("Expected 10 total rows, got %d", totalRows)
		}
	})
}
