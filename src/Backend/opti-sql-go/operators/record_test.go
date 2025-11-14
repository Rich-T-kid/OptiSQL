package operators

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// Test 1: SchemaBuilder.WithField and WithoutField
func TestSchemaBuilderWithField(t *testing.T) {
	sb := &SchemaBuilder{
		fields: make([]arrow.Field, 0, 10),
	}

	// Add fields
	sb.WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("salary", arrow.PrimitiveTypes.Float64, true)

	// Verify fields were added
	if len(sb.fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(sb.fields))
	}

	// Check field names
	expectedNames := []string{"age", "name", "salary"}
	for i, expected := range expectedNames {
		if sb.fields[i].Name != expected {
			t.Errorf("Field %d: expected name '%s', got '%s'", i, expected, sb.fields[i].Name)
		}
	}

	// Check field types
	if !arrow.TypeEqual(sb.fields[0].Type, arrow.PrimitiveTypes.Int32) {
		t.Errorf("Field 'age': expected Int32 type, got %s", sb.fields[0].Type)
	}
	if !arrow.TypeEqual(sb.fields[1].Type, arrow.BinaryTypes.String) {
		t.Errorf("Field 'name': expected String type, got %s", sb.fields[1].Type)
	}
	if !arrow.TypeEqual(sb.fields[2].Type, arrow.PrimitiveTypes.Float64) {
		t.Errorf("Field 'salary': expected Float64 type, got %s", sb.fields[2].Type)
	}

	// Check nullable flags
	if sb.fields[0].Nullable != false {
		t.Errorf("Field 'age': expected nullable=false, got %v", sb.fields[0].Nullable)
	}
	if sb.fields[2].Nullable != true {
		t.Errorf("Field 'salary': expected nullable=true, got %v", sb.fields[2].Nullable)
	}
}

func TestSchemaBuilderWithoutField(t *testing.T) {
	sb := &SchemaBuilder{
		fields: make([]arrow.Field, 0, 10),
	}

	// Add fields
	sb.WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("salary", arrow.PrimitiveTypes.Float64, true).
		WithField("active", arrow.FixedWidthTypes.Boolean, false)

	// Verify 4 fields
	if len(sb.fields) != 4 {
		t.Errorf("Expected 4 fields, got %d", len(sb.fields))
	}

	// Remove fields
	sb.WithoutField("name", "active")

	// Verify only 2 fields remain
	if len(sb.fields) != 2 {
		t.Errorf("Expected 2 fields after removal, got %d", len(sb.fields))
	}

	// Verify remaining fields are age and salary
	if sb.fields[0].Name != "age" {
		t.Errorf("Expected first field to be 'age', got '%s'", sb.fields[0].Name)
	}
	if sb.fields[1].Name != "salary" {
		t.Errorf("Expected second field to be 'salary', got '%s'", sb.fields[1].Name)
	}
}

// Test 1.5: SchemaBuilder.Build with WithField and WithoutField
func TestSchemaBuilderBuildWithFields(t *testing.T) {
	sb := &SchemaBuilder{
		fields: make([]arrow.Field, 0, 10),
	}

	// Add fields and build
	sb.WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("email", arrow.BinaryTypes.String, false).
		WithField("score", arrow.PrimitiveTypes.Float64, true)

	schema := sb.Build()

	// Validate schema has 3 fields
	if schema.NumFields() != 3 {
		t.Errorf("Expected schema with 3 fields, got %d", schema.NumFields())
	}

	// Validate field names
	field0 := schema.Field(0)
	if field0.Name != "id" {
		t.Errorf("Expected field 0 name 'id', got '%s'", field0.Name)
	}

	field1 := schema.Field(1)
	if field1.Name != "email" {
		t.Errorf("Expected field 1 name 'email', got '%s'", field1.Name)
	}

	field2 := schema.Field(2)
	if field2.Name != "score" {
		t.Errorf("Expected field 2 name 'score', got '%s'", field2.Name)
	}

	// Validate types
	if !arrow.TypeEqual(field0.Type, arrow.PrimitiveTypes.Int32) {
		t.Errorf("Expected field 'id' type Int32, got %s", field0.Type)
	}
	if !arrow.TypeEqual(field1.Type, arrow.BinaryTypes.String) {
		t.Errorf("Expected field 'email' type String, got %s", field1.Type)
	}
	if !arrow.TypeEqual(field2.Type, arrow.PrimitiveTypes.Float64) {
		t.Errorf("Expected field 'score' type Float64, got %s", field2.Type)
	}
}

func TestSchemaBuilderBuildWithFieldsRemoved(t *testing.T) {
	sb := &SchemaBuilder{
		fields: make([]arrow.Field, 0, 10),
	}

	// Add fields, remove some, then build
	sb.WithField("a", arrow.PrimitiveTypes.Int32, false).
		WithField("b", arrow.BinaryTypes.String, false).
		WithField("c", arrow.PrimitiveTypes.Float64, false).
		WithField("d", arrow.FixedWidthTypes.Boolean, false).
		WithoutField("b", "d")
	schema := sb.Build()

	// Validate schema has only 2 fields (a and c)
	if schema.NumFields() != 2 {
		t.Errorf("Expected schema with 2 fields after removal, got %d", schema.NumFields())
	}

	// Validate remaining field names
	if schema.Field(0).Name != "a" {
		t.Errorf("Expected field 0 name 'a', got '%s'", schema.Field(0).Name)
	}
	if schema.Field(1).Name != "c" {
		t.Errorf("Expected field 1 name 'c', got '%s'", schema.Field(1).Name)
	}
}

// Test 2: GenDataTypeArray functions
func TestGenIntArray(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	arr := rbb.GenIntArray(10, 20, 30, 40)
	defer arr.Release()

	// Check length
	if arr.Len() != 4 {
		t.Errorf("Expected array length 4, got %d", arr.Len())
	}

	// Check type
	if !arrow.TypeEqual(arr.DataType(), arrow.PrimitiveTypes.Int32) {
		t.Errorf("Expected Int32 type, got %s", arr.DataType())
	}

	// Check values
	int32Arr := arr.(*array.Int32)
	expectedValues := []int32{10, 20, 30, 40}
	for i, expected := range expectedValues {
		if int32Arr.Value(i) != expected {
			t.Errorf("Index %d: expected %d, got %d", i, expected, int32Arr.Value(i))
		}
	}
}

func TestGenFloatArray(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	arr := rbb.GenFloatArray(1.5, 2.7, 3.14, 9.99)
	defer arr.Release()

	// Check length
	if arr.Len() != 4 {
		t.Errorf("Expected array length 4, got %d", arr.Len())
	}

	// Check type
	if !arrow.TypeEqual(arr.DataType(), arrow.PrimitiveTypes.Float64) {
		t.Errorf("Expected Float64 type, got %s", arr.DataType())
	}

	// Check values
	float64Arr := arr.(*array.Float64)
	expectedValues := []float64{1.5, 2.7, 3.14, 9.99}
	for i, expected := range expectedValues {
		if float64Arr.Value(i) != expected {
			t.Errorf("Index %d: expected %f, got %f", i, expected, float64Arr.Value(i))
		}
	}
}

func TestGenStringArray(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	arr := rbb.GenStringArray("Alice", "Bob", "Charlie")
	defer arr.Release()

	// Check length
	if arr.Len() != 3 {
		t.Errorf("Expected array length 3, got %d", arr.Len())
	}

	// Check type
	if !arrow.TypeEqual(arr.DataType(), arrow.BinaryTypes.String) {
		t.Errorf("Expected String type, got %s", arr.DataType())
	}

	// Check values
	stringArr := arr.(*array.String)
	expectedValues := []string{"Alice", "Bob", "Charlie"}
	for i, expected := range expectedValues {
		if stringArr.Value(i) != expected {
			t.Errorf("Index %d: expected '%s', got '%s'", i, expected, stringArr.Value(i))
		}
	}
}

func TestGenBoolArray(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	arr := rbb.GenBoolArray(true, false, true, true)
	defer arr.Release()

	// Check length
	if arr.Len() != 4 {
		t.Errorf("Expected array length 4, got %d", arr.Len())
	}

	// Check type
	if !arrow.TypeEqual(arr.DataType(), arrow.FixedWidthTypes.Boolean) {
		t.Errorf("Expected Boolean type, got %s", arr.DataType())
	}

	// Check values
	boolArr := arr.(*array.Boolean)
	expectedValues := []bool{true, false, true, true}
	for i, expected := range expectedValues {
		if boolArr.Value(i) != expected {
			t.Errorf("Index %d: expected %v, got %v", i, expected, boolArr.Value(i))
		}
	}
}

// Test 3: Validate function
func TestValidateIncorrectColumnTypes(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema expecting Int32 and String
	schema := rbb.SchemaBuilder.
		WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		Build()

	// Create columns with wrong types (Float64 instead of Int32)
	wrongCol := rbb.GenFloatArray(1.5, 2.5, 3.5)
	defer wrongCol.Release()
	nameCol := rbb.GenStringArray("Alice", "Bob", "Charlie")
	defer nameCol.Release()

	// Validate should fail
	err := rbb.validate(schema, []arrow.Array{wrongCol, nameCol})
	if err == nil {
		t.Error("Expected validation error for incorrect column type, got nil")
	}
}

func TestValidateMismatchedColumnCount(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema with 2 fields
	schema := rbb.SchemaBuilder.
		WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		Build()

	// Create only 1 column
	ageCol := rbb.GenIntArray(25, 30, 22)
	defer ageCol.Release()

	// Validate should fail
	err := rbb.validate(schema, []arrow.Array{ageCol})
	if err == nil {
		t.Error("Expected validation error for column count mismatch, got nil")
	}
}

func TestValidateCorrectSchemaAndColumns(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema
	schema := rbb.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("score", arrow.PrimitiveTypes.Float64, false).
		Build()

	// Create matching columns
	idCol := rbb.GenIntArray(1, 2, 3)
	defer idCol.Release()
	scoreCol := rbb.GenFloatArray(95.5, 87.3, 92.1)
	defer scoreCol.Release()

	// Validate should pass
	err := rbb.validate(schema, []arrow.Array{idCol, scoreCol})
	if err != nil {
		t.Errorf("Expected validation to pass, got error: %v", err)
	}
}

// Test 4: NewRecordBatch function
func TestNewRecordBatchSuccess(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema
	schema := rbb.SchemaBuilder.
		WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		Build()

	// Create matching columns
	ageCol := rbb.GenIntArray(25, 30, 22)
	defer ageCol.Release()
	nameCol := rbb.GenStringArray("Alice", "Bob", "Charlie")
	defer nameCol.Release()

	// Create RecordBatch
	rb, err := rbb.NewRecordBatch(schema, []arrow.Array{ageCol, nameCol})
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Validate RecordBatch
	if rb == nil {
		t.Fatal("Expected non-nil RecordBatch")
	}
	if rb.Schema.NumFields() != 2 {
		t.Errorf("Expected schema with 2 fields, got %d", rb.Schema.NumFields())
	}
	if len(rb.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rb.Columns))
	}
}

func TestNewRecordBatchMisalignedSchema(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema with 3 fields
	schema := rbb.SchemaBuilder.
		WithField("a", arrow.PrimitiveTypes.Int32, false).
		WithField("b", arrow.BinaryTypes.String, false).
		WithField("c", arrow.PrimitiveTypes.Float64, false).
		Build()

	// Create only 2 columns
	col1 := rbb.GenIntArray(1, 2, 3)
	defer col1.Release()
	col2 := rbb.GenStringArray("x", "y", "z")
	defer col2.Release()

	// Should fail
	_, err := rbb.NewRecordBatch(schema, []arrow.Array{col1, col2})
	if err == nil {
		t.Error("Expected error for misaligned schema and columns, got nil")
	}
}

func TestNewRecordBatchIncorrectDataTypes(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create schema expecting Int32 and Float64
	schema := rbb.SchemaBuilder.
		WithField("count", arrow.PrimitiveTypes.Int32, false).
		WithField("value", arrow.PrimitiveTypes.Float64, false).
		Build()

	// Create columns with wrong types (both String)
	col1 := rbb.GenStringArray("1", "2", "3")
	defer col1.Release()
	col2 := rbb.GenStringArray("4.5", "5.5", "6.5")
	defer col2.Release()

	// Should fail
	_, err := rbb.NewRecordBatch(schema, []arrow.Array{col1, col2})
	if err == nil {
		t.Error("Expected error for incorrect data types, got nil")
	}
}

// Test 5: Integration test - Full workflow
func TestRecordBatchBuilderIntegration(t *testing.T) {
	// Create builder
	rbb := NewRecordBatchBuilder()

	// Build schema with multiple fields
	rbb.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("score", arrow.PrimitiveTypes.Float64, true).
		WithField("active", arrow.FixedWidthTypes.Boolean, false).
		WithField("temp", arrow.BinaryTypes.String, false). // Will be removed
		WithoutField("temp")                                // Remove it

	schema := rbb.Schema()

	// Verify schema has 4 fields (temp was removed)
	if schema.NumFields() != 4 {
		t.Fatalf("Expected 4 fields in schema, got %d", schema.NumFields())
	}

	// Generate data arrays
	idCol := rbb.GenIntArray(1, 2, 3, 4, 5)
	defer idCol.Release()

	nameCol := rbb.GenStringArray("Alice", "Bob", "Charlie", "David", "Eve")
	defer nameCol.Release()

	scoreCol := rbb.GenFloatArray(95.5, 87.3, 92.1, 88.0, 91.5)
	defer scoreCol.Release()

	activeCol := rbb.GenBoolArray(true, false, true, true, false)
	defer activeCol.Release()

	// Verify array lengths
	if idCol.Len() != 5 {
		t.Errorf("Expected idCol length 5, got %d", idCol.Len())
	}

	// Create RecordBatch
	rb, err := rbb.NewRecordBatch(schema, []arrow.Array{idCol, nameCol, scoreCol, activeCol})
	if err != nil {
		t.Fatalf("Failed to create RecordBatch: %v", err)
	}

	// Validate RecordBatch structure
	if rb.Schema.NumFields() != 4 {
		t.Errorf("Expected RecordBatch schema with 4 fields, got %d", rb.Schema.NumFields())
	}
	if len(rb.Columns) != 4 {
		t.Errorf("Expected RecordBatch with 4 columns, got %d", len(rb.Columns))
	}

	// Validate field names in order
	expectedFieldNames := []string{"id", "name", "score", "active"}
	for i, expectedName := range expectedFieldNames {
		actualName := rb.Schema.Field(i).Name
		if actualName != expectedName {
			t.Errorf("Field %d: expected name '%s', got '%s'", i, expectedName, actualName)
		}
	}

	// Validate column data types
	if !arrow.TypeEqual(rb.Columns[0].DataType(), arrow.PrimitiveTypes.Int32) {
		t.Errorf("Column 0: expected Int32, got %s", rb.Columns[0].DataType())
	}
	if !arrow.TypeEqual(rb.Columns[1].DataType(), arrow.BinaryTypes.String) {
		t.Errorf("Column 1: expected String, got %s", rb.Columns[1].DataType())
	}
	if !arrow.TypeEqual(rb.Columns[2].DataType(), arrow.PrimitiveTypes.Float64) {
		t.Errorf("Column 2: expected Float64, got %s", rb.Columns[2].DataType())
	}
	if !arrow.TypeEqual(rb.Columns[3].DataType(), arrow.FixedWidthTypes.Boolean) {
		t.Errorf("Column 3: expected Boolean, got %s", rb.Columns[3].DataType())
	}

	// Validate some actual values
	idArr := rb.Columns[0].(*array.Int32)
	if idArr.Value(0) != 1 || idArr.Value(4) != 5 {
		t.Errorf("ID column values incorrect")
	}

	nameArr := rb.Columns[1].(*array.String)
	if nameArr.Value(0) != "Alice" || nameArr.Value(2) != "Charlie" {
		t.Errorf("Name column values incorrect")
	}

	scoreArr := rb.Columns[2].(*array.Float64)
	if scoreArr.Value(0) != 95.5 || scoreArr.Value(1) != 87.3 {
		t.Errorf("Score column values incorrect")
	}

	activeArr := rb.Columns[3].(*array.Boolean)
	if activeArr.Value(0) != true || activeArr.Value(1) != false {
		t.Errorf("Active column values incorrect")
	}

	// Test error case: try to create RecordBatch with mismatched columns
	wrongCol := rbb.GenFloatArray(1.1, 2.2, 3.3, 4.4, 5.5)
	defer wrongCol.Release()

	_, err = rbb.NewRecordBatch(schema, []arrow.Array{wrongCol, nameCol, scoreCol, activeCol})
	if err == nil {
		t.Error("Expected error when creating RecordBatch with wrong column type, got nil")
	}
}

// TestRecordBatchDeepEqual tests every branch of the DeepEqual method
func TestRecordBatchDeepEqual(t *testing.T) {
	rbb := NewRecordBatchBuilder()

	// Create a base schema and RecordBatch for testing
	schema1 := rbb.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("score", arrow.PrimitiveTypes.Float64, false).
		Build()

	idCol := rbb.GenIntArray(1, 2, 3)
	nameCol := rbb.GenStringArray("Alice", "Bob", "Charlie")
	scoreCol := rbb.GenFloatArray(95.5, 87.3, 92.1)

	rb1, err := rbb.NewRecordBatch(schema1, []arrow.Array{idCol, nameCol, scoreCol})
	if err != nil {
		t.Fatalf("Failed to create base RecordBatch: %v", err)
	}

	// Mini test 1: Schema inequality - should hit "if !rb.Schema.Equal(other.Schema) { return false }"
	t.Run("MiniTest_SchemaNotEqual", func(t *testing.T) {
		builderA := NewRecordBatchBuilder()
		diffSchema := builderA.SchemaBuilder.
			WithField("different", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()
		colA1 := builderA.GenIntArray(1, 2, 3)
		colA2 := builderA.GenStringArray("Alice", "Bob", "Charlie")
		colA3 := builderA.GenFloatArray(95.5, 87.3, 92.1)
		rbA, _ := builderA.NewRecordBatch(diffSchema, []arrow.Array{colA1, colA2, colA3})
		if rb1.DeepEqual(rbA) {
			t.Error("DeepEqual should return false when schemas differ")
		}
	})

	// Mini test 2: Column count inequality - should hit "if len(rb.Columns) != len(other.Columns) { return false }"
	t.Run("MiniTest_ColumnCountNotEqual", func(t *testing.T) {
		builderB := NewRecordBatchBuilder()
		schemaB := builderB.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			Build()
		colB1 := builderB.GenIntArray(1, 2, 3)
		colB2 := builderB.GenStringArray("Alice", "Bob", "Charlie")
		rbB, _ := builderB.NewRecordBatch(schemaB, []arrow.Array{colB1, colB2})
		if rb1.DeepEqual(rbB) {
			t.Error("DeepEqual should return false when column counts differ")
		}
	})

	// Mini test 3: Array inequality in loop - should hit "if !array.Equal(rb.Columns[i], other.Columns[i]) { return false }"
	t.Run("MiniTest_ArrayNotEqual", func(t *testing.T) {
		builderC := NewRecordBatchBuilder()
		schemaC := builderC.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()
		colC1 := builderC.GenIntArray(999, 888, 777) // Different data
		colC2 := builderC.GenStringArray("Alice", "Bob", "Charlie")
		colC3 := builderC.GenFloatArray(95.5, 87.3, 92.1)
		rbC, _ := builderC.NewRecordBatch(schemaC, []arrow.Array{colC1, colC2, colC3})
		if rb1.DeepEqual(rbC) {
			t.Error("DeepEqual should return false when array data differs")
		}
	})

	// Mini test 4: All conditions pass - should hit final "return true"
	t.Run("MiniTest_AllEqual", func(t *testing.T) {
		builderD := NewRecordBatchBuilder()
		schemaD := builderD.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()
		colD1 := builderD.GenIntArray(1, 2, 3)
		colD2 := builderD.GenStringArray("Alice", "Bob", "Charlie")
		colD3 := builderD.GenFloatArray(95.5, 87.3, 92.1)
		rbD, _ := builderD.NewRecordBatch(schemaD, []arrow.Array{colD1, colD2, colD3})
		if !rb1.DeepEqual(rbD) {
			t.Error("DeepEqual should return true when all conditions match")
		}
	})

	// Test case 1: Identical RecordBatches should be equal (tests the happy path - return true)
	t.Run("Identical RecordBatches", func(t *testing.T) {
		builder1 := NewRecordBatchBuilder()
		schema1a := builder1.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()

		idCol1a := builder1.GenIntArray(1, 2, 3)
		nameCol1a := builder1.GenStringArray("Alice", "Bob", "Charlie")
		scoreCol1a := builder1.GenFloatArray(95.5, 87.3, 92.1)

		rb1a, err := builder1.NewRecordBatch(schema1a, []arrow.Array{idCol1a, nameCol1a, scoreCol1a})
		if err != nil {
			t.Fatalf("Failed to create identical RecordBatch: %v", err)
		}

		if !rb1.DeepEqual(rb1a) {
			t.Error("Expected identical RecordBatches to be equal")
		}
	})

	// Test case 2: Different schemas should return false (tests first if branch)
	t.Run("Different Schemas", func(t *testing.T) {
		builder2 := NewRecordBatchBuilder()
		// Different schema: different field name
		differentSchema := builder2.SchemaBuilder.
			WithField("user_id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()

		idCol2 := builder2.GenIntArray(1, 2, 3)
		nameCol2 := builder2.GenStringArray("Alice", "Bob", "Charlie")
		scoreCol2 := builder2.GenFloatArray(95.5, 87.3, 92.1)

		rb2, err := builder2.NewRecordBatch(differentSchema, []arrow.Array{idCol2, nameCol2, scoreCol2})
		if err != nil {
			t.Fatalf("Failed to create RecordBatch with different schema: %v", err)
		}

		if rb1.DeepEqual(rb2) {
			t.Error("Expected RecordBatches with different schemas to not be equal")
		}
	})

	// Test case 3: Different number of columns should return false (tests second if branch)
	t.Run("Different Column Count", func(t *testing.T) {
		builder3 := NewRecordBatchBuilder()
		// Schema with only 2 fields instead of 3
		fewerFieldsSchema := builder3.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			Build()

		idCol3 := builder3.GenIntArray(1, 2, 3)
		nameCol3 := builder3.GenStringArray("Alice", "Bob", "Charlie")

		rb3, err := builder3.NewRecordBatch(fewerFieldsSchema, []arrow.Array{idCol3, nameCol3})
		if err != nil {
			t.Fatalf("Failed to create RecordBatch with fewer columns: %v", err)
		}

		if rb1.DeepEqual(rb3) {
			t.Error("Expected RecordBatches with different column counts to not be equal")
		}
	})

	// Test case 4: Same schema and column count, but different column data should return false
	// (tests the for loop and array.Equal returning false)
	t.Run("Different Column Data", func(t *testing.T) {
		builder4 := NewRecordBatchBuilder()
		schema4 := builder4.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()

		// Different data in the first column
		idCol4 := builder4.GenIntArray(10, 20, 30) // Different values
		nameCol4 := builder4.GenStringArray("Alice", "Bob", "Charlie")
		scoreCol4 := builder4.GenFloatArray(95.5, 87.3, 92.1)

		rb4, err := builder4.NewRecordBatch(schema4, []arrow.Array{idCol4, nameCol4, scoreCol4})
		if err != nil {
			t.Fatalf("Failed to create RecordBatch with different data: %v", err)
		}

		if rb1.DeepEqual(rb4) {
			t.Error("Expected RecordBatches with different column data to not be equal")
		}
	})

	// Test case 5: Different data in middle column (tests for loop continues to check all columns)
	t.Run("Different Middle Column Data", func(t *testing.T) {
		builder5 := NewRecordBatchBuilder()
		schema5 := builder5.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()

		idCol5 := builder5.GenIntArray(1, 2, 3)
		nameCol5 := builder5.GenStringArray("Dave", "Eve", "Frank") // Different names
		scoreCol5 := builder5.GenFloatArray(95.5, 87.3, 92.1)

		rb5, err := builder5.NewRecordBatch(schema5, []arrow.Array{idCol5, nameCol5, scoreCol5})
		if err != nil {
			t.Fatalf("Failed to create RecordBatch with different middle column: %v", err)
		}

		if rb1.DeepEqual(rb5) {
			t.Error("Expected RecordBatches with different middle column to not be equal")
		}
	})

	// Test case 6: Different data in last column (tests for loop completes and finds inequality)
	t.Run("Different Last Column Data", func(t *testing.T) {
		builder6 := NewRecordBatchBuilder()
		schema6 := builder6.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			WithField("name", arrow.BinaryTypes.String, false).
			WithField("score", arrow.PrimitiveTypes.Float64, false).
			Build()

		idCol6 := builder6.GenIntArray(1, 2, 3)
		nameCol6 := builder6.GenStringArray("Alice", "Bob", "Charlie")
		scoreCol6 := builder6.GenFloatArray(100.0, 100.0, 100.0) // Different scores

		rb6, err := builder6.NewRecordBatch(schema6, []arrow.Array{idCol6, nameCol6, scoreCol6})
		if err != nil {
			t.Fatalf("Failed to create RecordBatch with different last column: %v", err)
		}

		if rb1.DeepEqual(rb6) {
			t.Error("Expected RecordBatches with different last column to not be equal")
		}
	})

	// Test case 7: Same RecordBatch compared to itself (tests reflexivity)
	t.Run("Same RecordBatch Instance", func(t *testing.T) {
		if !rb1.DeepEqual(rb1) {
			t.Error("Expected RecordBatch to be equal to itself")
		}
	})

	// Test case 8: Empty RecordBatches should be equal
	t.Run("Empty RecordBatches", func(t *testing.T) {
		builder8a := NewRecordBatchBuilder()
		emptySchema8a := builder8a.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			Build()
		emptyCol8a := builder8a.GenIntArray()
		rb_empty8a, err := builder8a.NewRecordBatch(emptySchema8a, []arrow.Array{emptyCol8a})
		if err != nil {
			t.Fatalf("Failed to create empty RecordBatch 1: %v", err)
		}

		builder8b := NewRecordBatchBuilder()
		emptySchema8b := builder8b.SchemaBuilder.
			WithField("id", arrow.PrimitiveTypes.Int32, false).
			Build()
		emptyCol8b := builder8b.GenIntArray()

		rb_empty8b, err := builder8b.NewRecordBatch(emptySchema8b, []arrow.Array{emptyCol8b})
		if err != nil {
			t.Fatalf("Failed to create empty RecordBatch 2: %v", err)
		}

		if !rb_empty8a.DeepEqual(rb_empty8b) {
			t.Error("Expected empty RecordBatches to be equal")
		}
	})
}
