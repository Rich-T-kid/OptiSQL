package operators

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// generateDummyRecordBatch1 creates a test RecordBatch with employee data
func generateDummyRecordBatch1() RecordBatch {
	dummyBuilder := NewRecordBatchBuilder()
	dummyBuilder.SchemaBuilder.WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("salary", arrow.PrimitiveTypes.Float64, false)

	columns := []arrow.Array{
		dummyBuilder.GenIntArray(1, 2, 3, 4, 5),
		dummyBuilder.GenStringArray("Alice", "Bob", "Charlie", "David", "Eve"),
		dummyBuilder.GenIntArray(25, 30, 35, 40, 45),
		dummyBuilder.GenFloatArray(50000.0, 60000.0, 70000.0, 80000.0, 90000.0),
	}
	RecordBatch, _ := dummyBuilder.NewRecordBatch(dummyBuilder.Schema(), columns)
	return *RecordBatch
}

// generateDummyRecordBatch2 creates a test RecordBatch with product data
func generateDummyRecordBatch2() RecordBatch {
	dummyBuilder := NewRecordBatchBuilder()

	// Define a different schema
	dummyBuilder.SchemaBuilder.
		WithField("product_id", arrow.PrimitiveTypes.Int32, false).
		WithField("product_name", arrow.BinaryTypes.String, false).
		WithField("quantity", arrow.PrimitiveTypes.Int32, false).
		WithField("price", arrow.PrimitiveTypes.Float64, false).
		WithField("in_stock", arrow.FixedWidthTypes.Boolean, false)

	// Generate dummy columns
	columns := []arrow.Array{
		dummyBuilder.GenIntArray(101, 102, 103, 104, 105),
		dummyBuilder.GenStringArray("Keyboard", "Mouse", "Monitor", "Laptop", "Headphones"),
		dummyBuilder.GenIntArray(10, 50, 15, 5, 20),
		dummyBuilder.GenFloatArray(49.99, 19.99, 199.99, 999.99, 79.99),
		dummyBuilder.GenBoolArray(true, true, false, true, true),
	}

	// Build the record batch
	recordBatch, _ := dummyBuilder.NewRecordBatch(dummyBuilder.Schema(), columns)
	return *recordBatch
}

// generateEmptyRecordBatch creates a RecordBatch with schema but no rows
func generateEmptyRecordBatch() RecordBatch {
	builder := NewRecordBatchBuilder()
	builder.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int32, false).
		WithField("name", arrow.BinaryTypes.String, false)

	columns := []arrow.Array{
		builder.GenIntArray(),
		builder.GenStringArray(),
	}

	recordBatch, _ := builder.NewRecordBatch(builder.Schema(), columns)
	return *recordBatch
}

// generateNullableRecordBatch creates a RecordBatch with nullable fields containing nulls
func generateNullableRecordBatch() RecordBatch {
	builder := NewRecordBatchBuilder()
	builder.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int32, true).
		WithField("value", arrow.PrimitiveTypes.Float64, true)

	// Build arrays with null values manually
	mem := memory.NewGoAllocator()

	// Int32 array with nulls
	intBuilder := array.NewInt32Builder(mem)
	intBuilder.AppendValues([]int32{1, 2, 3}, []bool{true, false, true})
	intArray := intBuilder.NewArray()

	// Float64 array with nulls
	floatBuilder := array.NewFloat64Builder(mem)
	floatBuilder.AppendValues([]float64{1.1, 2.2, 3.3}, []bool{true, true, false})
	floatArray := floatBuilder.NewArray()

	columns := []arrow.Array{intArray, floatArray}
	recordBatch, _ := builder.NewRecordBatch(builder.Schema(), columns)
	return *recordBatch
}

// TestSerializerInit verifies serializer creation and schema validation
func TestSerializerInit(t *testing.T) {
	recordBatch := generateDummyRecordBatch1()
	serializer, err := NewSerializer(recordBatch.Schema)
	if err != nil {
		t.Fatalf("Failed to initialize serializer: %v", err)
	}

	// Validate schema matches
	if !serializer.schema.Equal(recordBatch.Schema) {
		t.Fatal("Serializer schema does not match the provided schema")
	}

	// Validate schema field count
	if serializer.schema.NumFields() != 4 {
		t.Fatalf("Expected 4 fields, got %d", serializer.schema.NumFields())
	}

	// Validate field names
	expectedFields := []string{"id", "name", "age", "salary"}
	for i, expected := range expectedFields {
		if serializer.schema.Field(i).Name != expected {
			t.Errorf("Field %d: expected name %q, got %q", i, expected, serializer.schema.Field(i).Name)
		}
	}

	// Validate field types
	if serializer.schema.Field(0).Type.ID() != arrow.INT32 {
		t.Errorf("Field 'id': expected INT32 type, got %v", serializer.schema.Field(0).Type)
	}
	if serializer.schema.Field(1).Type.ID() != arrow.STRING {
		t.Errorf("Field 'name': expected STRING type, got %v", serializer.schema.Field(1).Type)
	}
}

// TestSchemaOnlySerialization tests standalone schema serialization/deserialization
func TestSchemaOnlySerialization(t *testing.T) {
	recordBatch := generateDummyRecordBatch1()
	t.Logf("original schema before serialization: %v\n", recordBatch.Schema)

	ss, err := NewSerializer(recordBatch.Schema)
	if err != nil {
		t.Fatalf("Failed to initialize serializer: %v", err)
	}

	// Serialize schema
	serializedSchema, err := ss.SerializeSchema(recordBatch.Schema)
	if err != nil {
		t.Fatalf("Schema serialization failed: %v", err)
	}
	t.Logf("serialized schema bytes length: %d\n", len(serializedSchema))

	// Deserialize schema
	deserializedSchema, err := ss.schemaFromDisk(bytes.NewBuffer(serializedSchema))
	if err != nil {
		t.Fatalf("Schema deserialization failed: %v", err)
	}

	// Validate schemas match
	if !deserializedSchema.Equal(recordBatch.Schema) {
		t.Fatal("Deserialized schema does not match the original schema")
	}
	t.Logf("schema after serialization & deserialization: %v\n", deserializedSchema)

	// Validate field properties
	for i := 0; i < recordBatch.Schema.NumFields(); i++ {
		origField := recordBatch.Schema.Field(i)
		deserField := deserializedSchema.Field(i)

		if origField.Name != deserField.Name {
			t.Errorf("Field %d name mismatch: expected %q, got %q", i, origField.Name, deserField.Name)
		}
		if origField.Type.ID() != deserField.Type.ID() {
			t.Errorf("Field %d type mismatch: expected %v, got %v", i, origField.Type, deserField.Type)
		}
		if origField.Nullable != deserField.Nullable {
			t.Errorf("Field %d nullable mismatch: expected %v, got %v", i, origField.Nullable, deserField.Nullable)
		}
	}
}

// TestSerializerSchemaValidationFails verifies schema mismatch detection
func TestSerializerSchemaValidationFails(t *testing.T) {
	// RecordBatch 1 uses schema A
	rb1 := generateDummyRecordBatch1()

	// RecordBatch 2 uses schema B (intentionally different)
	rb2 := generateDummyRecordBatch2()

	// Initialize serializer with schema from rb1
	serializer, err := NewSerializer(rb1.Schema)
	if err != nil {
		t.Fatalf("Failed to initialize serializer: %v", err)
	}

	// Verify serializer schema is correct before test
	if !serializer.schema.Equal(rb1.Schema) {
		t.Fatal("Serializer schema does not match the initial schema")
	}

	// Attempt to serialize a record batch with a DIFFERENT schema
	_, err = serializer.SerializeBatchColumns(rb2)
	if err == nil {
		t.Fatal("Expected schema validation error, but got nil")
	}

	// Make sure Schema() still returns the original serializer schema
	decoded := serializer.Schema()
	if !decoded.Equal(rb1.Schema) {
		t.Fatal("Schema() returned an incorrect schema after validation failure")
	}
}

// TestSerializeDeserializeRoundTrip performs full round-trip test with sub-tests
func TestSerializeDeserializeRoundTrip(t *testing.T) {
	rb1 := generateDummyRecordBatch1()

	t.Run("Schema Serialization", func(t *testing.T) {
		serializer, err := NewSerializer(rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to create serializer: %v", err)
		}

		// Serialize schema
		schemaBytes, err := serializer.SerializeSchema(rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to serialize schema: %v", err)
		}

		// Deserialize schema
		buf := bytes.NewBuffer(schemaBytes)
		deserializedSchema, err := serializer.schemaFromDisk(buf)
		if err != nil {
			t.Fatalf("Failed to deserialize schema: %v", err)
		}

		// Validate
		if !deserializedSchema.Equal(rb1.Schema) {
			t.Errorf("Schemas do not match after round-trip")
		}
	})

	t.Run("Columns Serialization", func(t *testing.T) {
		serializer, err := NewSerializer(rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to create serializer: %v", err)
		}

		// Serialize columns
		columnsBytes, err := serializer.SerializeBatchColumns(rb1)
		if err != nil {
			t.Fatalf("Failed to serialize columns: %v", err)
		}

		// Deserialize columns
		buf := bytes.NewBuffer(columnsBytes)
		deserializedColumns, err := serializer.DecodeRecordBatch(buf, rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to deserialize columns: %v", err)
		}

		// Validate column count
		if len(deserializedColumns) != len(rb1.Columns) {
			t.Fatalf("Expected %d columns, got %d", len(rb1.Columns), len(deserializedColumns))
		}

		// Validate each column
		for i, origCol := range rb1.Columns {
			deserCol := deserializedColumns[i]

			// Check length
			if deserCol.Len() != origCol.Len() {
				t.Errorf("Column %d: length mismatch: expected %d, got %d", i, origCol.Len(), deserCol.Len())
				continue
			}

			// Check data type
			if deserCol.DataType().ID() != origCol.DataType().ID() {
				t.Errorf("Column %d: type mismatch: expected %v, got %v", i, origCol.DataType(), deserCol.DataType())
				continue
			}

			// Validate data values based on type
			switch origCol.DataType().ID() {
			case arrow.INT32:
				origData := origCol.(*array.Int32).Int32Values()
				deserData := deserCol.(*array.Int32).Int32Values()
				for j := 0; j < len(origData); j++ {
					if origData[j] != deserData[j] {
						t.Errorf("Column %d, row %d: expected %d, got %d", i, j, origData[j], deserData[j])
					}
				}
			case arrow.FLOAT64:
				origData := origCol.(*array.Float64).Float64Values()
				deserData := deserCol.(*array.Float64).Float64Values()
				for j := 0; j < len(origData); j++ {
					if origData[j] != deserData[j] {
						t.Errorf("Column %d, row %d: expected %f, got %f", i, j, origData[j], deserData[j])
					}
				}
			case arrow.STRING:
				origArr := origCol.(*array.String)
				deserArr := deserCol.(*array.String)
				for j := 0; j < origArr.Len(); j++ {
					if origArr.Value(j) != deserArr.Value(j) {
						t.Errorf("Column %d, row %d: expected %q, got %q", i, j, origArr.Value(j), deserArr.Value(j))
					}
				}
			}
		}
	})

	t.Run("Full RecordBatch Round-Trip", func(t *testing.T) {
		serializer, err := NewSerializer(rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to create serializer: %v", err)
		}

		// Create buffer for full serialization
		var buf bytes.Buffer

		// Write schema
		schemaBytes, err := serializer.SerializeSchema(rb1.Schema)
		if err != nil {
			t.Fatalf("Failed to serialize schema: %v", err)
		}
		buf.Write(schemaBytes)

		// Write columns
		columnsBytes, err := serializer.SerializeBatchColumns(rb1)
		if err != nil {
			t.Fatalf("Failed to serialize columns: %v", err)
		}
		buf.Write(columnsBytes)

		// Read everything back
		reader := bytes.NewReader(buf.Bytes())

		// Deserialize schema
		deserSchema, err := serializer.schemaFromDisk(reader)
		if err != nil {
			t.Fatalf("Failed to deserialize schema: %v", err)
		}

		// Deserialize columns
		deserColumns, err := serializer.DecodeRecordBatch(reader, deserSchema)
		if err != nil {
			t.Fatalf("Failed to deserialize columns: %v", err)
		}

		// Create new RecordBatch
		builder := NewRecordBatchBuilder()
		deserBatch, err := builder.NewRecordBatch(deserSchema, deserColumns)
		if err != nil {
			t.Fatalf("Failed to create deserialized RecordBatch: %v", err)
		}

		// Validate
		if !deserBatch.Schema.Equal(rb1.Schema) {
			t.Errorf("Deserialized schema does not match original")
		}
		if len(deserBatch.Columns) != len(rb1.Columns) {
			t.Errorf("Column count mismatch: expected %d, got %d", len(rb1.Columns), len(deserBatch.Columns))
		}
		// Check row count by checking first column length
		if len(deserBatch.Columns) > 0 && len(rb1.Columns) > 0 {
			if deserBatch.Columns[0].Len() != rb1.Columns[0].Len() {
				t.Errorf("Row count mismatch: expected %d, got %d", rb1.Columns[0].Len(), deserBatch.Columns[0].Len())
			}
		}
	})
}

// TestEmptyRecordBatchSerialization tests edge case with zero rows
func TestEmptyRecordBatchSerialization(t *testing.T) {
	rb := generateEmptyRecordBatch()

	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}

	// Serialize
	columnsBytes, err := serializer.SerializeBatchColumns(rb)
	if err != nil {
		t.Fatalf("Failed to serialize empty batch: %v", err)
	}

	// Deserialize
	buf := bytes.NewBuffer(columnsBytes)
	deserColumns, err := serializer.DecodeRecordBatch(buf, rb.Schema)
	if err != nil {
		t.Fatalf("Failed to deserialize empty batch: %v", err)
	}

	// Validate
	if len(deserColumns) != len(rb.Columns) {
		t.Fatalf("Column count mismatch: expected %d, got %d", len(rb.Columns), len(deserColumns))
	}

	for i, col := range deserColumns {
		if col.Len() != 0 {
			t.Errorf("Column %d: expected length 0, got %d", i, col.Len())
		}
	}
}

// TestNullValuesSerialization tests nullable fields with null values
func TestNullValuesSerialization(t *testing.T) {
	rb := generateNullableRecordBatch()

	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}

	// Serialize
	columnsBytes, err := serializer.SerializeBatchColumns(rb)
	if err != nil {
		t.Fatalf("Failed to serialize nullable batch: %v", err)
	}

	// Deserialize
	buf := bytes.NewBuffer(columnsBytes)
	deserColumns, err := serializer.DecodeRecordBatch(buf, rb.Schema)
	if err != nil {
		t.Fatalf("Failed to deserialize nullable batch: %v", err)
	}

	// Validate null bitmap preservation
	for i, origCol := range rb.Columns {
		deserCol := deserColumns[i]

		if origCol.NullN() != deserCol.NullN() {
			t.Errorf("Column %d: null count mismatch: expected %d, got %d",
				i, origCol.NullN(), deserCol.NullN())
		}

		// Check each row's nullness
		for j := 0; j < origCol.Len(); j++ {
			if origCol.IsNull(j) != deserCol.IsNull(j) {
				t.Errorf("Column %d, row %d: null status mismatch: expected %v, got %v",
					i, j, origCol.IsNull(j), deserCol.IsNull(j))
			}
		}
	}
}

// TestMultipleBatchesSerialization tests writing/reading multiple batches to same buffer
func TestMultipleBatchesSerialization(t *testing.T) {
	rb1 := generateDummyRecordBatch1()
	rb2 := generateDummyRecordBatch1() // Same schema, different instance

	serializer, err := NewSerializer(rb1.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}

	var buf bytes.Buffer

	// Write schema once
	schemaBytes, err := serializer.SerializeSchema(rb1.Schema)
	if err != nil {
		t.Fatalf("Failed to serialize schema: %v", err)
	}
	buf.Write(schemaBytes)

	// Write first batch
	batch1Bytes, err := serializer.SerializeBatchColumns(rb1)
	if err != nil {
		t.Fatalf("Failed to serialize batch 1: %v", err)
	}
	buf.Write(batch1Bytes)

	// Write second batch
	batch2Bytes, err := serializer.SerializeBatchColumns(rb2)
	if err != nil {
		t.Fatalf("Failed to serialize batch 2: %v", err)
	}
	buf.Write(batch2Bytes)

	// Read back
	reader := bytes.NewReader(buf.Bytes())

	// Read schema
	schema, err := serializer.schemaFromDisk(reader)
	if err != nil {
		t.Fatalf("Failed to deserialize schema: %v", err)
	}

	// Read first batch
	cols1, err := serializer.DecodeRecordBatch(reader, schema)
	if err != nil {
		t.Fatalf("Failed to deserialize batch 1: %v", err)
	}

	// Read second batch
	cols2, err := serializer.DecodeRecordBatch(reader, schema)
	if err != nil {
		t.Fatalf("Failed to deserialize batch 2: %v", err)
	}

	// Validate both batches
	if len(cols1) != len(rb1.Columns) {
		t.Errorf("Batch 1: column count mismatch")
	}
	if len(cols2) != len(rb2.Columns) {
		t.Errorf("Batch 2: column count mismatch")
	}

	// Verify EOF after reading all batches
	_, err = serializer.DecodeRecordBatch(reader, schema)
	if err != io.EOF {
		t.Errorf("Expected EOF after reading all batches, got: %v", err)
	}
}

// TestBasicArrowTypeFromString tests type string parsing
func TestBasicArrowTypeFromString(t *testing.T) {
	// cover all supported branches plus an unsupported case
	testCases := []struct {
		typeStr    string
		expectType arrow.Type
		expectErr  bool
	}{
		{"null", arrow.NULL, false},
		{"bool", arrow.BOOL, false},

		{"int8", arrow.INT8, false},
		{"int16", arrow.INT16, false},
		{"int32", arrow.INT32, false},
		{"int64", arrow.INT64, false},

		{"uint8", arrow.UINT8, false},
		{"uint16", arrow.UINT16, false},
		{"uint32", arrow.UINT32, false},
		{"uint64", arrow.UINT64, false},

		{"float32", arrow.FLOAT32, false},
		{"float64", arrow.FLOAT64, false},

		{"string", arrow.STRING, false},
		{"utf8", arrow.STRING, false},
		{"large_string", arrow.LARGE_STRING, false},
		{"large_utf8", arrow.LARGE_STRING, false},

		{"binary", arrow.BINARY, false},
		{"large_binary", arrow.LARGE_BINARY, false},

		// unsupported type should return an error
		{"not_a_type", arrow.Type(0), true},
	}

	for _, tc := range testCases {
		t.Run(tc.typeStr, func(t *testing.T) {
			dt, err := BasicArrowTypeFromString(tc.typeStr)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for %q but got nil and dt=%v", tc.typeStr, dt)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error parsing %q: %v", tc.typeStr, err)
			}
			if dt == nil {
				t.Fatalf("type parsing returned nil for %q", tc.typeStr)
			}
			if dt.ID() != tc.expectType {
				t.Fatalf("for %q expected type %v but got %v", tc.typeStr, tc.expectType, dt.ID())
			}
		})
	}
}

// TestSerializeRecordBatchDeepEqual writes a record batch to an in-memory buffer
// (schema + columns), reads the schema back using DeserializeSchema, then
// reads the record batch and verifies DeepEqual between original and round-tripped
// RecordBatch.
func TestSerializeRecordBatchDeepEqual(t *testing.T) {
	rb := generateDummyRecordBatch1()

	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}

	var buf bytes.Buffer

	// write schema first
	schemaBytes, err := serializer.SerializeSchema(rb.Schema)
	if err != nil {
		t.Fatalf("failed to serialize schema: %v", err)
	}
	buf.Write(schemaBytes)

	// write columns
	colsBytes, err := serializer.SerializeBatchColumns(rb)
	if err != nil {
		t.Fatalf("failed to serialize columns: %v", err)
	}
	buf.Write(colsBytes)

	// now read back
	reader := bytes.NewReader(buf.Bytes())

	// DeserializeSchema first to validate schema round-trip
	deserializedSchema, err := serializer.DeserializeSchema(reader)
	if err != nil {
		t.Fatalf("DeserializeSchema failed: %v", err)
	}
	if !deserializedSchema.Equal(rb.Schema) {
		t.Fatalf("schema mismatch after DeserializeSchema: expected %v got %v", rb.Schema, deserializedSchema)
	}

	// DecodeRecordBatch reads columns from the same reader
	arrays, err := serializer.DecodeRecordBatch(reader, deserializedSchema)
	if err != nil {
		t.Fatalf("DecodeRecordBatch failed: %v", err)
	}

	// Build RecordBatch from deserialized arrays
	builder := NewRecordBatchBuilder()
	gotRB, err := builder.NewRecordBatch(deserializedSchema, arrays)
	if err != nil {
		t.Fatalf("failed to construct RecordBatch from deserialized arrays: %v", err)
	}

	if !rb.DeepEqual(gotRB) {
		t.Fatalf("original and deserialized RecordBatch differ")
	}
}

// TestDecodeRecordBatchInvalidSchema ensures DecodeRecordBatch fails when the
// provided schema does not match the serializer's schema.
func TestDecodeRecordBatchInvalidSchema(t *testing.T) {
	rb := generateDummyRecordBatch1()

	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}

	// Prepare buffer that contains only columns for rb
	colsBytes, err := serializer.SerializeBatchColumns(rb)
	if err != nil {
		t.Fatalf("failed to serialize columns: %v", err)
	}
	reader := bytes.NewReader(colsBytes)

	// Create a deliberately different schema (swap a field type)
	wrongBuilder := NewRecordBatchBuilder()
	wrongBuilder.SchemaBuilder.
		WithField("id", arrow.PrimitiveTypes.Int64, false).
		WithField("name", arrow.BinaryTypes.String, false).
		WithField("age", arrow.PrimitiveTypes.Int32, false).
		WithField("salary", arrow.PrimitiveTypes.Float64, false)
	wrongSchema := wrongBuilder.Schema()

	// Expect an ErrInvalidSchema because serializer.schema != wrongSchema
	_, err = serializer.DecodeRecordBatch(reader, wrongSchema)
	if err == nil {
		t.Fatalf("expected DecodeRecordBatch to fail due to invalid schema, but it succeeded")
	}
}

// TestSerializationWithDifferentTypes tests all supported Arrow types
func TestSerializationWithDifferentTypes(t *testing.T) {
	builder := NewRecordBatchBuilder()
	builder.SchemaBuilder.
		WithField("int32_col", arrow.PrimitiveTypes.Int32, false).
		WithField("int64_col", arrow.PrimitiveTypes.Int64, false).
		WithField("float32_col", arrow.PrimitiveTypes.Float32, false).
		WithField("float64_col", arrow.PrimitiveTypes.Float64, false).
		WithField("string_col", arrow.BinaryTypes.String, false).
		WithField("bool_col", arrow.FixedWidthTypes.Boolean, false)

	mem := memory.NewGoAllocator()

	int32Builder := array.NewInt32Builder(mem)
	int32Builder.AppendValues([]int32{1, 2, 3}, nil)
	int32Array := int32Builder.NewArray()

	int64Builder := array.NewInt64Builder(mem)
	int64Builder.AppendValues([]int64{100, 200, 300}, nil)
	int64Array := int64Builder.NewArray()

	float32Builder := array.NewFloat32Builder(mem)
	float32Builder.AppendValues([]float32{1.1, 2.2, 3.3}, nil)
	float32Array := float32Builder.NewArray()

	float64Builder := array.NewFloat64Builder(mem)
	float64Builder.AppendValues([]float64{10.1, 20.2, 30.3}, nil)
	float64Array := float64Builder.NewArray()

	stringBuilder := array.NewStringBuilder(mem)
	stringBuilder.AppendValues([]string{"a", "b", "c"}, nil)
	stringArray := stringBuilder.NewArray()

	boolBuilder := array.NewBooleanBuilder(mem)
	boolBuilder.AppendValues([]bool{true, false, true}, nil)
	boolArray := boolBuilder.NewArray()

	columns := []arrow.Array{
		int32Array, int64Array, float32Array,
		float64Array, stringArray, boolArray,
	}

	rb, err := builder.NewRecordBatch(builder.Schema(), columns)
	if err != nil {
		t.Fatalf("Failed to create RecordBatch: %v", err)
	}

	// Serialize and deserialize
	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}

	columnsBytes, err := serializer.SerializeBatchColumns(*rb)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	buf := bytes.NewBuffer(columnsBytes)
	deserColumns, err := serializer.DecodeRecordBatch(buf, rb.Schema)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Validate all columns
	if len(deserColumns) != len(columns) {
		t.Fatalf("Column count mismatch: expected %d, got %d", len(columns), len(deserColumns))
	}

	// Validate each type
	for i, deserCol := range deserColumns {
		if deserCol.DataType().ID() != columns[i].DataType().ID() {
			t.Errorf("Column %d: type mismatch: expected %v, got %v",
				i, columns[i].DataType(), deserCol.DataType())
		}
	}
}

func TestNullSchemaSerialize(t *testing.T) {
	rb := generateNullableRecordBatch()
	for i := range rb.Schema.Fields() {
		t.Logf("is nullable? : %v\n", rb.Schema.Field(i).Nullable)
	}
	serializer, err := NewSerializer(rb.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}

	// Serialize schema
	_, err = serializer.SerializeSchema(rb.Schema)
	if err != nil {
		t.Fatalf("Schema serialization failed: %v", err)
	}
}

func TestSeralizeToDisk(t *testing.T) {
	r1 := generateDummyRecordBatch1()
	serializer, err := NewSerializer(r1.Schema)
	if err != nil {
		t.Fatalf("Failed to create serializer: %v", err)
	}
	randStr := time.Now().Unix()
	tmpFile, err := os.Create("serialized_data_" + fmt.Sprintf("%d", randStr) + ".bin")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	schemaContent, _ := serializer.SerializeSchema(r1.Schema)
	columnContent, _ := serializer.SerializeBatchColumns(r1)
	schemaContent = append(schemaContent, columnContent...)
	_, err = tmpFile.Write(schemaContent)
	if err != nil {
		t.Fatalf("Failed to write serialized data to disk: %v", err)
	}
	// now decode from disk
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Failed to seek to start of file: %v", err)
	}
	deserSchema, err := serializer.DeserializeSchema(tmpFile)
	if err != nil {
		t.Fatalf("Failed to deserialize schema from disk: %v", err)
	}
	if !deserSchema.Equal(r1.Schema) {
		t.Fatalf("Deserialized schema does not match original schema")
	}
	deserColumns, err := serializer.DecodeRecordBatch(tmpFile, deserSchema)
	if err != nil {
		t.Fatalf("Failed to deserialize columns from disk: %v", err)
	}
	if len(deserColumns) != len(r1.Columns) {
		t.Fatalf("Column count mismatch after deserialization from disk")
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}
	if err := os.Remove(tmpFile.Name()); err != nil {
		t.Fatalf("Failed to remove temp file: %v", err)
	}
}
