package operators

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
Protocol |
Schema
┌──────────────────────────────────────────┐
│ uint32      numberOfFields               │
├──────────────────────────────────────────┤
│ uint32      field1NameLength             │
│ bytes[...]  field1Name                   │
│ uint32      field1TypeLength             │
│ bytes[...]  field1TypeString             │
│ uint8       field1Nullable               │
├──────────────────────────────────────────┤
│ uint32      field2NameLength             │
│ bytes[...]  field2Name                   │
│ uint32      field2TypeLength             │
│ bytes[...]  field2TypeString             │
│ uint8       field2Nullable               │
├──────────────────────────────────────────┤
│ ... repeated for N fields ...            │
└──────────────────────────────────────────┘
Example:
[int32 nameLength][‘age’]
[int32 typeLength][‘int32’]
[byte nullable]


Column Data
┌──────────────────────────────────────────┐
│ int64    lengthOfArray (num rows)        │
├──────────────────────────────────────────┤
│ uint32   numBuffers                      │
├──────────────────────────────────────────┤
│ uint64   buffer0Length                   │
│ bytes[]  buffer0Bytes                    │
├──────────────────────────────────────────┤
│ uint64   buffer1Length                   │
│ bytes[]  buffer1Bytes                    │
├──────────────────────────────────────────┤
│ ... repeated for N buffers ...           │
└──────────────────────────────────────────┘
Int32 example (5 elements, validity + values buffers):
[5]   // array length
[2]   // numBuffers = 2

// validity buffer
[1]                 // length (in bytes)
[0b11111...]        // raw bitmap byte

// values buffer
[20]                // length = 5 × 4 bytes
[raw binary ints…]


Record Batch
┌──────────────────────────────────────────┐
schemaBlock
column0Block
column1Block
column2Block
...
(column blocks for batch 1)
column0Block
column1Block
column2Block
...
(column blocks for batch 2)
...
└──────────────────────────────────────────┘


Record Batch Serialization Protocol
PURPOSE
This protocol defines how to serialize intermediate record batches to disk for pipeline-breaking
operators (sort, join, aggregation) where all records won't fit in RAM.
KEY ASSUMPTIONS

All batches share the SAME SCHEMA within a single operation
Examples:

sort(col) - each element is the exact same type
join(col1 == col2) - keep left side in memory/separate file, right side has same schema


Schema is kept IN MEMORY attached to the serialization handler for validation



READING PROCEDURE

Read schema block first (done once at start)
For each record batch:

Read numberOfFields from in-memory schema
For each column:

Read columnSize
Read columnData
Validate data type against in-memory schema for correct encoding




Schema tells you exactly how many columns to read per batch

IMPORTANT NOTES

Schema is written to disk ONLY for validation against in-memory schema
Between reading each column, check in-memory schema for data type encoding
This trades more disk space for safety and clarity
The interface is implemented by a struct rather than attached to RecordBatch directly
to save allocations (especially for multiple spills: sort, hash join, aggregation)

==================================
LOOSE NOTES / DEVELOPMENT HISTORY
V1 Issues:
Format was: dataTypeSize|dataType|BatchSize|BatchElements|BatchSize|BatchElements...
Problems:

Only handled single column
What about sort order?
What happens to other columns while writing single column to disk?
Conclusion: Must write entire record batch to disk

V2 Improvements:

Write schema to disk for validation
Check schema between reading each column
Schema indicates how many columns per batch
Accept that we're writing more data - it's worth it for correctness
*/

type serializer struct {
	schema *arrow.Schema // schema is always attached to the serializer
}

func NewSerializer(schema *arrow.Schema) (*serializer, error) {
	return &serializer{
		schema: schema,
	}, nil
}
func (s *serializer) Schema() *arrow.Schema {
	return s.schema
}

// overwrite the input schema here. it should be the same but in the case where its not id rather the input schema be the source of truth
func (ss *serializer) SerializeBatchColumns(r RecordBatch) ([]byte, error) {
	if !ss.schema.Equal(r.Schema) {
		return nil, ErrInvalidSchema("serializer schema and record batch schema are not aligned")
	}
	columnContent, err := ss.columnsTodisk(r.Columns)
	if err != nil {
		return nil, err
	}
	return columnContent, nil
}
func (ss *serializer) SerializeSchema(s *arrow.Schema) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. number of fields
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s.Fields()))); err != nil {
		return nil, err
	}

	for _, f := range s.Fields() {
		// --- Field Name ---
		nameBytes := []byte(f.Name)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, err
		}

		// --- Field Type (use Arrow's string representation) ---
		typeBytes := []byte(f.Type.String())
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(typeBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(typeBytes); err != nil {
			return nil, err
		}

		// --- Nullable ---
		var nullable uint8
		if f.Nullable {
			nullable = 1
		} else {
			nullable = 0
		}
		if err := binary.Write(buf, binary.LittleEndian, nullable); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
func (ss *serializer) columnsTodisk(columns []arrow.Array) ([]byte, error) {
	buf := new(bytes.Buffer)

	for _, col := range columns {
		data := col.Data()

		// Write array length (number of rows)
		if err := binary.Write(buf, binary.LittleEndian, int64(data.Len())); err != nil {
			return nil, err
		}

		// Number of buffers for this column
		buffers := data.Buffers()
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(buffers))); err != nil {
			return nil, err
		}

		// Write each buffer
		for _, b := range buffers {
			if b == nil || b.Len() == 0 {
				// Write 0 length
				if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
					return nil, err
				}
				continue
			}

			// Write length of buffer
			if err := binary.Write(buf, binary.LittleEndian, uint64(b.Len())); err != nil {
				return nil, err
			}

			// Write buffer contents
			if _, err := buf.Write(b.Bytes()); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func (ss *serializer) DeserializeSchema(data io.Reader) (*arrow.Schema, error) {
	// read in the schema first
	return ss.schemaFromDisk(data)
}

// after reading in the schema we read in one column at a time
func (ss *serializer) DeserializeNextColumn(r io.Reader, dt arrow.DataType) (arrow.Array, error) {
	// 1. Read the number of elements in this column batch
	var length int64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// 2. Read number of buffers for this column
	var numBuffers uint32
	if err := binary.Read(r, binary.LittleEndian, &numBuffers); err != nil {
		return nil, err
	}

	buffers := make([]*memory.Buffer, numBuffers)

	// 3. Read each buffer in order
	for i := uint32(0); i < numBuffers; i++ {
		// buffer length
		var size uint64
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			return nil, err
		}

		if size == 0 {
			// Null / empty buffer
			buffers[i] = nil
			continue
		}

		// Read raw bytes
		raw := make([]byte, size)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, err
		}

		buffers[i] = memory.NewBufferBytes(raw)
	}

	// 4. Construct Arrow ArrayData
	arrData := array.NewData(
		dt,
		int(length),
		buffers, // buffers
		nil,     // children (none for primitive)
		-1,      // null count (setting it to -1 lets Arrow compute it lazily)
		0,       // offset
	)

	// 5. Wrap into Array type
	return array.MakeFromData(arrData), nil
}

// must call ss.DeserializeSchema first or else this will not work properly
func (ss *serializer) DecodeRecordBatch(r io.Reader, schema *arrow.Schema) ([]arrow.Array, error) {
	if !ss.schema.Equal(schema) {
		return nil, ErrInvalidSchema("serializer schema and provided schema do not match")
	}
	arrays := make([]arrow.Array, len(schema.Fields()))

	for i, field := range schema.Fields() {
		arr, err := ss.DeserializeNextColumn(r, field.Type)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		arrays[i] = arr
	}

	return arrays, nil
}

func (ss *serializer) schemaFromDisk(data io.Reader) (*arrow.Schema, error) {

	// number of fields
	var num uint32
	if err := binary.Read(data, binary.LittleEndian, &num); err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0, num)

	for i := uint32(0); i < num; i++ {
		// read name
		var nameLen uint32
		err := binary.Read(data, binary.LittleEndian, &nameLen)
		if err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		_, err = data.Read(nameBytes)
		if err != nil {
			return nil, err
		}

		// read type
		var typeLen uint32
		err = binary.Read(data, binary.LittleEndian, &typeLen)
		if err != nil {
			return nil, err
		}
		typeBytes := make([]byte, typeLen)
		_, err = data.Read(typeBytes)
		if err != nil {
			return nil, err
		}
		typ, err := BasicArrowTypeFromString(string(typeBytes))
		if err != nil {
			return nil, err
		}

		// read nullable
		var nullable uint8
		err = binary.Read(data, binary.LittleEndian, &nullable)
		if err != nil {
			return nil, err
		}

		fields = append(fields, arrow.Field{
			Name:     string(nameBytes),
			Type:     typ,
			Nullable: nullable == 1,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

func BasicArrowTypeFromString(s string) (arrow.DataType, error) {
	switch s {
	case "null":
		return arrow.Null, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil

	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil

	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil

	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil

	case "string", "utf8":
		return arrow.BinaryTypes.String, nil
	case "large_string", "large_utf8":
		return arrow.BinaryTypes.LargeString, nil

	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "large_binary":
		return arrow.BinaryTypes.LargeBinary, nil
	}

	return nil, fmt.Errorf("unsupported arrow type: %s", s)
}

/*

FILE:
┌────────────────────────┐
│ SCHEMA BLOCK           │
│   numberOfFields       │
│   (field entries...)   │
├────────────────────────┤
│ RECORD BATCH #1        │
│   COLUMN 0             │
│     arrayLength        │
│     numBuffers         │
│     buffers[...]       │
│   COLUMN 1             │
│   COLUMN 2             │
│   ...                  │
├────────────────────────┤
│ RECORD BATCH #2        │
│   COLUMN 0             │
│   COLUMN 1             │
│   COLUMN 2             │
│   ...                  │
└────────────────────────┘
EOF
*/

// lose notes / development history
/*

What are we look for?
for serialization for intermediate record batches.
The main use case is for pipeline breaking operators where its unsafe to assume that all the records will fit in ram
This means that all the inputs will the same schema | for example sort(col) -> each element is the exact same | join(col1 == col2) keep the left side in memory/in a separate file and the right side will have the exact same schema

This means that we can just work with the data directly since well know the schema. Just to be safe we can keep the schema in memory attached directly to the object/class handling the serialization

V1
Format on disk -> dataTypeSize|dataType|BatchSize|BatchElements|BatchSize|BatchElements.....

but this assumes were are only dealing with one column.
what about sort order?
what do we do with the other columns as were writing this single column to disk?


We have to write the entire record batch to disk

V2
write the schema out to disk as well but this is only to validate with the in memory schema
!! inbetween each column being read into memory check with in memory schema for their data type for correct encoding
!! schema will also tell u how many columns u have to read in for that specific record batch
format on disk -> **schema|ColumnNsize|columnNData|columnN+1size|columnN+1data|columnN+2size|columnN+2data...EndOfrecordBatch|columnNSize|columnNData|....
format on disk for schema -> number of fields | field1NameLength|field1Name|field1TypeLength|field1Type|field1Nullable| field2NameLength|field2Name|field2TypeLength|field2Type|field2Nullable...
were going to be writing more data but theres not much we can do about that. for now this is fine

optimizations
(1)wont need to add the schema each time
*/
// saves allocations to have a struct that implements this interface than to have this attached directly to RecordBatch
// especially in the cases where we have to spill to disk multiple times (sort,hash join, aggregation)
