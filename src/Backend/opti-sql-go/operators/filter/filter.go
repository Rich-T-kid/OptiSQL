package filter

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// FilterExpr takes in a field and column and yeilds a function that takes in an index and returns a bool indicating whether the row at that index satisfies the filter condition.
type FilterExpr func(filed arrow.Field, col arrow.Array) func(i int) bool

// example
func ExampleFilterExpr(field arrow.Field, col arrow.Array) func(i int) bool {
	{
		if field.Name == "age" && col.DataType().ID() == arrow.INT32 {
			return func(i int) bool {
				val := col.(*array.Int32).Value(i)
				return val > 30
			}
		}
		return func(i int) bool {
			return true
		}
	}
}
