package Expr

import "github.com/apache/arrow/go/v17/arrow"

type binaryOperator int

const (
	// arithmetic
	addition       binaryOperator = 1
	subtraction    binaryOperator = 2
	multiplication binaryOperator = 3
	division       binaryOperator = 4
	modulous       binaryOperator = 5
	// comparison
	equal              binaryOperator = 6
	notEqual           binaryOperator = 7
	lessThan           binaryOperator = 8
	lessThanOrEqual    binaryOperator = 9
	greaterThan        binaryOperator = 10
	greaterThanOrEqual binaryOperator = 11
	// logical
	and binaryOperator = 12
	or  binaryOperator = 13
	not binaryOperator = 14
)

type supportedFunctions int

const (
	upper supportedFunctions = 1
	lower supportedFunctions = 2
	abs   supportedFunctions = 3
	round supportedFunctions = 4
)

type aggFunctions = int

const (
	Sum   aggFunctions = 1
	Count aggFunctions = 2
	Avg   aggFunctions = 3
	Min   aggFunctions = 4
	Max   aggFunctions = 5
)

/*
Eval(expr):

	match expr:
	    Literal(x) -> return x
	    Column(name) -> return array of that column
	    BinaryExpr(left > right) -> eval left, eval right, apply operator
	    ScalarFunction(upper(name)) -> evaluate function
	    Alias(expr, name) -> just a name wrapper
*/
type Expr interface {
	ExprNode() // empty method, only for the sake of polymophism
}

/*
Alias | sql: select col1 as new_name from table_source
updates the column name in the output schema.
*/
type Alias struct {
	expr       []Expr
	columnName string
	name       string
}

// return batch.Columns[fieldIndex["age"]]
// resolves the arrow array corresponding to name passed in
// sql: select age
type ColumnResolve struct {
	name string
}

// Evaluates to a column of length = batch-size, filled with this literal.
// sql: select 1
type LiteralResolve struct {
	Type  arrow.DataType
	value any
}

type Operator struct {
	o binaryOperator
}
type BinaryExpr struct {
	left  []Expr
	op    Operator
	right []Expr
}

type ScalarFunction struct {
	function supportedFunctions
	input    []Expr // resolve to something you can procees IE, literal/coloumn Resolve
}
type AggregateFunction struct {
	function aggFunctions
	args     []Expr
}

type CastExpr struct {
	expr       []Expr // can be a Literal or Column (check for datatype then)
	targetType arrow.DataType
}
