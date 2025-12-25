package substrait

import (
	"encoding/json"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/join"
	"opti-sql-go/operators/project"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
)

var (
	ErrInvalidSubstraitPlan = func(e error) error {
		return fmt.Errorf("invalid JSON from frontend: %s", e.Error())
	}
	ErrMalformedEmitBody = fmt.Errorf("malformed logical plan: multiple root operators found, expected exactly one")

	ErrMissingEmitOperator = fmt.Errorf("malformed logical plan: missing 'Emit' operator")

	ErrInvalidEmitChildren = fmt.Errorf("malformed logical plan: 'Emit' input must be a key to a JSON object")

	ErrInvalidOperator = func(operator string) error {
		return fmt.Errorf("invalid operator '%s': cannot be called directly before 'Emit'", operator)
	}
	ErrBuildTreeFailed = func(operator string, context string) error {
		return fmt.Errorf("failed to build operator tree for '%s': %s", operator, context)
	}
)

type jsonOBJ = map[string]interface{}

/*
sql: select a, b from table1 order by a
"Emit": {
  "Sort": {
    "input": {
      "Project": {
        "input": {
          "Source": "s3://bucket/data.csv"
        },
        "columns": ["a", "b"],
        "alias": ["", ""]
      }
    },
    "by": [{ "column": "a" }]
  }
}
*/
// represents outer layer of substrait plan
type Emiter struct {
	emitOperator operators.Operator
}
type planMetaData struct {
	id            string
	localFileName string // check if empty before deleting the file
}

func NewPlanMetaData(id string) *planMetaData {
	return &planMetaData{id: id}

}

// first turn into json. The plan should fit into ram to consume it all
func consumePlan(r io.Reader, p *planMetaData) (*Emiter, error) {

	contents, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	inMemoryRepr := make(jsonOBJ)
	err = json.Unmarshal(contents, &inMemoryRepr)
	if err != nil {
		return nil, ErrInvalidSubstraitPlan(err)
	}
	if len(inMemoryRepr) != 1 {
		return nil, ErrMalformedEmitBody
	}
	_, exist := inMemoryRepr["Emit"] // TODO! standerdize the spelling and casing of this or else everythign else will break
	if !exist {
		return nil, ErrMissingEmitOperator
	}
	//fmt.Printf("map:\t%v\n", inMemoryRepr)
	tree, ok := inMemoryRepr["Emit"].(map[string]any)
	if !ok {
		return nil, ErrInvalidEmitChildren
	}
	return buildTree(tree, p)
}

func buildTree(m jsonOBJ, plan *planMetaData) (*Emiter, error) {
	//key=Operator , value=arguments to that operator

	// the tree needs to be built from the bottom up. Recurse all the way down until you reach a leaf node || key == "Source"

	for k, v := range m {
		// dont print before only after
		body := v.(map[string]any)
		var op operators.Operator
		switch strings.ToLower(k) {
		case "filter":
			filterOP, err := parseFilter(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("filter", err.Error())
			}
			op = filterOP
			return &Emiter{op}, nil
		case "project":
			projectOP, err := parseProject(body, plan)
			if err != nil {
				return nil, ErrBuildTreeFailed("project", err.Error())
			}
			op = projectOP
			return &Emiter{op}, nil
		case "sort":
			sortOP, err := parseSort(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("sort", err.Error())
			}
			op = sortOP
			return &Emiter{op}, nil

		case "distinct":
			distinctOP, err := parseDistinct(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("distinct", err.Error())
			}
			op = distinctOP
			return &Emiter{op}, nil
		case "limit":
			limitOP, err := parseLimit(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("limit", err.Error())
			}
			op = limitOP
			return &Emiter{op}, nil
		case "groupby":
			groupByOP, err := parseGroupBy(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("group-by", err.Error())
			}
			op = groupByOP
			return &Emiter{op}, err

		case "join":
			joinOP, err := parseJoin(body)
			if err != nil {
				return nil, ErrBuildTreeFailed("join", err.Error())
			}
			op = joinOP
			return &Emiter{op}, err

		case "source", "expression": // invalid branch
			//(1) Source:cannot directy return from source
			//(2) expressions: cannot directy return expressions, need to call project on top
			return nil, ErrInvalidOperator(k)
		}
	}
	return nil, ErrBuildTreeFailed("unknown", "no valid operator found in logical plan")
}
func parseSource(sourceOBJ jsonOBJ, plan *planMetaData) (operators.Operator, error) {
	//"need to parse out the actuall file name form the url"
	fields := []string{"file-name", "local"}
	err := containsFields(fields, sourceOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"string", "boolean"}, sourceOBJ)
	if err != nil {
		return nil, err
	}
	name := sourceOBJ["file-name"].(string)
	pieces := strings.Split(name, ".")
	if len(pieces) < 1 {
		return nil, fmt.Errorf("invalid file name used as source, must end in .csv or .parquet")
	}
	var kind string
	switch strings.ToLower(pieces[len(pieces)-1]) {
	case "csv":
		kind = "csv"
	case "parquet":
		kind = "parquet"
	default:
		return nil, fmt.Errorf("invalid file mime was used in source operator")
	}
	local := sourceOBJ["local"].(bool)
	ntwResource, err := project.NewStreamReader(name)
	if err != nil {
		return nil, err
	}
	if !local && kind == "parquet" {
		parquetRootNode, err := project.NewParquetSource(ntwResource)
		if err != nil {
			return nil, err
		}
		return parquetRootNode, nil
	}
	localFile, err := ntwResource.DownloadLocally(plan.id)
	if err != nil {
		return nil, err
	}
	curDir, _ := os.Getwd()
	plan.localFileName = fmt.Sprintf("%s/%s", curDir, localFile.Name())
	switch kind {
	case "csv":
		csvRootNode, err := project.NewProjectCSVLeaf(localFile)
		if err != nil {
			return nil, err
		}
		return csvRootNode, nil
	case "parquet":
		parquetRootNode, err := project.NewParquetSource(localFile)
		if err != nil {
			return nil, err
		}
		return parquetRootNode, nil
	}
	return nil, nil

}
func parseFilter(filterOBJ jsonOBJ) (*filter.FilterExec, error) {
	return nil, nil
}
func parseProject(sourceOBJ jsonOBJ, plan *planMetaData) (*project.ProjectExec, error) {
	return nil, nil
}
func parseSort(sourceOBJ jsonOBJ) (*aggr.SortExec, error) {
	return nil, nil
}
func parseDistinct(sourceOBJ jsonOBJ) (*filter.DistinctExec, error) {
	return nil, nil
}

func parseLimit(sourceOBJ jsonOBJ) (*filter.LimitExec, error) {
	return nil, nil
}

func parseGroupBy(sourceOBJ jsonOBJ) (*aggr.GroupByExec, error) {
	return nil, nil
}
func parseJoin(sourceOBJ jsonOBJ) (*join.HashJoinExec, error) {
	return nil, nil
}
func parseHaving(sourceOBJ jsonOBJ) (*aggr.HavingExec, error) {
	return nil, nil
}

// expressions need to be handled in a special way since they contain serveral keys
func parseExpression(m jsonOBJ) (Expr.Expression, error) {
	// grab tje expr_type and then parse based on that
	err := containsFields([]string{"expr_type"}, m)
	if err != nil {
		fmt.Printf("(parseExpression) eror: %v\n", err)
		return nil, fmt.Errorf("malformed expression body. Doesnt contain expr_type field")
	}
	switch m["expr_type"].(string) {
	case "ColumnResolve":
		neededFields := []string{"name"}
		fieldTypes := []string{"string"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		if m["name"] == "" {
			return nil, fmt.Errorf("column resolve name cannot be empty")
		}
		cr := Expr.NewColumnResolve(m["name"].(string))
		return cr, nil
	case "LiteralResolve":
		neededFields := []string{"value", "lit_type"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		fieldTypes := []string{m["lit_type"].(string), "string"} // ! todo
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body (Types): %v", err)
		}
		var value any
		var arrowType arrow.DataType
		switch m["lit_type"].(string) {
		case "int":
			arrowType = arrow.PrimitiveTypes.Int32
			v, _ := m["value"].(int)
			value = int32(v)
		case "string":
			arrowType = arrow.BinaryTypes.String
			v, _ := m["value"].(string)
			value = string(v)
		case "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
			v, _ := m["value"].(bool)
			value = bool(v)
		case "float64":
			arrowType = arrow.PrimitiveTypes.Float64
			v, _ := m["value"].(float64)
			value = float64(v)
		default:
			return nil, fmt.Errorf("invalid Literal Type was passed to Literal Resolve")
		}
		lr := Expr.NewLiteralResolve(arrowType, value)
		return lr, nil
	case "BinaryExpr":
		neededFields := []string{"op", "left", "right"} // ! todo
		fieldTypes := []string{}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
	case "ScalarFunction":
		neededFields := []string{"func", "expr"}
		fieldTypes := []string{"string", "object"} // ! todo
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		function := m["func"].(string)
		fn := Expr.SupportedFunctions(-1)
		switch function {
		case "Upper", "Lower", "Abs", "Round":
			fn = Expr.FnToScalarFunction(function)

		}
		if fn == Expr.SupportedFunctions(-1) {
			return nil, fmt.Errorf("invalid scalr function provided %s", function)

		}
		expr, err := parseExpression(m["expr"].(map[string]any))
		if err != nil {
			return nil, err
		}
		sf := Expr.NewScalarFunction(Expr.FnToScalarFunction(function), expr)
		return sf, nil
	case "Alias":
		neededFields := []string{"name", "expr"}
		fieldTypes := []string{} // ! todo
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
	case "CastExpr":
		neededFields := []string{"expr", "to_type"}
		fieldTypes := []string{} // ! todo
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
	case "NullCheckExpr":
		neededFields := []string{"expr", "in_null"}
		fieldTypes := []string{} // ! todo
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
	default:
		return nil, fmt.Errorf("invalid expression: %v", m["expr_type"])
	}
	return nil, fmt.Errorf("unreachable code")

}

// check that all the fileds exist, if any are missing return and error indicating which fields are missing
// ignore any extra fields that may be present for now
func containsFields(fields []string, obj map[string]any) error {
	var missing []string

	for _, f := range fields {
		if _, ok := obj[f]; !ok {
			missing = append(missing, f)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"missing required fields: %s",
			strings.Join(missing, ", "),
		)
	}

	return nil
}

type misMatchTypes struct {
	idx              uint8
	fieldName        string
	value            any // from "%v" formating
	expectedDataType string
}

func correctFieldTypes(fields []string, fieldTypes []string, obj jsonOBJ) error {
	if len(fields) != len(fieldTypes) {
		return fmt.Errorf("fields and fieldTypes must have the same number of elements")
	}
	var misMatches []misMatchTypes

	// dont need to do _,ok  pattern here because we can assume contains fields is called before this one
	for i, field := range fields {
		value := obj[field]
		expected := fieldTypes[i]
		if !matchesExpectedType(value, expected) {
			misMatches = append(misMatches, misMatchTypes{
				idx:              uint8(i),
				fieldName:        field,
				value:            value,
				expectedDataType: expected,
			})
		}

	}
	if len(misMatches) > 0 {
		return fmt.Errorf("all fields did not match their expected data types \t%#v", misMatches)

	}
	return nil // mismatch in field and their expected types, field1 is not of expected type T1
}

func matchesExpectedType(value any, expected string) bool {
	switch expected {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "int":
		_, ok := value.(int)
		return ok
	case "float64":
		_, ok := value.(float64)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}
