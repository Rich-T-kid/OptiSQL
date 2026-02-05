package test

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	aggr "opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/project"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
)

// TestAliasRegressionProjectFilter - Test 1: Project with alias, then filter on aliased column
// SQL: SELECT id AS user_id, username FROM source1 WHERE user_id > 5
func TestAliasRegressionProjectFilter(t *testing.T) {
	src := source1Project()

	projExprs := Expr.NewExpressions(
		Expr.NewAlias(Expr.NewColumnResolve("id"), "user_id"),
		Expr.NewColumnResolve("username"),
	)
	proj, err := project.NewProjectExec(src, projExprs)
	if err != nil {
		t.Errorf("project failed: %v", err)
	}

	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("user_id"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 5),
	)

	filt, err := filter.NewFilterExec(proj, pred)
	if err != nil {
		t.Errorf("filter failed (aliasing broken): %v\nAvailable: %v",
			err, operators.GetSchemaFieldNames(proj.Schema()))
	}

	batch, err := filt.Next(10)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("SUCCESS - Result:\n%v", batch.PrettyPrint())
	}
}

// TestAliasRegressionFilterBeforeAggr - Test 2a: Filter BEFORE aggregation (WHERE clause)
// SQL: SELECT username, AVG(account_balance_usd) FROM source1 WHERE id > 5 GROUP BY username
func TestAliasRegressionFilterBeforeAggr(t *testing.T) {
	src := source1Project()

	// WHERE id > 5
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("id"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 5),
	)

	filt, err := filter.NewFilterExec(src, pred)
	if err != nil {
		t.Errorf("filter before aggregation failed: %v", err)
	}

	// GROUP BY username
	groupByExprs := []Expr.Expression{
		Expr.NewColumnResolve("username"),
	}

	// AVG(account_balance_usd)
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Avg,
			Expr.NewColumnResolve("account_balance_usd")),
	}

	groupByOp, err := aggr.NewGroupByExec(filt, aggrExprs, groupByExprs)
	if err != nil {
		t.Errorf("group by after filter failed: %v", err)
	}
	t.Logf("%v\n", groupByOp.Schema())

	outputCols := operators.GetSchemaFieldNames(groupByOp.Schema())
	t.Logf("Output columns after filter->groupby: %v", outputCols)

	batch, err := groupByOp.Next(10)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestAliasRegressionFilterAfterAggr - Test 2b: Filter AFTER aggregation (HAVING clause)
// SQL: SELECT username, AVG(account_balance_usd) FROM source1 GROUP BY username HAVING AVG(account_balance_usd) > 500
func TestAliasRegressionFilterAfterAggr(t *testing.T) {
	src := source1Project()

	// GROUP BY username
	groupByExprs := []Expr.Expression{
		Expr.NewColumnResolve("username"),
	}

	// AVG(account_balance_usd)
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Avg,
			Expr.NewColumnResolve("account_balance_usd")),
	}

	groupByOp, err := aggr.NewGroupByExec(src, aggrExprs, groupByExprs)
	if err != nil {
		t.Errorf("group by failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(groupByOp.Schema())
	t.Logf("GroupBy columns: %v", outputCols)

	if len(outputCols) < 2 {
		t.Error("Expected at least 2 columns (groupby + aggregation)")
	}

	// HAVING AVG(account_balance_usd) > 500
	// The aggregation column should be at index 1
	avgColName := outputCols[1]
	t.Logf("Attempting HAVING on aggregation column: %s", avgColName)

	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve(avgColName),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 500.0),
	)

	havingFilter, err := filter.NewFilterExec(groupByOp, pred)
	if err != nil {
		t.Errorf("HAVING (filter after aggregation) failed: %v\nColumn: %s\nAvailable: %v",
			err, avgColName, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestAliasRegressionGroupBy - Test 3: Group by with HAVING clause
// SQL: SELECT username, SUM(account_balance_usd) FROM source1 GROUP BY username HAVING SUM(account_balance_usd) > 500
func TestAliasRegressionGroupBy(t *testing.T) {
	src := source1Project()

	groupByExprs := []Expr.Expression{
		Expr.NewColumnResolve("username"),
	}

	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Sum, Expr.NewColumnResolve("account_balance_usd")),
		aggr.NewAggregateFunctions(aggr.Count, Expr.NewColumnResolve("id")),
	}

	groupByOp, err := aggr.NewGroupByExec(src, aggrExprs, groupByExprs)
	if err != nil {
		t.Errorf("group by failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(groupByOp.Schema())
	t.Logf("GroupBy columns: %v", outputCols)

	if len(outputCols) < 2 {
		t.Error("Expected at least 2 columns")
	}

	sumColName := outputCols[1]
	t.Logf("Attempting HAVING on column: %s", sumColName)

	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve(sumColName),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 500.0),
	)

	havingFilter, err := filter.NewFilterExec(groupByOp, pred)
	if err != nil {
		t.Errorf("HAVING failed (aliasing broken): %v\nColumn: %s\nAvailable: %v",
			err, sumColName, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("SUCCESS - Result:\n%v", batch.PrettyPrint())
	}
}

// TestAggrWithAliasFilterOnAlias tests that when using an alias in aggregation,
// the filter (HAVING) should reference the alias name, NOT the underlying column
// SQL: SELECT SUM(account_balance_usd) AS total_balance FROM source1 HAVING total_balance > 1000
func TestAggrWithAliasFilterOnAlias(t *testing.T) {
	src := source1Project()

	// SUM(account_balance_usd) AS total_balance
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Sum,
			Expr.NewAlias(Expr.NewColumnResolve("account_balance_usd"), "total_balance")),
	}

	aggrOp, err := aggr.NewGlobalAggrExec(src, aggrExprs)
	if err != nil {
		t.Errorf("aggregation failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(aggrOp.Schema())
	t.Logf("Aggregation output columns: %v", outputCols)

	// Expected: column name should be "total_balance", NOT "sum_account_balance_usd"
	if len(outputCols) != 1 || outputCols[0] != "total_balance" {
		t.Errorf("Expected column name 'total_balance', got: %v", outputCols)
	}

	// HAVING total_balance > 1000
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("total_balance"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 1000.0),
	)

	havingFilter, err := filter.NewFilterExec(aggrOp, pred)
	if err != nil {
		t.Errorf("HAVING on aliased aggregation failed: %v\nAvailable columns: %v", err, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestAggrWithoutAliasUsesColumnName tests that without an alias,
// the column name should just be the column name (no prefix)
// SQL: SELECT SUM(account_balance_usd) FROM source1 HAVING account_balance_usd > 500
func TestAggrWithoutAliasUsesColumnName(t *testing.T) {
	src := source1Project()

	// SUM(account_balance_usd) - no alias
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Sum,
			Expr.NewColumnResolve("account_balance_usd")),
	}

	aggrOp, err := aggr.NewGlobalAggrExec(src, aggrExprs)
	if err != nil {
		t.Errorf("aggregation failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(aggrOp.Schema())
	t.Logf("Aggregation output columns: %v", outputCols)

	// Expected: column name should be just "account_balance_usd"
	if len(outputCols) != 1 || outputCols[0] != "account_balance_usd" {
		t.Errorf("Expected column name 'account_balance_usd', got: %v", outputCols)
	}

	// Filter on the column name
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("account_balance_usd"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 500.0),
	)

	havingFilter, err := filter.NewFilterExec(aggrOp, pred)
	if err != nil {
		t.Errorf("Filter on non-aliased aggregation failed: %v\nAvailable columns: %v", err, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestGroupByWithAliasedAggregation tests GROUP BY with aliased aggregation
// SQL: SELECT username, AVG(account_balance_usd) AS avg_balance FROM source1 GROUP BY username HAVING avg_balance > 500
func TestGroupByWithAliasedAggregation(t *testing.T) {
	src := source1Project()

	groupByExprs := []Expr.Expression{
		Expr.NewColumnResolve("username"),
	}

	// AVG(account_balance_usd) AS avg_balance
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Avg,
			Expr.NewAlias(Expr.NewColumnResolve("account_balance_usd"), "avg_balance")),
	}

	groupByOp, err := aggr.NewGroupByExec(src, aggrExprs, groupByExprs)
	if err != nil {
		t.Errorf("group by failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(groupByOp.Schema())
	t.Logf("GroupBy output columns: %v", outputCols)

	// Expected: columns should be ["username", "avg_balance"]
	if len(outputCols) != 2 {
		t.Errorf("Expected 2 columns, got %d: %v", len(outputCols), outputCols)
	}
	if outputCols[0] != "username" {
		t.Errorf("Expected first column 'username', got: %s", outputCols[0])
	}
	if outputCols[1] != "avg_balance" {
		t.Errorf("Expected second column 'avg_balance', got: %s", outputCols[1])
	}

	// HAVING avg_balance > 500
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("avg_balance"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 500.0),
	)

	havingFilter, err := filter.NewFilterExec(groupByOp, pred)
	if err != nil {
		t.Errorf("HAVING on aliased aggregation failed: %v\nAvailable columns: %v", err, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestGroupByWithoutAliasedAggregation tests GROUP BY without alias - should use column name
// SQL: SELECT username, COUNT(id) FROM source1 GROUP BY username HAVING id > 5
func TestGroupByWithoutAliasedAggregation(t *testing.T) {
	src := source1Project()

	groupByExprs := []Expr.Expression{
		Expr.NewColumnResolve("username"),
	}

	// COUNT(id) - no alias
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Count,
			Expr.NewColumnResolve("id")),
	}

	groupByOp, err := aggr.NewGroupByExec(src, aggrExprs, groupByExprs)
	if err != nil {
		t.Errorf("group by failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(groupByOp.Schema())
	t.Logf("GroupBy output columns: %v", outputCols)

	// Expected: columns should be ["username", "id"]
	if len(outputCols) != 2 {
		t.Errorf("Expected 2 columns, got %d: %v", len(outputCols), outputCols)
	}
	if outputCols[0] != "username" {
		t.Errorf("Expected first column 'username', got: %s", outputCols[0])
	}
	if outputCols[1] != "id" {
		t.Errorf("Expected second column 'id', got: %s", outputCols[1])
	}

	// HAVING id > 5 (referencing the COUNT result by the column name)
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("id"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 5.0),
	)

	havingFilter, err := filter.NewFilterExec(groupByOp, pred)
	if err != nil {
		t.Errorf("HAVING on non-aliased aggregation failed: %v\nAvailable columns: %v", err, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestMultipleAggrWithMixedAliasing tests multiple aggregations with some aliased, some not
// SQL: SELECT COUNT(id) AS user_count, SUM(account_balance_usd) FROM source1 HAVING user_count > 5
func TestMultipleAggrWithMixedAliasing(t *testing.T) {
	src := source1Project()

	// COUNT(id) AS user_count, SUM(account_balance_usd) (no alias)
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Count,
			Expr.NewAlias(Expr.NewColumnResolve("id"), "user_count")),
		aggr.NewAggregateFunctions(aggr.Sum,
			Expr.NewColumnResolve("account_balance_usd")),
	}

	aggrOp, err := aggr.NewGlobalAggrExec(src, aggrExprs)
	if err != nil {
		t.Errorf("aggregation failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(aggrOp.Schema())
	t.Logf("Aggregation output columns: %v", outputCols)

	// Expected: ["user_count", "account_balance_usd"]
	if len(outputCols) != 2 {
		t.Errorf("Expected 2 columns, got %d: %v", len(outputCols), outputCols)
	}
	if outputCols[0] != "user_count" {
		t.Errorf("Expected first column 'user_count', got: %s", outputCols[0])
	}
	if outputCols[1] != "account_balance_usd" {
		t.Errorf("Expected second column 'account_balance_usd', got: %s", outputCols[1])
	}

	// Filter on the aliased column
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("user_count"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 5.0),
	)

	havingFilter, err := filter.NewFilterExec(aggrOp, pred)
	if err != nil {
		t.Errorf("HAVING on user_count failed: %v\nAvailable columns: %v", err, outputCols)
	}

	batch, err := havingFilter.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestProjectThenAggrWithAlias tests projection followed by aggregation with alias
// SQL: SELECT id AS user_id FROM source1; then SELECT COUNT(user_id) AS total FROM ...
func TestProjectThenAggrWithAlias(t *testing.T) {
	src := source1Project()

	// First project: id AS user_id
	projExprs := Expr.NewExpressions(
		Expr.NewAlias(Expr.NewColumnResolve("id"), "user_id"),
		Expr.NewColumnResolve("username"),
	)
	proj, err := project.NewProjectExec(src, projExprs)
	if err != nil {
		t.Errorf("project failed: %v", err)
	}

	// Then aggregate: COUNT(user_id) AS total
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Count,
			Expr.NewAlias(Expr.NewColumnResolve("user_id"), "total")),
	}

	aggrOp, err := aggr.NewGlobalAggrExec(proj, aggrExprs)
	if err != nil {
		t.Errorf("aggregation failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(aggrOp.Schema())
	t.Logf("Aggregation output columns: %v", outputCols)

	// Expected: column name should be "total"
	if len(outputCols) != 1 || outputCols[0] != "total" {
		t.Errorf("Expected column name 'total', got: %v", outputCols)
	}

	batch, err := aggrOp.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
	}

	if batch != nil {
		t.Logf("Result:\n%v", batch.PrettyPrint())
	}
}

// TestInvalidFilterOnWrongColumnName tests that filtering on the wrong column name fails
// This is a negative test - it should FAIL if the column name is wrong
// SQL: SELECT SUM(balance) AS total FROM source1 HAVING account_balance_usd > 500 (should fail - account_balance_usd doesn't exist after aggregation)
func TestInvalidFilterOnWrongColumnName(t *testing.T) {
	src := source1Project()

	// SUM(account_balance_usd) AS total
	aggrExprs := []aggr.AggregateFunctions{
		aggr.NewAggregateFunctions(aggr.Sum,
			Expr.NewAlias(Expr.NewColumnResolve("account_balance_usd"), "total")),
	}

	aggrOp, err := aggr.NewGlobalAggrExec(src, aggrExprs)
	if err != nil {
		t.Errorf("aggregation failed: %v", err)
	}

	outputCols := operators.GetSchemaFieldNames(aggrOp.Schema())
	t.Logf("Aggregation output columns: %v", outputCols)

	// Try to filter on "account_balance_usd" which no longer exists (should be "total")
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("account_balance_usd"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 500.0),
	)

	_, err = filter.NewFilterExec(aggrOp, pred)
	if err == nil {
		t.Error("Expected filter to fail when referencing non-existent column 'account_balance_usd', but it succeeded")
	}

	t.Logf("Correctly failed: %v", err)
}

// TestSimpleAliasWithFilter tests basic aliasing followed by filtering on the alias
// SQL: SELECT age_years AS age FROM source1 WHERE age > 5
func TestSimpleAliasWithFilter(t *testing.T) {
	src := source1Project()

	// SELECT age_years AS age
	projExprs := Expr.NewExpressions(
		Expr.NewColumnResolve("id"),
		Expr.NewAlias(Expr.NewColumnResolve("age_years"), "age"),
	)

	proj, err := project.NewProjectExec(src, projExprs)
	if err != nil {
		t.Errorf("project failed: %v", err)
		return
	}

	outputCols := operators.GetSchemaFieldNames(proj.Schema())
	t.Logf("Project output columns: %v", outputCols)

	// Expected: column should be "age"
	if len(outputCols) != 2 {
		t.Errorf("Expected column names  `id` 'age', got: %v", outputCols)
	}

	// WHERE age > 5
	pred := Expr.NewBinaryExpr(
		Expr.NewColumnResolve("age"),
		Expr.GreaterThan,
		Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 5),
	)

	filt, err := filter.NewFilterExec(proj, pred)
	if err != nil {
		t.Errorf("filter on aliased column failed: %v\nAvailable columns: %v", err, outputCols)
		return
	}

	batch, err := filt.Next(100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("execution failed: %v", err)
		return
	}

	if batch == nil {
		t.Errorf("empty record batch was returned")
	}
}
