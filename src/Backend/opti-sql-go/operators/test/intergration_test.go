package test

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	join "opti-sql-go/operators/Join"
	aggr "opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/project"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
)

/*
composes individual operators into one another to test multiple together
*/
const (
	source1Path = "../../../test_data/csv/intergration_test_data_1.csv"
	source2Path = "../../../test_data/csv/intergration_test_data_2.csv"
)

/*
column names:
id,username,email_address,is_active,age_years,account_balance_usd,average_session_minutes,favorite_color
*/
func source1Project() operators.Operator {
	f, err := os.Open(source1Path)
	if err != nil {
		panic(fmt.Sprintf("failed to open source file: %v", err))
	}
	p, _ := project.NewProjectCSVLeaf(f)
	return p
}

/*
colunn names:
id,department_name,manager_name,manager_email
*/
func source2Project() operators.Operator {
	f, err := os.Open(source2Path)
	if err != nil {
		panic(fmt.Sprintf("failed to open source file: %v", err))
	}
	p, _ := project.NewProjectCSVLeaf(f)
	return p
}
func TestPrettyPrintSources(t *testing.T) {
	p1, p2 := source1Project(), source2Project()
	rc1, _ := p1.Next(5)
	rc2, _ := p2.Next(5)

	t.Logf("source 1 batch: %v\n", rc1.PrettyPrint())
	t.Logf("source 2 batch: %v\n", rc2.PrettyPrint())
}

// TestSelectFilterLimit contains two subtests that build pipelines
// combining Select (project), Filter, and Limit for source1 CSV.
// Each subtest constructs the pipeline, calls Next once, and prints the
// resulting batch via PrettyPrint.
/*
(1)
Operators : Select, Filter, Limit
sql query:
(1.A)SELECT id, username, age_years FROM source1 WHERE age_years > 30 LIMIT 10;
(1.B)SELECT username, age_years
FROM source1
WHERE is_active = true AND age_years < 25
LIMIT 3;
(1.C)SELECT id, favorite_color
FROM source1
WHERE favorite_color = 'Red'
LIMIT 7;
*/

func TestSelectFilterLimit(t *testing.T) {
	// (1.A) SELECT id, username, age_years FROM source1 WHERE age_years > 30 LIMIT 10;
	t.Run("1A", func(t *testing.T) {
		// (1.A) SELECT id, username, age_years FROM source1 WHERE age_years > 30 LIMIT 10;
		src := source1Project()
		t.Logf("\t%v\n", src.Schema())

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age_years"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 30),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		projExprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("username"),
			Expr.NewColumnResolve("age_years"),
		)
		proj, err := project.NewProjectExec(filt, projExprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 10)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(10)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		if batch == nil {
			t.Logf("(1A) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (1.B) SELECT username, age_years FROM source1 WHERE is_active = true AND age_years < 25 LIMIT 3;
	t.Run("1B", func(t *testing.T) {
		src := source1Project()

		left := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("is_active"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true),
		)
		right := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age_years"),
			Expr.LessThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 25),
		)
		pred := Expr.NewBinaryExpr(left, Expr.And, right)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		projExprs := Expr.NewExpressions(
			Expr.NewColumnResolve("username"),
			Expr.NewColumnResolve("age_years"),
		)
		proj, err := project.NewProjectExec(filt, projExprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 3)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		if batch == nil {
			t.Logf("(1B) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1B) batch:\n%v\n", batch.PrettyPrint())
	})
	// (1.C) SELECT id, favorite_color FROM source1 WHERE favorite_color = 'Red' LIMIT 7;
	t.Run("1C", func(t *testing.T) {
		src := source1Project()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("favorite_color"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "Red"),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		projExprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("favorite_color"),
		)
		proj, err := project.NewProjectExec(filt, projExprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 7)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}

		if batch == nil {
			t.Logf("(1C) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1C) batch:\n%v\n", batch.PrettyPrint())
	})

}

// -------------------------------------------------------------------------
// (2) Operators: Filter, Scalar functions
// (2.A) SELECT id, username, LOWER(favorite_color) as fav_color_lower FROM source1 WHERE UPPER(favorite_color) = 'BLUE';
// (2.B) SELECT username, LOWER(email_address) AS email_lower FROM source1 WHERE UPPER(username) = 'ALICE';
func TestFilterScalarFunctions(t *testing.T) {
	// (2.A) SELECT id, username, LOWER(favorite_color) as fav_color_lower FROM source1 WHERE UPPER(favorite_color) = 'BLUE';
	t.Run("2A", func(t *testing.T) {
		src := source1Project()

		pred := Expr.NewBinaryExpr(
			Expr.NewScalarFunction(Expr.Upper, Expr.NewColumnResolve("favorite_color")),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "BLUE"),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("username"),
			Expr.NewAlias(Expr.NewScalarFunction(Expr.Lower, Expr.NewColumnResolve("favorite_color")), "fav_color_lower"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(2A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(2A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (2.B) SELECT username, LOWER(email_address) AS email_lower FROM source1 WHERE UPPER(username) = 'ALICE';
	t.Run("2B", func(t *testing.T) {
		src := source1Project()

		pred := Expr.NewBinaryExpr(
			Expr.NewScalarFunction(Expr.Upper, Expr.NewColumnResolve("username")),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "ALICE"),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("username"),
			Expr.NewAlias(Expr.NewScalarFunction(Expr.Lower, Expr.NewColumnResolve("email_address")), "email_lower"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}
		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch != nil {
			t.Fatalf("was expecting an empty batch but recieved %s\n", batch.PrettyPrint())
			return
		}
	})
}

// -------------------------------------------------------------------------
// (3) Operators: select, Sort
// (3.A) SELECT id, account_balance_usd, username FROM source1 ORDER BY account_balance_usd ASC
// (3.B) SELECT id, favorite_color FROM source1 ORDER BY favorite_color ASC;
func TestSelectSort(t *testing.T) {
	// (3.A) SELECT id, account_balance_usd, username FROM source1 ORDER BY account_balance_usd ASC
	t.Run("3A", func(t *testing.T) {
		src := source1Project()
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("account_balance_usd"),
			Expr.NewColumnResolve("username"),
		)
		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		sk := aggr.NewSortKey(Expr.NewColumnResolve("account_balance_usd"), true)
		sortExec, err := aggr.NewSortExec(proj, aggr.CombineSortKeys(sk))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}
		batch, err := sortExec.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(3A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(3A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (3.B) SELECT id, favorite_color FROM source1 ORDER BY favorite_color ASC;
	t.Run("3B", func(t *testing.T) {
		src := source1Project()
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("favorite_color"),
		)
		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}
		sk := aggr.NewSortKey(Expr.NewColumnResolve("favorite_color"), true)
		sortExec, err := aggr.NewSortExec(proj, aggr.CombineSortKeys(sk))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}
		batch, err := sortExec.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(3B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(3B) batch:\n%v\n", batch.PrettyPrint())
	})
}

// -------------------------------------------------------------------------
// (4) Operators: Join(INNER), Select
// (4.A) SELECT s1.id, s1.username, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.id = s2.id;
// (4.B) SELECT s1.id, s1.email_address, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.id = s2.id;
func TestJoinSelect(t *testing.T) {
	// (4.A) SELECT s1.id, s1.username, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.favorite_color = s2.manager_name;
	t.Run("4A", func(t *testing.T) {
		src1 := source1Project()
		src2 := source2Project()
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("id")},
			[]Expr.Expression{Expr.NewColumnResolve("id")},
		)
		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}
		exprs := Expr.NewExpressions(
			Expr.NewAlias(Expr.NewColumnResolve("left_id"), "id"),
			Expr.NewColumnResolve("username"),
			Expr.NewColumnResolve("department_name"),
		)
		t.Logf("\t%v\n", j.Schema())
		proj, err := project.NewProjectExec(j, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}
		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(4A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(4A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (4.B) SELECT s1.id, s1.email_address, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.id = s2.id;
	t.Run("4B", func(t *testing.T) {
		src1 := source1Project()
		src2 := source2Project()
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("id")},
			[]Expr.Expression{Expr.NewColumnResolve("id")},
		)
		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}
		exprs := Expr.NewExpressions(
			Expr.NewAlias(Expr.NewColumnResolve("left_id"), "cool_guy_id"),
			Expr.NewColumnResolve("email_address"),
			Expr.NewColumnResolve("department_name"),
		)
		proj, err := project.NewProjectExec(j, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}
		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(4B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(4B) batch:\n%v\n", batch.PrettyPrint())
	})
}

func TestGroupByAggregation(t *testing.T) {
	// (5.A) SELECT favorite_color, AVG(age_years) AS avg_age, SUM(account_balance_usd) AS total_balance FROM source1 GROUP BY favorite_color order by avg_age;
	t.Run("5A", func(t *testing.T) {
		src := source1Project()

		groupBy := []Expr.Expression{Expr.NewColumnResolve("favorite_color")}
		aggs := []aggr.AggregateFunctions{
			aggr.NewAggregateFunctions(aggr.Avg, Expr.NewColumnResolve("age_years")),
			aggr.NewAggregateFunctions(aggr.Sum, Expr.NewColumnResolve("account_balance_usd")),
		}

		gb, err := aggr.NewGroupByExec(src, aggs, groupBy)
		if err != nil {
			t.Fatalf("groupby init failed: %v", err)
		}
		sortExec, err := aggr.NewSortExec(gb, aggr.CombineSortKeys(aggr.NewSortKey(Expr.NewColumnResolve("avg_Column(age_years)"), true)))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}

		batch, err := sortExec.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(5A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(5A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (5.B) SELECT is_active, COUNT(*) AS active_count, AVG(age_years) AS avg_age FROM source1 GROUP BY is_active;
	t.Run("5B", func(t *testing.T) {
		src := source1Project()
		fmt.Printf("\t%v\n", src.Schema())
		groupBy := []Expr.Expression{Expr.NewColumnResolve("is_active")}
		aggs := []aggr.AggregateFunctions{
			aggr.NewAggregateFunctions(aggr.Count, Expr.NewColumnResolve("id")),
			aggr.NewAggregateFunctions(aggr.Avg, Expr.NewColumnResolve("age_years")),
		}

		gb, err := aggr.NewGroupByExec(src, aggs, groupBy)
		if err != nil {
			t.Fatalf("groupby init failed: %v", err)
		}

		batch, err := gb.Next(1000)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(5B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(5B) batch:\n%v\n", batch.PrettyPrint())
	})
}

// TestDistinctSort runs DISTINCT + Sort pipelines for source1
// (6.A)SELECT DISTINCT favorite_color
// FROM source1
// ORDER BY favorite_color DESC;
// (6.B)SELECT DISTINCT is_active
// FROM source1
// ORDER BY is_active DESC;
func TestDistinctSort(t *testing.T) {
	// (6.A) SELECT DISTINCT favorite_color FROM source1 ORDER BY favorite_color DESC;
	t.Run("6A", func(t *testing.T) {
		src := source1Project()

		cols := []Expr.Expression{Expr.NewColumnResolve("favorite_color")}
		distinct, err := filter.NewDistinctExec(src, cols)
		if err != nil {
			t.Fatalf("distinct init failed: %v", err)
		}

		sk := aggr.NewSortKey(Expr.NewColumnResolve("favorite_color"), false) // DESC
		sortExec, err := aggr.NewSortExec(distinct, aggr.CombineSortKeys(sk))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}
		proj, err := project.NewProjectExec(sortExec, Expr.NewExpressions(Expr.NewColumnResolve("favorite_color")))
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(6A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(6A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (6.B) SELECT DISTINCT is_active FROM source1 ORDER BY is_active DESC;
	t.Run("6B", func(t *testing.T) {
		src := source1Project()

		cols := []Expr.Expression{Expr.NewColumnResolve("is_active")}
		distinct, err := filter.NewDistinctExec(src, cols)
		if err != nil {
			t.Fatalf("distinct init failed: %v", err)
		}

		sk := aggr.NewSortKey(Expr.NewColumnResolve("is_active"), false) // DESC
		sortExec, err := aggr.NewSortExec(distinct, aggr.CombineSortKeys(sk))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}
		proj, err := project.NewProjectExec(sortExec, Expr.NewExpressions(Expr.NewColumnResolve("is_active")))
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(6B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(6B) batch:\n%v\n", batch.PrettyPrint())
	})
}

// TestJoinFilterProjLimit runs join + filter + project + limit pipelines
// (7.A)SELECT s1.id, s1.username, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.id = s2.id WHERE s1.age_years > 30 LIMIT 5;
// (7.B)SELECT s1.username, s2.manager_email FROM source1 AS s1 JOIN source2 AS s2 ON s1.id = s2.id WHERE s2.department_name = 'Engineering' LIMIT 3;
// (7.C)SELECT s1.id, s2.manager_name FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id WHERE s1.account_balance_usd > 10000 LIMIT 2;
func TestJoinFilterProjLimit(t *testing.T) {
	// (7.A)SELECT s1.id, s1.username, s2.department_name FROM source1 AS s1 INNER JOIN source2 AS s2 ON s1.id = s2.id WHERE s1.age_years > 30 LIMIT 5;
	t.Run("7A", func(t *testing.T) {
		src1 := source1Project()
		src2 := source2Project()
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("id")},
			[]Expr.Expression{Expr.NewColumnResolve("id")},
		)
		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}
		fmt.Printf("schema:%v\n", j.Schema())
		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age_years"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 30),
		)

		filt, err := filter.NewFilterExec(j, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewAlias(Expr.NewColumnResolve("left_id"), "id"),
			Expr.NewColumnResolve("username"),
			Expr.NewAlias(Expr.NewColumnResolve("department_name"), "deptartment"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 5)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(7A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(7A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (7.B)SELECT s1.username, s2.manager_email FROM source1 AS s1 JOIN source2 AS s2 ON s1.id = s2.id WHERE s2.department_name = 'Engineering' LIMIT 3;
	t.Run("7B", func(t *testing.T) {
		src1 := source1Project()
		src2 := source2Project()
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("id")},
			[]Expr.Expression{Expr.NewColumnResolve("id")},
		)
		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("department_name"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "Engineering"),
		)

		filt, err := filter.NewFilterExec(j, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("username"),
			Expr.NewColumnResolve("manager_email"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 3)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(7B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(7B) batch:\n%v\n", batch.PrettyPrint())
	})

	// (7.C)SELECT s1.id, s2.manager_name FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id WHERE s1.account_balance_usd > 10000 LIMIT 2;
	t.Run("7C", func(t *testing.T) {
		src1 := source1Project()
		src2 := source2Project()
		clause := join.NewJoinClause(
			[]Expr.Expression{Expr.NewColumnResolve("id")},
			[]Expr.Expression{Expr.NewColumnResolve("id")},
		)
		j, err := join.NewHashJoinExec(src1, src2, clause, join.InnerJoin, nil)
		if err != nil {
			t.Fatalf("join init failed: %v", err)
		}

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("account_balance_usd"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 10000.0),
		)

		filt, err := filter.NewFilterExec(j, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("left_id"),
			Expr.NewColumnResolve("manager_name"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		lim, err := filter.NewLimitExec(proj, 2)
		if err != nil {
			t.Fatalf("limit init failed: %v", err)
		}

		batch, err := lim.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(7C) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(7C) batch:\n%v\n", batch.PrettyPrint())
	})
}

/*
(8)
Operators: ScalarFunction(ABS, ROUND), Filter, Projection

SQL:
(8.A)SELECT id, ROUND(ABS(average_session_minutes)) AS rounded_session
FROM source1
WHERE ABS(average_session_minutes) > 5;
(8.B)SELECT username, ROUND(account_balance_usd) AS rounded_balance
FROM source1
WHERE ABS(account_balance_usd) > 5000;
*/

// TestScalarAbsRound runs scalar ABS/ROUND with Filter + Projection
// (8.A)SELECT id, ROUND(ABS(average_session_minutes)) AS rounded_session FROM source1 WHERE ABS(average_session_minutes) > 5;
// (8.B)SELECT username, ROUND(account_balance_usd) AS rounded_balance FROM source1 WHERE ABS(account_balance_usd) > 5000;
func TestScalarAbsRound(t *testing.T) {
	// (8.A)SELECT id, ROUND(ABS(average_session_minutes)) AS rounded_session FROM source1 WHERE ABS(average_session_minutes) > 5;
	t.Run("8A", func(t *testing.T) {
		src := source1Project()

		// predicate: ABS(average_session_minutes) > 5
		pred := Expr.NewBinaryExpr(
			Expr.NewScalarFunction(Expr.Abs, Expr.NewColumnResolve("average_session_minutes")),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 5.0),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		// projection: id, ROUND(ABS(average_session_minutes)) as rounded_session
		roundExpr := Expr.NewScalarFunction(Expr.Round, Expr.NewScalarFunction(Expr.Abs, Expr.NewColumnResolve("average_session_minutes")))
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewAlias(roundExpr, "rounded_session"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(8A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(8A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (8.B)SELECT username, ROUND(account_balance_usd) AS rounded_balance FROM source1 WHERE ABS(account_balance_usd) > 5000;
	t.Run("8B", func(t *testing.T) {
		src := source1Project()

		// predicate: ABS(account_balance_usd) > 5000
		pred := Expr.NewBinaryExpr(
			Expr.NewScalarFunction(Expr.Abs, Expr.NewColumnResolve("account_balance_usd")),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, 5000.0),
		)

		filt, err := filter.NewFilterExec(src, pred)
		if err != nil {
			t.Fatalf("filter init failed: %v", err)
		}

		roundExpr := Expr.NewScalarFunction(Expr.Round, Expr.NewColumnResolve("account_balance_usd"))
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("username"),
			Expr.NewAlias(roundExpr, "rounded_balance"),
		)
		proj, err := project.NewProjectExec(filt, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		batch, err := proj.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(8B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(8B) batch:\n%v\n", batch.PrettyPrint())
	})
}

// TestSelectMultiSort runs multi-column ORDER BY tests
// (9.A)SELECT id, username, age_years FROM source1 ORDER BY age_years DESC, username ASC;
// (9.B)SELECT id, email_address, age_years FROM source1 ORDER BY age_years ASC, email_address DESC;
func TestSelectMultiSort(t *testing.T) {
	// (9.A)
	t.Run("9A", func(t *testing.T) {
		src := source1Project()
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("username"),
			Expr.NewColumnResolve("age_years"),
		)
		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		sk1 := aggr.NewSortKey(Expr.NewColumnResolve("age_years"), false) // DESC
		sk2 := aggr.NewSortKey(Expr.NewColumnResolve("username"), true)   // ASC
		sortExec, err := aggr.NewSortExec(proj, aggr.CombineSortKeys(sk1, sk2))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}

		batch, err := sortExec.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(9A) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(9A) batch:\n%v\n", batch.PrettyPrint())
	})

	// (9.B)
	t.Run("9B", func(t *testing.T) {
		src := source1Project()
		exprs := Expr.NewExpressions(
			Expr.NewColumnResolve("id"),
			Expr.NewColumnResolve("email_address"),
			Expr.NewColumnResolve("age_years"),
		)
		proj, err := project.NewProjectExec(src, exprs)
		if err != nil {
			t.Fatalf("project init failed: %v", err)
		}

		sk1 := aggr.NewSortKey(Expr.NewColumnResolve("age_years"), true)      // ASC
		sk2 := aggr.NewSortKey(Expr.NewColumnResolve("email_address"), false) // DESC
		sortExec, err := aggr.NewSortExec(proj, aggr.CombineSortKeys(sk1, sk2))
		if err != nil {
			t.Fatalf("sort init failed: %v", err)
		}

		batch, err := sortExec.Next(100)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error: %v", err)
		}
		if batch == nil {
			t.Logf("(9B) got nil batch (possibly EOF)")
			return
		}
		t.Logf("(9B) batch:\n%v\n", batch.PrettyPrint())
	})
}
