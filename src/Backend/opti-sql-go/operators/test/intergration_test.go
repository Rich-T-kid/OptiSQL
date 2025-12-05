package test

import (
	"errors"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
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
			t.Logf("(1.A) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1.A) batch:\n%v\n", batch.PrettyPrint())
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
			t.Logf("(1.B) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1.B) batch:\n%v\n", batch.PrettyPrint())
	})
	// (1.C) SELECT id, favorite_color FROM source1 WHERE favorite_color = 'Red' LIMIT 7;
	t.Run("(1.C)", func(t *testing.T) {
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
			t.Logf("(1.C) got nil batch (possibly EOF)")
			return
		}

		t.Logf("(1.C) batch:\n%v\n", batch.PrettyPrint())
	})

}

/*
(2)
Operators: Filter, Scalar functions
sql query:
(2.A)SELECT id, username, LOWER(favorite_color) as fav_color_lower FROM source1 WHERE UPPER(favorite_color) = 'BLUE';
(2.B)SELECT username, LOWER(email_address) AS email_lower
FROM source1
WHERE UPPER(username) = 'ALICE';
*/

/*
(3)
Operators: select, Sort
sql query:
(3.A)SELECT id, account_balance_usd, username
FROM source1
ORDER BY account_balance_usd ASC
(3.B)SELECT id, favorite_color
FROM source1
ORDER BY favorite_color ASC;
*/

/*
(4)
Operators: Join(INNER), Select
SQL:
(4.A)SELECT s1.id, s1.username, s2.department_name
FROM source1 AS s1
INNER JOIN source2 AS s2
ON s1.favorite_color = s2.manager_name;
(4.B)SELECT s1.id, s1.email_address, s2.department_name
FROM source1 AS s1
INNER JOIN source2 AS s2
ON s1.favorite_color = s2.manager_name;
*/

/*
(5)
Operators: GroupBy, Aggregation(SUM, AVG), Select
SQL:
(5.A)SELECT favorite_color, AVG(age_years) AS avg_age, SUM(account_balance_usd) AS total_balance
FROM source1
GROUP BY favorite_color;
(5.B)SELECT is_active, COUNT(*) AS active_count, AVG(age_years) AS avg_age
FROM source1
GROUP BY is_active;

*/

/*
(6)
Operators: Distinct, Sort(DESC)
SQL:
(6.A)SELECT DISTINCT favorite_color
FROM source1
ORDER BY favorite_color DESC;
(6.B)SELECT DISTINCT is_active
FROM source1
ORDER BY is_active DESC;

*/

/*
(7)
Operators: Join(INNER), Filter, Projection, Limit

SQL:
(7.A)SELECT s1.id, s1.username, s2.department_name
FROM source1 AS s1
INNER JOIN source2 AS s2
ON s1.favorite_color = s2.manager_name
WHERE s1.age_years > 30
LIMIT 5;
(7.B)SELECT s1.username, s2.manager_email
FROM source1 AS s1
JOIN source2 AS s2
ON s1.favorite_color = s2.manager_name
WHERE s2.department_name = 'Engineering'
LIMIT 3;
(7.C)SELECT s1.id, s2.manager_name
FROM source1 s1
JOIN source2 s2
ON s1.favorite_color = s2.manager_name
WHERE s1.account_balance_usd > 10000
LIMIT 2;
*/

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

/*
(9)
Operators: Sort (multiple columns), Select

SQL:
(9.A)SELECT id, username, age_years
FROM source1
ORDER BY age_years DESC, username ASC;
(9.B)SELECT id, email_address, age_years
FROM source1
ORDER BY age_years ASC, email_address DESC;

*/

/*
(10)
Operators: Join (INNER, multiple conditions), Select, Sort (multiple columns)

(10.A)SELECT s1.id, s1.username, s2.manager_name, s2.budget
FROM source1 AS s1
INNER JOIN source2 AS s2
    ON s1.favorite_color = s2.manager_name
   AND s1.region = s2.region
ORDER BY s2.budget DESC, s1.username ASC;

*/
