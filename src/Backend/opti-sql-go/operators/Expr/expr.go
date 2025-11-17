package Expr

// evaluate expressions
// for example Column + Literal
// Column - Column
// Literal / Literal

//1. Arithmetic Expressions
// SELECT salary * 1.2, price + tax, -(discount)
//2.Alias Expressions
//SELECT name AS employee_name, age AS employee_age
//SELECT salary * 1.2 AS new_salary
//3.String Expressions
//first_name || ' ' || last_name
//UPPER(name)
//LOWER(email)
//SUBSTRING(name, 1, 3)
//4. Function calls
//ABS(x)
//ROUND(salary, 2)
//LENGTH(name)
//COALESCE(a, b)
//5. Constants
//SELECT 1, 'hello', 3.14
