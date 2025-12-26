---
## Expressions

Expressions are encoded as **tagged objects** using `expr_type`.
They are evaluated row-wise against a record batch and return an Arrow array.

When applicable, expressions **may include an explicit Arrow type** to avoid inference ambiguity.
---

## `Valid Literal Types`

```bash
"int"
"string"
"boolean"
"float64"
```

## `Valid Binary Operators`

```bash
"Addition"
"Subtraction"
"Multiplication"
"Division"
# comparison
"Equal"
"NotEqual"
"LessThan"
"LessThanOrEqual"
"GreaterThan"
"GreaterThanOrEqual"
# logical
"And"
"Or"
```

## `Valid Scalar functions`

```bash
"Upper"
"Lower"
"Abs"
"Round"
```

## `Valid Aggregations functions`

```bash
"Sum"
"Count"
"Avg"
"Min"
"Max"
```

## `ColumnResolve`

Resolves a column from the input batch.

```bash
{
  "expr_type": "ColumnResolve",
  "name": "a"
}
```

---

## `LiteralResolve`

Represents a constant literal value.

```bash
{
  "expr_type": "LiteralResolve",
  "value": 10,
  "lit_type": "int"
}
```

### Notes

- `lit_type` is **optional**
- When provided, it must be a valid Arrow primitive type
- If omitted, the engine may infer the type

---

## `BinaryExpr`

Applies a binary operator to two expressions.

```bash
{
  "expr_type": "BinaryExpr",
  "op": "GreaterThan", # or any (valid) binary operator
  "left": {
    "expr_type": "ColumnResolve",
    "name": "a"
  },
  "right": {
    "expr_type": "LiteralResolve",
    "value": 10,
    "lit_type": "int"
  }
}
```

- Comparison and logical operators must return a boolean array
- Left and right expressions must resolve to compatible Arrow types

---

## `ScalarFunction`

Applies a scalar function element-wise.

```bash
{
  "expr_type": "ScalarFunction",
  "func": "Upper", # or any (Valid) scalar function
  "expr":
    {
      "expr_type": "ColumnResolve",
      "name": "name"
    }

}
```

---

## `Alias`

Attaches a name to an expression.

```bash
{
  "expr_type": "Alias",
  "expr": {
    "expr_type": "ColumnResolve",
    "name": "a"
  },
  "name": "alias_a"
}
```

- Alias affects **naming only**
- Evaluation result is unchanged

---

## `CastExpr`

Casts the result of an expression to a specific Arrow type.

```bash
{
  "expr_type": "CastExpr",
  "expr": {
    "expr_type": "ColumnResolve",
    "name": "a"
  },
  "to_type": "float64"
}
```

---

## Expression Type Enum

`expr_type` is a **closed enum**.

```bash
ColumnResolve
LiteralResolve
BinaryExpr
ScalarFunction
Alias
CastExpr
NullCheckExpr
```

Each expression object **must** contain exactly one `expr_type`.

---
