# Custom intermediate in memory representation of sql logical/physical plans

### why?

_The primary reason for this layer is flexibility. By decoupling intermediate data representation from Substrait plans, we can accept multiple data formats. As long as we interpret them into this IR, the physical operators work unchanged_

## source operator

```bash
"source-node":
{"file-name":"link-to-s3","local":boolean}
# file ext must end in .csv or .parquet
#local? donwload to local machine of keep streaming from s3 bucket
```

## Project operator

**sql** : `select a , b , c`

```bash
"Project":
{ "input": {operator},
  "expressions": [{Expression},{Expression},{Expression}]}
```

## Filter Operator

**sql**: `select a,b from source where a > 10`

```bash
"Filter":{"input":{operator},"expression":{Expression}}
```

**Example**

```bash
"Filter":{"input":{csv_source_exec},"expression":{"expr_type":"LiteralResolve","value":10,"Lit_Type":"int"}}
```

---

## Distinct Operator

**sql**: `select distinct a, b from source`

```bash
"Distinct": {
  "input": {operator},
  "expressions": [{Expression},{Expression},{Expression}]
}
```

- Removes duplicate rows based on the specified columns
- Output includes only the listed columns

---

## Limit Operator

**sql**: `select a,b from source limit 10`

```bash
"Limit": {
  "input": {operator},
  "limit": 10
}
```

---

## Sort Operator

**sql**: `select a,b from source order by a desc, b asc`

```bash
"Sort": {
  "input": {operator},
  "by": [
    {
    "Expr":{Expression},
    "asc":boolean,
    },
    {
    "Expr":{Expression}, # generally resolves to columns
    "asc":boolean,
    },
  ]
}
```

- `order` defaults to `ASC` if omitted

---

## Single Column Aggregation Operator

**sql**: `select sum(a) from source`

```bash
"Aggregate": {
  "input": {operator},
  "function": "Sum",
  "column": {Expression},
  "alias": "sum_a"
}
```

- Operates on exactly one column
- `alias` is **optional**
- Output contains a single row

---

## Having Operator

**sql**: `select sum(a) from source having sum(a) > 10`

```bash
"Having": {
  "input": {operator},
  "expression": {Expression}
}
```

- Semantics identical to `Filter`
- Applied after aggregation
- Expression must resolve to a boolean mask

---

## Join Operator

**sql**:
`select * from a join b on a.id = b.id`

```bash
"Join": {
  "left": {operator},
  "right": {operator},
  "join_type": "Inner",
  "on": [
    {
      "left":  { "expr_type": "ColumnResolve", "name": "a.id" },
      "right": { "expr_type": "ColumnResolve", "name": "b.id" }
    }
  ]
}
```

### Supported Join Types

- `Inner`

### Notes

- `on` is an array to support multi-column joins
- Join condition expressions must be **equality comparisons**

---

## Group By Operator

**sql**:
`select b, sum(a) from source group by b`

```bash
"GroupBy": {
  "input": {operator},
  "group_by": [
    { "expr_type": "ColumnResolve", "name": "b" }
  ],
  "aggregates": [
    {
      "function": "Sum",
      "column": "a",
      "alias": "sum_a"
    }
  ]
}
```

### Notes

- `group_by` defines the grouping keys
- Each aggregate operates on **exactly one column**
- `alias` on aggregates is optional
- One output row is produced per group

---

## Example 1 — Source → Filter

**sql**: `select * from source where a > 10`

```bash
"Emit": {
  "Filter": {
    "input": {
      "Source": "s3://bucket/data.csv"
    },
    "expression": {
      "expr_type": "BinaryExpr",
      "op": "GreaterThan",
      "left": { "expr_type": "ColumnResolve", "name": "a" },
      "right": {
        "expr_type": "LiteralResolve",
        "value": 10,
        "lit_type": "i32"
      }
    }
  }
}
```

---

## Example 2 — Source → Project → Sort

**sql**: `select a, b from source order by a`

```bash
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
```

---

## Example 3 — Source → Group By → Aggregate

**sql**: `select b, count(a) from source group by b`

```bash
"Emit": {
  "GroupBy": {
    "input": {
      "Source": "s3://bucket/data.csv"
    },
    "group_by": [
      { "expr_type": "ColumnResolve", "name": "b" }
    ],
    "aggregates": [
      {
        "function": "Count",
        "column": "a",
        "alias": "count_a"
      }
    ]
  }
}
```

Here’s a **clean final pair** that fits the docs tone:
one **combined but still simple**, one **slightly more advanced** (no deep nesting, no aggregation).

---

## Example 4 — Source → Distinct → Limit

**sql**: `select distinct a from source limit 5`

```bash
"Emit": {
  "Limit": {
    "input": {
      "Distinct": {
        "input": {
          "Source": "s3://bucket/data.csv"
        },
        "columns": ["a"]
      }
    },
    "limit": 5
  }
}
```

---

## Example 5 — Join → Filter → Sort → Limit

**sql**:

```sql
select u.name, o.amount
from users u
join orders o on u.id = o.user_id
where o.amount > 50
order by o.amount desc
limit 10
```

```bash
"Emit": {
  "Limit": {
    "input": {
      "Sort": {
        "input": {
          "Filter": {
            "input": {
              "Join": {
                "left": { "Source": "s3://bucket/users.csv" },
                "right": { "Source": "s3://bucket/orders.csv" },
                "join_type": "Inner",
                "on": [
                  {
                    "left":  { "expr_type": "ColumnResolve", "name": "u.id" },
                    "right": { "expr_type": "ColumnResolve", "name": "o.user_id" }
                  }
                ]
              }
            },
            "expression": {
              "expr_type": "BinaryExpr",
              "op": "GreaterThan",
              "left": { "expr_type": "ColumnResolve", "name": "o.amount" },
              "right": {
                "expr_type": "LiteralResolve",
                "value": 50,
                "lit_type": "i32"
              }
            }
          }
        },
        "by": [{ "column": "o.amount", "order": "DESC" }]
      }
    },
    "limit": 10
  }
}
```

---
