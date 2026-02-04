# Custom intermediate in memory representation of sql logical/physical plans

### why?

_The primary reason for this layer is flexibility. By decoupling intermediate data representation from Substrait plans, we can accept multiple data formats. As long as we interpret them into this IR, the physical operators work unchanged_

## Source operator

```bash
{
  "Operator": "Source",
  "Source": {
      "file-name": "link-to-s3",
      "local": false
  }
}
# file ext must end in .csv or .parquet
# local? download to local machine or keep streaming from s3 bucket
```

---

## Project operator

**sql** : `select a , b , c`

```bash
{
  "Operator": "Project",
  "Project": {
    "input": {operator},
    "expressions": [{Expression},{Expression},{Expression}]
  }
}
```

#### example

```bash
{
  "Operator": "Project",
  "Project": {
    "input": {
      "Operator": "Source",
      "Source": {
        "source-node": {
          "file-name": "country-full.csv",
          "local": false
        }
      }
    },
    "expressions": [
      { "Expression": "<Expression>" },
      { "Expression": "<Expression>" },
      { "Expression": "<Expression>" }
    ]
  }
}
```

---

## Filter Operator

**sql**: `select a,b from source where a > 10`

```bash
{
  "Operator": "Filter",
  "Filter": {
    "input": {operator},
    "expression": {Expression}
  }
}
```

**Example**

```bash
{
  "Operator": "Filter",
  "Filter": {
    "input": {
      "Operator": "Source",
      "Source": {
        "source-node": {
          "file-name": "s3://bucket/data.csv",
          "local": false
        }
      }
    },
    "expression": {
      "expr_type": "LiteralResolve",
      "value": 10,
      "lit_type": "int"
    }
  }
}
```

---

## Distinct Operator

**sql**: `select distinct a, b from source`

```bash
{
  "Operator": "Distinct",
  "Distinct": {
    "input": {operator},
    "expressions": [{Expression},{Expression},{Expression}]
  }
}
```

- Removes duplicate rows based on the specified columns
- Output includes only the listed columns

---

## Limit Operator

**sql**: `select a,b from source limit 10`

```bash
{
  "Operator": "Limit",
  "Limit": {
    "input": {operator},
    "limit": 10
  }
}
```

#### max value for limit is 2^16-1 (max uint16)

---

## Sort Operator

**sql**: `select a,b from source order by a desc, b asc`

```bash
{
  "Operator": "Sort",
  "Sort": {
    "input": {operator},
    "by": [
      {
        "expr": {Expression},
        "asc": boolean
      },
      {
        "expr": {Expression},
        "asc": boolean
      }
    ]
  }
}
```

- `order` defaults to `ASC` if omitted

---

## Single Column Aggregation Operator

**sql**: `select sum(a) from source`

```bash
{
  "Operator": "Aggregate",
  "Aggregate": {
    "input": {operator},
    "aggrs": [
      {
      "function": "sum",
      "expr": {Expression},
      }
    ]
  }
}
```

- Operates on exactly one column
- `alias` is **optional**
- Output contains a single row

---

## Having Operator

**sql**: `select sum(a) from source having sum(a) > 10`

```bash
{
  "Operator": "Having",
  "Having": {
    "input": {operator},
    "expression": {Expression}
  }
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
{
  "Operator": "Join",
  "Join": {
    "left": {operator},
    "right": {operator},
    "join_type": "Inner",
    "on": [
      {
        "left":  { "expr_type": "ColumnResolve", "name": "a.id" },
        "right": { "expr_type": "ColumnResolve", "name": "b.id" }
      },
      {
        "left":  { "expr_type": "ColumnResolve", "name": "a.age" },
        "right": { "expr_type": "ColumnResolve", "name": "b.distance" }
      }
    ]
  }
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
{
  "Operator": "GroupBy",
  "GroupBy": {
    "input": {operator},
    "group_by": [
      { "expr_type": "ColumnResolve", "name": "b" }
    ],
    "aggrs": [
      {
        "function": "Sum",
        "expr": {Expression},
      }
    ]
  }
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
{
  "Emit": {
      "Operator": "Filter",
      "Filter": {
        "input": {
          "Operator": "Source",
          "Source": {
            "source-node": {
              "file-name": "s3://bucket/data.csv",
              "local": false
            }
          }
        },
        "expression": {
          "expr_type": "BinaryExpr",
          "op": "GreaterThan",
          "left": { "expr_type": "ColumnResolve", "name": "a" },
          "right": {
            "expr_type": "LiteralResolve",
            "value": 10,
            "lit_type": "int"
          }
        }
      }

  }
}
```

---

## Example 2 — Source → Project → Sort

**sql**: `select a, b from source order by a`

```bash
{
  "Emit": {
      "Operator": "Sort",
      "Sort": {
        "input": {
          "Operator": "Project",
          "Project": {
            "input": {
              "Operator": "Source",
              "Source": {
                "source-node": {
                  "file-name": "s3://bucket/data.csv",
                  "local": false
                }
              }
            },
            "expressions": [
              { "Expression": "a" },
              { "Expression": "b" }
            ]
          }
        },
        "by": [
          {
            "Expr": { "expr_type": "ColumnResolve", "name": "a" },
            "asc": true
          }
        ]
      }
  }
}
```

---

## Example 3 — Source → Group By → Aggregate

**sql**: `select b, count(a) from source group by b`

```bash
{
  "Emit": {
      "Operator": "GroupBy",
      "GroupBy": {
        "input": {
          "Operator": "Source",
          "Source": {
            "source-node": {
              "file-name": "s3://bucket/data.csv",
              "local": false
            }
          }
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
}
```

---

## Example 4 — Source → Distinct → Limit

**sql**: `select distinct a from source limit 5`

```bash
{
  "Emit": {
      "Operator": "Limit",
      "Limit": {
        "input": {
          "Operator": "Distinct",
          "Distinct": {
            "input": {
              "Operator": "Source",
              "Source": {
                "source-node": {
                  "file-name": "s3://bucket/data.csv",
                  "local": false
                }
              }
            },
            "expressions": [
              { "Expression": "a" }
            ]
          }
        },
        "limit": 5
      }
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
{
  "Emit": {
      "Operator": "Limit",
      "Limit": {
        "input": {
          "Operator": "Sort",
          "Sort": {
            "input": {
              "Operator": "Filter",
              "Filter": {
                "input": {
                  "Operator": "Join",
                  "Join": {
                    "left": {
                      "Operator": "Source",
                      "Source": {
                        "source-node": {
                          "file-name": "s3://bucket/users.csv",
                          "local": false
                        }
                      }
                    },
                    "right": {
                      "Operator": "Source",
                      "Source": {
                        "source-node": {
                          "file-name": "s3://bucket/orders.csv",
                          "local": false
                        }
                      }
                    },
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
                    "lit_type": "int"
                  }
                }
              }
            },
            "by": [
              {
                "Expr": { "expr_type": "ColumnResolve", "name": "o.amount" },
                "asc": false
              }
            ]
          }
        },
        "limit": 10
      }
  }
}
```

---

SELECT id, age_years as age from integration_test_data WHERE age > 15 LIMIT 5
