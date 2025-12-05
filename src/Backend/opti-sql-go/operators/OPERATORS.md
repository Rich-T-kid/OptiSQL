# Operators — quick reference

This document gives a concise overview of the operator model used in this repository, how to construct the most common operators, and what each operator's constructor expects and why. Placeholders like `Expr.Expression` and `RecordBatch` refer to the repository types found under `Expr` and `operators/record.go`.

## What is an Operator?

An operator implements the `operators.Operator` interface:

- `Next(n uint16) (*operators.RecordBatch, error)` — return up to `n` rows (many operators ignore the exact n and read/produce what they need). Returns `io.EOF` when finished.
- `Schema() *arrow.Schema` — the operator's output schema.
- `Close() error` — release resources (files, network handles, etc.).

The basic data unit is `operators.RecordBatch` (schema + Arrow arrays + rowcount). Operators compose: the output of one operator becomes the input (child) of the next.

## Leaf (source) operators

Leaf operators are the pipeline entry points. They read data from some storage and produce `RecordBatch` values.

- CSV source
  - Constructor: `project.NewProjectCSVLeaf(io.Reader)`
  - Inputs: an `io.Reader` (file, buffer). Produces typed Arrow arrays from CSV columns.
  - Notes: simple, fast for local CSVs. Use when you want a streaming CSV source.

- Parquet source
  - Constructor: (parquet reader; see project package)
  - Inputs: parquet file handle. Produces Arrow arrays preserving parquet types.

- In-memory source
  - Constructor: `project.NewInMemoryProjectExec(names []string, columns []any)`
  - Inputs: column names and Go slices (used heavily in unit tests).
  - Notes: useful for deterministic test inputs and small-memory datasets.

- S3 / NetworkResource
  - use `project.NewStreamReader` to create a network file reader. this just means it allows chunk reading of files not on local disk.
  - Notes: the repository supports reading remote files; a configuration option lets you download the full remote file first to avoid per-request network latency when the operator needs repeated random access (e.g., for Parquet or when sorting). This is exposed as a NetworkResource / download option in the project/source constructors.
  - the result of `project.NewStreamReader(fileName)` can be passed directly to `project.NewProjectCSVLeaf(io.Reader)` and `project.NewParquetSource(readSeeker)`. This was intentional so its seemless to work with s3 files as possible

## How to construct operators — summary of common operators

The pattern is consistent: each operator has a `NewXxx...` constructor that takes one or more child operators, expression descriptors, or configuration params.

### Project (Select)
- Constructor: `project.NewProjectExec(child operators.Operator, exprs []Expr.Expression)`
- Purpose: evaluate a list of projection expressions (column refs, scalar functions, aliases) and return a batch with only the requested columns.
- What to pass in:
  - `child` — the input operator to project from (leaf or intermediate op).
  - `exprs` — expressions created with `Expr.NewColumnResolve`, `Expr.NewLiteralResolve`, `Expr.NewAlias`, `Expr.NewScalarFunction`, etc.
- Why: keeps expression evaluation centralized and lets downstream operators work with a narrow schema.

### Filter
- Constructor: `filter.NewFilterExec(child operators.Operator, predicate Expr.Expression)`
- Purpose: apply boolean predicates to input rows and emit only matching rows.
- What to pass in:
  - `child` — operator producing input rows.
  - `predicate` — an `Expr.Expression` that evaluates to boolean (can combine binary operators, scalar functions, null checks).
- Why: decouples predicate evaluation from projection and other operators; filter may buffer results across batches to serve limit-like requests.

### Limit
- Constructor: `filter.NewLimitExec(child operators.Operator, limit uint64)`
- Purpose: stop the pipeline after `limit` rows are emitted.
- What to pass in: the `child` operator and the numeric `limit`.
- Why: simple consumer-side cap; implemented as a thin operator above any child.

### Distinct
- Constructor: `filter.NewDistinctExec(child operators.Operator, colExprs []Expr.Expression)`
- Purpose: remove duplicate rows on the selected key columns.
- What to pass in: `child` and the list of key column expressions.
- Why: used to produce unique values for a given set of columns; often followed by `Sort` for deterministic order.

### Sort / TopK
- Constructors:
  - `aggr.NewSortExec(child operators.Operator, sortKeys []aggr.SortKey)` — fully sorts input
  - `aggr.NewTopKSortExec(child operators.Operator, sortKeys []aggr.SortKey, k uint16)` — keep top-k
- Purpose: order rows by one or more columns.
- What to pass in:
  - `child` — input operator
  - `sortKeys` — built with `aggr.NewSortKey(expr Expr.Expression, asc bool)`; multiple keys are combined with `aggr.CombineSortKeys(...)`.
- Why: some consumers require sorted input (ORDER BY) or only the top-k entries (TopK).
- Notes: current implementations read data into memory and sort; care must be taken for large datasets.

### GroupBy / Aggregation
- Constructors:
  - `aggr.NewGroupByExec(child operators.Operator, groupExpr []aggr.AggregateFunctions, groupBy []Expr.Expression)` — group-by with aggregates
  - `aggr.NewGlobalAggrExec(child operators.Operator, aggExprs []aggr.AggregateFunctions)` — global aggregation (no GROUP BY)
- Purpose: compute aggregates (SUM, AVG, COUNT, MIN, MAX) grouped by one or more columns.
- What to pass in:
  - `child` — input operator
  - `groupExpr` / `aggExprs` — list of `aggr.AggregateFunctions` (built with `aggr.NewAggregateFunctions(aggr.AggrFunc, Expr.Expression)`) describing the aggregate function and its child expression (usually a column).
  - `groupBy` — expressions for the group-by keys (column resolves).
- Why: central place for aggregator logic; constructors validate types (numeric types for SUM/AVG) and construct the output schema.

### Join (HashJoin)
- Constructor: `join.NewHashJoinExec(left, right operators.Operator, clause join.JoinClause, joinType join.JoinType, filters []Expr.Expression)`
- Purpose: perform hash-based joins (Inner, Left, Right).
- What to pass in:
  - `left`, `right` — child operators for the two sides of the join (usually scans or projections)
  - `clause` — `join.NewJoinClause(leftExprs []Expr.Expression, rightExprs []Expr.Expression)` describing which columns pair together (supports multiple equality clauses)
  - `joinType` — `join.InnerJoin`, `join.LeftJoin`, etc.
  - `filters` — optional post-join filters (not always used) | still need to implement this but no time soon, as these can just be treated as Filter Opererations
- Why: joins combine rows from two inputs. The constructor validates schema compatibility and builds the combined output schema (prefixing duplicate column names with `left_`/`right_`).
- Implementation notes: the HashJoin reads the entirety of both children (current implementation) into memory and builds a hash table on the right side for probing.

## Common constructor patterns & rationale

- Child operator(s) always come first: most operators are constructed around one input (`child`) or two (`left`, `right`). This makes pipelines composable.
- Expressions are passed as `Expr.Expression` objects. Use the `Expr` package helpers to build column resolves, literals, scalar functions, binary operators and aliases.
- Constructors perform validation: type checking for aggregates, matching # of join expressions, or validity of projection expressions — this fails fast at construction time instead of at runtime.
- Many blocking operators (Sort, GroupBy, Join) read the full input before producing output. Be careful with large inputs — these operators are not yet externalized (spill-to-disk) and may require configuration or chunking for large datasets.

## Practical examples (pseudocode)

- Project + Filter + Limit pipeline:

```go
src := project.NewProjectCSVLeaf(fileReader)
pred := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.GreaterThan, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, 30))
filt, _ := filter.NewFilterExec(src, pred)
projExprs := Expr.NewExpressions(Expr.NewColumnResolve("id"), Expr.NewColumnResolve("name"))
proj, _ := project.NewProjectExec(filt, projExprs)
lim, _ := filter.NewLimitExec(proj, 10)
batch, _ := lim.Next(10)
```

- GroupBy example:

```go
col := func(n string) Expr.Expression { return Expr.NewColumnResolve(n) }
aggs := []aggr.AggregateFunctions{{AggrFunc: aggr.Sum, Child: col("salary")}}
gb, _ := aggr.NewGroupByExec(src, aggs, []Expr.Expression{col("department")})
result, _ := gb.Next(1000)
```

- HashJoin example (equality on `id`):

```go
clause := join.NewJoinClause([]Expr.Expression{Expr.NewColumnResolve("id")}, []Expr.Expression{Expr.NewColumnResolve("id")})
j, _ := join.NewHashJoinExec(leftSrc, rightSrc, clause, join.InnerJoin, nil)
batch, _ := j.Next(100)
```

## Notes & best practices

- Always call `Close()` on the root operator when done (after `Next` returns `io.EOF`) to release files and network handles.
- Use `project.NewInMemoryProjectExec` for tests — it builds reproducible `RecordBatch` inputs quickly.
- When writing pipelines that may read remote files, prefer to configure the source to download the whole file if the operator will need random access or many read passes (sorting, joining, grouping). This avoids repeated network calls and unpredictable latency.
- Watch out for duplicate column names after joins: the join constructor prefixes with `left_`/`right_` when needed.

## Where to look next in the codebase
- `operators/record.go` — `Operator` interface and `RecordBatch` helpers (builder, PrettyPrint).
- `operators/project/` — project implementations and CSV/parquet readers.
- `operators/filter/` — Filter, Limit, Distinct operator implementations.
- `operators/aggr/` — Sort, TopK, GroupBy and aggregate implementations.
- `operators/Join/` — HashJoin implementation.

Reading the tests
-----------------

For concrete examples of how SQL statements map to operator pipelines, read the integration/unit tests in `operators/test/` (and other test files under `operators/`). The tests build real pipelines (CSV/InMemory -> Filter/Project/Join/GroupBy/Sort/etc.) and show the exact constructor calls and expressions used to represent SQL queries. They are the best source of truth for small end-to-end examples.