# Expressions and Aggregations

## Expression Factories

- `col(name)`
- `lit(value)`

## Expression Families

- Arithmetic: `add`, `sub`, `mul`, `div`, `mod`, `pow`
- Comparison: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
- Logical: `and`, `or`, `not`
- Null and conditional helpers: `coalesce`, `fillNull`, `when/then/otherwise`
- String/date accessors for rich transformations

## Aggregations

- Numeric/statistical: `sum`, `mean`, `min`, `max`, `std`
- Cardinality/list: `count`, `countDistinct`, `list`, `mode`
- Position: `first`, `last`
- Correlation: `op.corr(col('x'), col('y'))`

## Row Number Helpers

- Window-style row numbering via expression API
- Derive helper with `op.rowNumber()` in row-construction pipelines
