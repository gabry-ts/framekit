# Error Reference

FrameKit uses a structured error hierarchy. Every error thrown by the library is an instance of `FrameKitError` (or a subclass) and carries a machine-readable `ErrorCode` that you can use for programmatic error handling.

```ts
import {
  FrameKitError,
  ColumnNotFoundError,
  TypeMismatchError,
  ShapeMismatchError,
  ParseError,
  IOError,
  ErrorCode,
} from 'framekit';
```

---

## ErrorCode Enum

```ts
enum ErrorCode {
  COLUMN_NOT_FOUND   = 'COLUMN_NOT_FOUND',
  TYPE_MISMATCH      = 'TYPE_MISMATCH',
  SHAPE_MISMATCH     = 'SHAPE_MISMATCH',
  PARSE_ERROR        = 'PARSE_ERROR',
  IO_ERROR           = 'IO_ERROR',
  OUT_OF_MEMORY      = 'OUT_OF_MEMORY',
  INVALID_OPERATION  = 'INVALID_OPERATION',
}
```

| Code | Meaning |
|------|---------|
| `COLUMN_NOT_FOUND` | A referenced column does not exist in the DataFrame |
| `TYPE_MISMATCH` | An operation received a value or column of an incompatible type |
| `SHAPE_MISMATCH` | Row counts or dimensions are incompatible between DataFrames |
| `PARSE_ERROR` | Input data could not be decoded or validated |
| `IO_ERROR` | A file or network read/write operation failed |
| `OUT_OF_MEMORY` | The operation exceeded available memory |
| `INVALID_OPERATION` | The requested operation is not valid in the current context |

---

## Error Classes

### `FrameKitError`

```ts
class FrameKitError extends Error {
  readonly code: ErrorCode;
  constructor(code: ErrorCode, message: string);
}
```

Base error class for all FrameKit errors. Every error carries a `code` property from the `ErrorCode` enum and a human-readable `message`.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode` | Machine-readable error code |
| `message` | `string` | Human-readable description |

---

### `ColumnNotFoundError`

```ts
class ColumnNotFoundError extends FrameKitError {
  readonly column: string;
  readonly available: string[];
  constructor(column: string, available: string[]);
}
```

Thrown when an operation references a column that does not exist. The `available` property lists all columns that do exist, which is useful for debugging typos.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode.COLUMN_NOT_FOUND` | Always `COLUMN_NOT_FOUND` |
| `column` | `string` | The column name that was not found |
| `available` | `string[]` | List of column names that exist in the DataFrame |

**Common triggers:** `select()`, `drop()`, `col()`, `sortBy()`, `groupBy()`, `join()`, `pivot()`, `melt()`, `rename()`, `withColumn()` (when referencing other columns).

---

### `TypeMismatchError`

```ts
class TypeMismatchError extends FrameKitError {
  constructor(message: string);
}
```

Thrown when a value or column has an incompatible type for the requested operation. For example, attempting arithmetic on a string column or casting to an unsupported type.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode.TYPE_MISMATCH` | Always `TYPE_MISMATCH` |

**Common triggers:** `cast()`, arithmetic expressions on non-numeric columns, date operations on non-date columns.

---

### `ShapeMismatchError`

```ts
class ShapeMismatchError extends FrameKitError {
  constructor(message: string);
}
```

Thrown when two DataFrames have incompatible dimensions. For example, `assign()` requires both DataFrames to have the same number of rows.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode.SHAPE_MISMATCH` | Always `SHAPE_MISMATCH` |

**Common triggers:** `assign()`, `withColumn()` with an array of the wrong length, `concat()` with mismatched columns.

---

### `ParseError`

```ts
class ParseError extends FrameKitError {
  constructor(message: string);
}
```

Thrown when input data cannot be parsed. This includes malformed CSV, invalid JSON, corrupted binary formats, and invalid query strings.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode.PARSE_ERROR` | Always `PARSE_ERROR` |

**Common triggers:** `fromCSV()`, `fromJSON()`, `fromParquet()`, `fromExcel()`, `query()`.

---

### `IOError`

```ts
class IOError extends FrameKitError {
  constructor(message: string);
}
```

Thrown when a file system or network operation fails. This includes missing files, permission errors, and network timeouts.

| Property | Type | Description |
|----------|------|-------------|
| `code` | `ErrorCode.IO_ERROR` | Always `IO_ERROR` |

**Common triggers:** `fromFile()`, `toFile()`, `scanCSV()`, `scanNDJSON()`.

---

## Error Handling

### Basic try/catch

```ts
import { DataFrame, ColumnNotFoundError } from 'framekit';

try {
  df.select('nonexistent');
} catch (err) {
  if (err instanceof ColumnNotFoundError) {
    console.error(`Column "${err.column}" not found.`);
    console.error(`Available columns: ${err.available.join(', ')}`);
  }
}
```

### Checking error codes

When you need to handle errors generically without importing every subclass, check the `code` property:

```ts
import { DataFrame, FrameKitError, ErrorCode } from 'framekit';

try {
  const result = DataFrame.fromCSV(rawData);
  result.cast({ age: 'Int32' });
} catch (err) {
  if (err instanceof FrameKitError) {
    switch (err.code) {
      case ErrorCode.PARSE_ERROR:
        console.error('Failed to parse input:', err.message);
        break;
      case ErrorCode.TYPE_MISMATCH:
        console.error('Type error:', err.message);
        break;
      case ErrorCode.COLUMN_NOT_FOUND:
        console.error('Missing column:', err.message);
        break;
      default:
        console.error(`FrameKit error [${err.code}]:`, err.message);
    }
  } else {
    throw err; // re-throw non-FrameKit errors
  }
}
```

### Guarding I/O operations

```ts
import { DataFrame, IOError, ParseError } from 'framekit';

async function loadData(path: string) {
  try {
    return await DataFrame.fromFile(path);
  } catch (err) {
    if (err instanceof IOError) {
      console.error(`Could not read file "${path}": ${err.message}`);
      return DataFrame.empty();
    }
    if (err instanceof ParseError) {
      console.error(`File "${path}" is malformed: ${err.message}`);
      return DataFrame.empty();
    }
    throw err;
  }
}
```

### Validating columns before operations

To avoid `ColumnNotFoundError` at runtime, check column existence first:

```ts
const required = ['name', 'age', 'email'];
const missing = required.filter(c => !df.columns.includes(c));

if (missing.length > 0) {
  throw new Error(`Missing required columns: ${missing.join(', ')}`);
}

// Safe to proceed
df.select('name', 'age', 'email');
```
