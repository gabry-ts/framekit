export { DType } from './types/dtype';
export type { DTypeToTS } from './types/dtype';
export type { DataType, Schema } from './types/schema';
export type {
  CSVReadOptions,
  CSVWriteOptions,
  JSONReadOptions,
  JSONWriteOptions,
  NDJSONReadOptions,
  PrintOptions,
  SampleOptions,
} from './types/options';
export {
  ErrorCode,
  FrameKitError,
  ColumnNotFoundError,
  TypeMismatchError,
  ShapeMismatchError,
  ParseError,
  IOError,
} from './errors';
export { BitArray } from './storage/bitarray';
export { Column } from './storage/column';
export { Float64Column, Int32Column } from './storage/numeric';
export { Utf8Column } from './storage/string';
export { BooleanColumn } from './storage/boolean';
export { DateColumn } from './storage/date';
export { ObjectColumn } from './storage/object';
export { Series } from './series';
export { StringAccessor } from './accessors/string-accessor';
export { DateAccessor } from './accessors/date-accessor';
export type { TimeUnit } from './accessors/date-accessor';
export { DataFrame } from './dataframe';
export { GroupBy } from './ops/groupby';
export type { JoinType, JoinOnMapping, JoinOptions } from './ops/join';
export type { PivotOptions, PivotAggFunc } from './ops/pivot';
export type { MeltOptions } from './ops/melt';
export {
  Expr,
  NamedExpr,
  col,
  lit,
  AggExpr,
  SumAggExpr,
  MeanAggExpr,
  CountAggExpr,
  CountDistinctAggExpr,
  MinAggExpr,
  MaxAggExpr,
  StdAggExpr,
  FirstAggExpr,
  LastAggExpr,
  ListAggExpr,
  ModeAggExpr,
} from './expr/expr';
export { when, WhenBuilder, ThenBuilder } from './expr/conditional-expr';
export { LazyFrame, LazyGroupBy } from './lazy';
export type {
  PlanNode,
  ScanNode,
  FilterNode,
  SelectNode,
  ProjectNode,
  GroupByNode,
  JoinNode,
  SortNode,
  LimitNode,
  DistinctNode,
} from './engine/lazy/plan';
export { execute } from './engine/lazy/executor';
export { optimize } from './engine/lazy/optimizer';
export type { StreamCSVOptions } from './engine/streaming/scanner';
export type { StreamNDJSONOptions } from './engine/streaming/ndjson-scanner';
export { StringExprAccessor } from './expr/string-expr';
export { DateExprAccessor } from './expr/date-expr';
export {
  WindowRankExpr,
  WindowDenseRankExpr,
  WindowRowNumberExpr,
  WindowPercentRankExpr,
  WindowNtileExpr,
  CumSumExpr,
  CumMaxExpr,
  CumMinExpr,
  CumProdExpr,
  CumCountExpr,
  ShiftExpr,
  DiffExpr,
  PctChangeExpr,
  PartitionedWindowExpr,
  OrderedWindowExpr,
} from './ops/window';
import './expr/expr-accessors';
import './ops/window';

import { DataFrame } from './dataframe';

/**
 * Shorthand factory for creating a DataFrame from an array of objects.
 * Equivalent to DataFrame.fromRows(rows).
 */
export function df<S extends Record<string, unknown> = Record<string, unknown>>(
  rows: object[],
): DataFrame<S> {
  return DataFrame.fromRows<S>(rows);
}
