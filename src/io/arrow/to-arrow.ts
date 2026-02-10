import { DType } from '../../types/dtype';
import { IOError } from '../../errors';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function dtypeToArrowType(arrow: any, dtype: DType): any {
  switch (dtype) {
    case DType.Float64:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Float64();
    case DType.Int32:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Int32();
    case DType.Utf8:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Utf8();
    case DType.Boolean:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Bool();
    case DType.Date:
    case DType.DateTime:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.DateMillisecond();
    default:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Utf8();
  }
}

function coerceValue(value: unknown, dtype: DType): unknown {
  if (value === null || value === undefined) return null;
  switch (dtype) {
    case DType.Float64:
    case DType.Int32:
      return Number(value);
    case DType.Boolean:
      return Boolean(value);
    case DType.Date:
    case DType.DateTime:
      if (value instanceof Date) return value.getTime();
      if (typeof value === 'number') return value;
      return new Date(value as string).getTime();
    case DType.Utf8:
    default:
      if (typeof value === 'string') return value;
      if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return String(value);
      return typeof value === 'object' ? JSON.stringify(value) : String(value as string);
  }
}

export interface ToArrowInput {
  columnOrder: string[];
  getColumnValues: (name: string) => { values: unknown[]; dtype: DType };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function toArrowTable(input: ToArrowInput): Promise<any> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let arrow: any;
  try {
    const moduleName = 'apache-arrow';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    arrow = await import(moduleName);
  } catch {
    throw new IOError(
      'apache-arrow is required for Arrow interop but is not installed. Run: npm install apache-arrow',
    );
  }

  const { columnOrder, getColumnValues } = input;

  // Build a record of name -> vector for Table constructor
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const tableData: Record<string, any> = {};

  for (const name of columnOrder) {
    const { values, dtype } = getColumnValues(name);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const arrowType = dtypeToArrowType(arrow, dtype);
    const coerced = values.map((v) => coerceValue(v, dtype));

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    tableData[name] = arrow.vectorFromArray(coerced, arrowType);
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
  return new arrow.Table(tableData);
}
