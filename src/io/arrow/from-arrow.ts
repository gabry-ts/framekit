import { DType } from '../../types/dtype';
import { IOError } from '../../errors';

export interface ParsedArrowData {
  header: string[];
  columns: Record<string, unknown[]>;
  inferredTypes: Record<string, DType>;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function arrowTypeToDType(field: any): DType {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  const typeId = field.type?.typeId as number | undefined;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  const typeStr = String(field.type ?? '');

  // Arrow TypeId constants
  // Int = 2, Float = 3, Utf8 = 5, Bool = 6, Date = 8, Timestamp = 10, Dictionary = -1
  switch (typeId) {
    case 2: // Int
      return DType.Int32;
    case 3: // Float
      return DType.Float64;
    case 5: // Utf8
      return DType.Utf8;
    case 6: // Bool
      return DType.Boolean;
    case 8: // Date
    case 10: // Timestamp
      return DType.Date;
    case -1: // Dictionary (commonly used for strings)
      // Check if the dictionary values are Utf8
      if (typeStr.includes('Utf8')) return DType.Utf8;
      return DType.Utf8;
    default:
      // Fallback: inspect type string representation
      if (typeStr.includes('Int')) return DType.Int32;
      if (typeStr.includes('Float')) return DType.Float64;
      if (typeStr.includes('Utf8') || typeStr.includes('utf8')) return DType.Utf8;
      if (typeStr.includes('Bool')) return DType.Boolean;
      if (typeStr.includes('Date') || typeStr.includes('Timestamp')) return DType.Date;
      return DType.Utf8;
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function fromArrowTable(table: any): ParsedArrowData {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  if (!table || !table.schema || !table.schema.fields) {
    throw new IOError('Invalid Arrow Table: missing schema or fields');
  }

  const header: string[] = [];
  const columns: Record<string, unknown[]> = {};
  const inferredTypes: Record<string, DType> = {};

  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  const numRows = Number(table.numRows);
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
  const fields = table.schema.fields as any[];

  for (const field of fields) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const name = String(field.name);
    header.push(name);

    const dtype = arrowTypeToDType(field);
    inferredTypes[name] = dtype;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const column = table.getChild(name);
    const values: unknown[] = [];

    for (let i = 0; i < numRows; i++) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      const val = column.get(i);
      if (val === null || val === undefined) {
        values.push(null);
      } else if (dtype === DType.Date && typeof val === 'number') {
        values.push(new Date(val));
      } else {
        values.push(val);
      }
    }

    columns[name] = values;
  }

  return { header, columns, inferredTypes };
}
