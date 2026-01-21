import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

describe('hashJoin — inner join (US-031)', () => {
  const left = DataFrame.fromRows([
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Charlie', age: 35 },
    { id: 4, name: 'Diana', age: 28 },
  ]);

  const right = DataFrame.fromRows([
    { id: 1, dept: 'Engineering' },
    { id: 2, dept: 'Marketing' },
    { id: 5, dept: 'Sales' },
  ]);

  it('performs inner join on single key column', () => {
    const result = left.join(right, 'id');
    expect(result.length).toBe(2);
    expect(result.columns).toEqual(['id', 'name', 'age', 'dept']);
    expect(result.col('id').toArray()).toEqual([1, 2]);
    expect(result.col('name').toArray()).toEqual(['Alice', 'Bob']);
    expect(result.col('dept').toArray()).toEqual(['Engineering', 'Marketing']);
  });

  it('default join type is inner', () => {
    const result = left.join(right, 'id');
    const explicit = left.join(right, 'id', 'inner');
    expect(result.toArray()).toEqual(explicit.toArray());
  });

  it('supports composite key joins', () => {
    const l = DataFrame.fromRows([
      { a: 1, b: 'x', val: 10 },
      { a: 1, b: 'y', val: 20 },
      { a: 2, b: 'x', val: 30 },
    ]);
    const r = DataFrame.fromRows([
      { a: 1, b: 'x', score: 100 },
      { a: 2, b: 'x', score: 200 },
      { a: 3, b: 'z', score: 300 },
    ]);
    const result = l.join(r, ['a', 'b']);
    expect(result.length).toBe(2);
    expect(result.col('a').toArray()).toEqual([1, 2]);
    expect(result.col('b').toArray()).toEqual(['x', 'x']);
    expect(result.col('val').toArray()).toEqual([10, 30]);
    expect(result.col('score').toArray()).toEqual([100, 200]);
  });

  it('key column appears once in result', () => {
    const result = left.join(right, 'id');
    const idCols = result.columns.filter((c) => c === 'id');
    expect(idCols.length).toBe(1);
  });

  it('only matching rows appear in inner join', () => {
    const result = left.join(right, 'id');
    // id=3,4 from left and id=5 from right should not appear
    const ids = result.col('id').toArray();
    expect(ids).not.toContain(3);
    expect(ids).not.toContain(4);
    expect(ids).not.toContain(5);
  });

  it('handles null keys correctly (nulls do not match)', () => {
    const l = DataFrame.fromRows([
      { id: 1, val: 'a' },
      { id: null, val: 'b' },
      { id: 2, val: 'c' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, score: 10 },
      { id: null, score: 20 },
    ]);
    const result = l.join(r, 'id');
    expect(result.length).toBe(1);
    expect(result.col('id').toArray()).toEqual([1]);
    expect(result.col('val').toArray()).toEqual(['a']);
    expect(result.col('score').toArray()).toEqual([10]);
  });

  it('handles many-to-many matches', () => {
    const l = DataFrame.fromRows([
      { id: 1, lv: 'a' },
      { id: 1, lv: 'b' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, rv: 'x' },
      { id: 1, rv: 'y' },
    ]);
    const result = l.join(r, 'id');
    expect(result.length).toBe(4); // 2 x 2 cartesian for matching key
    expect(result.col('id').toArray()).toEqual([1, 1, 1, 1]);
    expect(result.col('lv').toArray()).toEqual(['a', 'a', 'b', 'b']);
    expect(result.col('rv').toArray()).toEqual(['x', 'y', 'x', 'y']);
  });

  it('renames duplicate non-key columns with _right suffix', () => {
    const l = DataFrame.fromRows([
      { id: 1, value: 10 },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, value: 20 },
    ]);
    const result = l.join(r, 'id');
    expect(result.columns).toContain('value');
    expect(result.columns).toContain('value_right');
    expect(result.col('value').toArray()).toEqual([10]);
    expect(result.col('value_right').toArray()).toEqual([20]);
  });

  it('returns empty DataFrame when no keys match', () => {
    const l = DataFrame.fromRows([{ id: 1, v: 'a' }]);
    const r = DataFrame.fromRows([{ id: 2, v: 'b' }]);
    const result = l.join(r, 'id');
    expect(result.length).toBe(0);
    expect(result.columns).toEqual(['id', 'v', 'v_right']);
  });

  it('throws ColumnNotFoundError for invalid key in left', () => {
    expect(() => left.join(right, 'nonexistent')).toThrow('nonexistent');
  });

  it('throws ColumnNotFoundError for invalid key in right', () => {
    const r = DataFrame.fromRows([{ other: 1 }]);
    expect(() => left.join(r, 'id')).toThrow('id');
  });

  it('preserves column types through join', () => {
    const result = left.join(right, 'id');
    expect(result.col('id').toArray()[0]).toBeTypeOf('number');
    expect(result.col('name').toArray()[0]).toBeTypeOf('string');
    expect(result.col('dept').toArray()[0]).toBeTypeOf('string');
  });

  it('handles join with date key columns', () => {
    const d1 = new Date('2024-01-01');
    const d2 = new Date('2024-01-02');
    const l = DataFrame.fromRows([
      { date: d1, val: 1 },
      { date: d2, val: 2 },
    ]);
    const r = DataFrame.fromRows([
      { date: d1, score: 10 },
    ]);
    const result = l.join(r, 'date');
    expect(result.length).toBe(1);
    expect(result.col('val').toArray()).toEqual([1]);
    expect(result.col('score').toArray()).toEqual([10]);
  });

  it('handles join with boolean key columns', () => {
    const l = DataFrame.fromRows([
      { active: true, name: 'Alice' },
      { active: false, name: 'Bob' },
    ]);
    const r = DataFrame.fromRows([
      { active: true, dept: 'Eng' },
    ]);
    const result = l.join(r, 'active');
    expect(result.length).toBe(1);
    expect(result.col('name').toArray()).toEqual(['Alice']);
  });
});

describe('hashJoin — left join (US-032)', () => {
  const left = DataFrame.fromRows([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
    { id: 4, name: 'Diana' },
  ]);

  const right = DataFrame.fromRows([
    { id: 1, dept: 'Engineering' },
    { id: 2, dept: 'Marketing' },
    { id: 5, dept: 'Sales' },
  ]);

  it('keeps all left rows, fills null for non-matching right columns', () => {
    const result = left.join(right, 'id', 'left');
    expect(result.length).toBe(4);
    expect(result.col('id').toArray()).toEqual([1, 2, 3, 4]);
    expect(result.col('name').toArray()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana']);
    expect(result.col('dept').toArray()).toEqual(['Engineering', 'Marketing', null, null]);
  });

  it('left join includes unmatched left rows with null right values', () => {
    const result = left.join(right, 'id', 'left');
    // id=3 and id=4 have no match in right, so dept is null
    const rows = result.toArray();
    const row3 = rows.find((r) => r['id'] === 3)!;
    expect(row3['dept']).toBeNull();
  });

  it('left join with null keys preserves left rows', () => {
    const l = DataFrame.fromRows([
      { id: 1, val: 'a' },
      { id: null, val: 'b' },
      { id: 2, val: 'c' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, score: 10 },
      { id: null, score: 20 },
    ]);
    const result = l.join(r, 'id', 'left');
    expect(result.length).toBe(3);
    expect(result.col('val').toArray()).toEqual(['a', 'b', 'c']);
    // null key row gets null score
    expect(result.col('score').toArray()).toEqual([10, null, null]);
  });

  it('left join with all matches is equivalent to inner join', () => {
    const l = DataFrame.fromRows([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, dept: 'Eng' },
      { id: 2, dept: 'Mkt' },
    ]);
    const leftResult = l.join(r, 'id', 'left');
    const innerResult = l.join(r, 'id', 'inner');
    expect(leftResult.toArray()).toEqual(innerResult.toArray());
  });
});

describe('hashJoin — right join (US-032)', () => {
  const left = DataFrame.fromRows([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
  ]);

  const right = DataFrame.fromRows([
    { id: 1, dept: 'Engineering' },
    { id: 4, dept: 'Sales' },
    { id: 5, dept: 'HR' },
  ]);

  it('keeps all right rows, fills null for non-matching left columns', () => {
    const result = left.join(right, 'id', 'right');
    expect(result.length).toBe(3);
    expect(result.col('id').toArray()).toEqual([1, 4, 5]);
    expect(result.col('name').toArray()).toEqual(['Alice', null, null]);
    expect(result.col('dept').toArray()).toEqual(['Engineering', 'Sales', 'HR']);
  });

  it('right join includes unmatched right rows with null left values', () => {
    const result = left.join(right, 'id', 'right');
    const rows = result.toArray();
    const row4 = rows.find((r) => r['id'] === 4)!;
    expect(row4['name']).toBeNull();
    expect(row4['dept']).toBe('Sales');
  });

  it('right join key column comes from right for unmatched rows', () => {
    const result = left.join(right, 'id', 'right');
    // id=4 and id=5 come from right side since no left match
    const ids = result.col('id').toArray();
    expect(ids).toContain(4);
    expect(ids).toContain(5);
  });
});

describe('hashJoin — outer join (US-032)', () => {
  const left = DataFrame.fromRows([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
  ]);

  const right = DataFrame.fromRows([
    { id: 2, dept: 'Marketing' },
    { id: 4, dept: 'Sales' },
  ]);

  it('keeps all rows from both sides, filling nulls for non-matches', () => {
    const result = left.join(right, 'id', 'outer');
    expect(result.length).toBe(4); // 3 left + 1 unmatched right
    expect(result.col('id').toArray()).toEqual([1, 2, 3, 4]);
    expect(result.col('name').toArray()).toEqual(['Alice', 'Bob', 'Charlie', null]);
    expect(result.col('dept').toArray()).toEqual([null, 'Marketing', null, 'Sales']);
  });

  it('outer join with no overlap includes all rows', () => {
    const l = DataFrame.fromRows([
      { id: 1, val: 'a' },
    ]);
    const r = DataFrame.fromRows([
      { id: 2, val: 'b' },
    ]);
    const result = l.join(r, 'id', 'outer');
    expect(result.length).toBe(2);
    expect(result.col('id').toArray()).toEqual([1, 2]);
  });

  it('outer join with full overlap is equivalent to inner join', () => {
    const l = DataFrame.fromRows([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, dept: 'Eng' },
      { id: 2, dept: 'Mkt' },
    ]);
    const outerResult = l.join(r, 'id', 'outer');
    const innerResult = l.join(r, 'id', 'inner');
    expect(outerResult.toArray()).toEqual(innerResult.toArray());
  });

  it('outer join with null keys preserves rows from both sides', () => {
    const l = DataFrame.fromRows([
      { id: 1, val: 'a' },
      { id: null, val: 'b' },
    ]);
    const r = DataFrame.fromRows([
      { id: 1, score: 10 },
      { id: null, score: 20 },
    ]);
    const result = l.join(r, 'id', 'outer');
    // id=1 matches, null left row kept, null right row kept
    expect(result.length).toBe(3);
  });

  it('outer join with composite keys', () => {
    const l = DataFrame.fromRows([
      { a: 1, b: 'x', lv: 10 },
      { a: 2, b: 'y', lv: 20 },
    ]);
    const r = DataFrame.fromRows([
      { a: 1, b: 'x', rv: 100 },
      { a: 3, b: 'z', rv: 300 },
    ]);
    const result = l.join(r, ['a', 'b'], 'outer');
    expect(result.length).toBe(3);
    expect(result.col('a').toArray()).toEqual([1, 2, 3]);
    expect(result.col('b').toArray()).toEqual(['x', 'y', 'z']);
    expect(result.col('lv').toArray()).toEqual([10, 20, null]);
    expect(result.col('rv').toArray()).toEqual([100, null, 300]);
  });
});

describe('join type argument (US-032)', () => {
  it('default join type is inner', () => {
    const l = DataFrame.fromRows([{ id: 1, v: 'a' }, { id: 2, v: 'b' }]);
    const r = DataFrame.fromRows([{ id: 1, w: 'x' }]);
    const defaultResult = l.join(r, 'id');
    const innerResult = l.join(r, 'id', 'inner');
    expect(defaultResult.toArray()).toEqual(innerResult.toArray());
  });

  it('join type is passed as third argument', () => {
    const l = DataFrame.fromRows([{ id: 1, v: 'a' }]);
    const r = DataFrame.fromRows([{ id: 1, w: 'x' }, { id: 2, w: 'y' }]);
    // Each type returns different results
    const inner = l.join(r, 'id', 'inner');
    const leftJ = l.join(r, 'id', 'left');
    const rightJ = l.join(r, 'id', 'right');
    const outer = l.join(r, 'id', 'outer');
    expect(inner.length).toBe(1);
    expect(leftJ.length).toBe(1);
    expect(rightJ.length).toBe(2);
    expect(outer.length).toBe(2);
  });
});
