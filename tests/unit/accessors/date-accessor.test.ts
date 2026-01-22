import { describe, it, expect } from 'vitest';
import { Series } from '../../../src/series';
import { DateColumn } from '../../../src/storage/date';
import { Float64Column } from '../../../src/storage/numeric';
import { TypeMismatchError } from '../../../src/errors';

function dateSeries(
  name: string,
  values: (Date | null)[],
): Series<Date> {
  return new Series<Date>(name, DateColumn.from(values));
}

describe('DateAccessor', () => {
  const dates = [
    new Date(2024, 0, 15, 10, 30, 45), // Jan 15, 2024 10:30:45
    new Date(2024, 5, 20, 14, 15, 30), // Jun 20, 2024 14:15:30
    new Date(2024, 11, 31, 23, 59, 59), // Dec 31, 2024 23:59:59
  ];
  const s = dateSeries('ts', dates);

  describe('constructor validation', () => {
    it('throws TypeMismatchError for non-Date series', () => {
      const numSeries = new Series<number>('num', Float64Column.from([1, 2]));
      expect(() => (numSeries as unknown as Series<Date>).dt).toThrow(
        TypeMismatchError,
      );
    });
  });

  describe('year()', () => {
    it('extracts year from dates', () => {
      const result = s.dt.year();
      expect(result.toArray()).toEqual([2024, 2024, 2024]);
    });
  });

  describe('month()', () => {
    it('extracts 1-indexed month', () => {
      const result = s.dt.month();
      expect(result.toArray()).toEqual([1, 6, 12]);
    });
  });

  describe('day()', () => {
    it('extracts day of month', () => {
      const result = s.dt.day();
      expect(result.toArray()).toEqual([15, 20, 31]);
    });
  });

  describe('hour()', () => {
    it('extracts hour', () => {
      const result = s.dt.hour();
      expect(result.toArray()).toEqual([10, 14, 23]);
    });
  });

  describe('minute()', () => {
    it('extracts minute', () => {
      const result = s.dt.minute();
      expect(result.toArray()).toEqual([30, 15, 59]);
    });
  });

  describe('second()', () => {
    it('extracts second', () => {
      const result = s.dt.second();
      expect(result.toArray()).toEqual([45, 30, 59]);
    });
  });

  describe('dayOfWeek()', () => {
    it('returns 0-6 (Sunday=0)', () => {
      const result = s.dt.dayOfWeek();
      const arr = result.toArray();
      // Jan 15, 2024 is Monday (1), Jun 20 is Thursday (4), Dec 31 is Tuesday (2)
      expect(arr).toEqual([
        dates[0]!.getDay(),
        dates[1]!.getDay(),
        dates[2]!.getDay(),
      ]);
    });
  });

  describe('dayOfYear()', () => {
    it('returns day of year', () => {
      const result = s.dt.dayOfYear();
      expect(result.get(0)).toBe(15); // Jan 15
      expect(typeof result.get(1)).toBe('number');
      expect(result.get(2)).toBe(366); // Dec 31, 2024 (leap year)
    });
  });

  describe('weekNumber()', () => {
    it('returns ISO week number', () => {
      const result = s.dt.weekNumber();
      expect(result.get(0)).toBeGreaterThanOrEqual(1);
      expect(result.get(0)).toBeLessThanOrEqual(53);
    });
  });

  describe('quarter()', () => {
    it('returns quarter 1-4', () => {
      const result = s.dt.quarter();
      expect(result.toArray()).toEqual([1, 2, 4]);
    });
  });

  describe('timestamp()', () => {
    it('returns Unix ms values', () => {
      const result = s.dt.timestamp();
      expect(result.toArray()).toEqual(dates.map((d) => d.getTime()));
    });
  });

  describe('format()', () => {
    it('formats dates with pattern', () => {
      const result = s.dt.format('YYYY-MM-DD');
      expect(result.toArray()).toEqual([
        '2024-01-15',
        '2024-06-20',
        '2024-12-31',
      ]);
    });

    it('formats with time components', () => {
      const result = s.dt.format('YYYY-MM-DD HH:mm:ss');
      expect(result.toArray()).toEqual([
        '2024-01-15 10:30:45',
        '2024-06-20 14:15:30',
        '2024-12-31 23:59:59',
      ]);
    });
  });

  describe('truncate()', () => {
    const d = new Date(2024, 5, 15, 10, 30, 45); // Jun 15, 2024 10:30:45
    const ts = dateSeries('t', [d]);

    it('truncates to year', () => {
      const result = ts.dt.truncate('year');
      expect(result.get(0)).toEqual(new Date(2024, 0, 1));
    });

    it('truncates to month', () => {
      const result = ts.dt.truncate('month');
      expect(result.get(0)).toEqual(new Date(2024, 5, 1));
    });

    it('truncates to day', () => {
      const result = ts.dt.truncate('day');
      expect(result.get(0)).toEqual(new Date(2024, 5, 15));
    });

    it('truncates to hour', () => {
      const result = ts.dt.truncate('hour');
      expect(result.get(0)).toEqual(new Date(2024, 5, 15, 10));
    });

    it('truncates to minute', () => {
      const result = ts.dt.truncate('minute');
      expect(result.get(0)).toEqual(new Date(2024, 5, 15, 10, 30));
    });

    it('truncates to second', () => {
      const result = ts.dt.truncate('second');
      expect(result.get(0)).toEqual(new Date(2024, 5, 15, 10, 30, 45));
    });
  });

  describe('null handling', () => {
    const withNull = dateSeries('ts', [
      new Date(2024, 0, 1),
      null,
      new Date(2024, 6, 4),
    ]);

    it('year() propagates nulls', () => {
      expect(withNull.dt.year().toArray()).toEqual([2024, null, 2024]);
    });

    it('month() propagates nulls', () => {
      expect(withNull.dt.month().toArray()).toEqual([1, null, 7]);
    });

    it('day() propagates nulls', () => {
      expect(withNull.dt.day().toArray()).toEqual([1, null, 4]);
    });

    it('timestamp() propagates nulls', () => {
      const arr = withNull.dt.timestamp().toArray();
      expect(arr[0]).toBe(new Date(2024, 0, 1).getTime());
      expect(arr[1]).toBeNull();
      expect(arr[2]).toBe(new Date(2024, 6, 4).getTime());
    });

    it('format() propagates nulls', () => {
      expect(withNull.dt.format('YYYY-MM-DD').toArray()).toEqual([
        '2024-01-01',
        null,
        '2024-07-04',
      ]);
    });

    it('truncate() propagates nulls', () => {
      const result = withNull.dt.truncate('month');
      expect(result.get(0)).toEqual(new Date(2024, 0, 1));
      expect(result.get(1)).toBeNull();
      expect(result.get(2)).toEqual(new Date(2024, 6, 1));
    });

    it('dayOfWeek() propagates nulls', () => {
      const arr = withNull.dt.dayOfWeek().toArray();
      expect(arr[1]).toBeNull();
    });

    it('quarter() propagates nulls', () => {
      expect(withNull.dt.quarter().toArray()).toEqual([1, null, 3]);
    });
  });

  describe('all-null series', () => {
    const allNull = dateSeries('ts', [null, null]);

    it('year() returns all nulls', () => {
      expect(allNull.dt.year().toArray()).toEqual([null, null]);
    });

    it('format() returns all nulls', () => {
      expect(allNull.dt.format('YYYY').toArray()).toEqual([null, null]);
    });

    it('truncate() returns all nulls', () => {
      expect(allNull.dt.truncate('day').toArray()).toEqual([null, null]);
    });
  });

  describe('Series .dt getter', () => {
    it('returns DateAccessor instance', () => {
      const accessor = s.dt;
      expect(accessor).toBeDefined();
      expect(accessor.year().length).toBe(3);
    });
  });
});
