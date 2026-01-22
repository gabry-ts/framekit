import { DType } from '../types/dtype';
import { Series } from '../series';
import { Float64Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { DateColumn } from '../storage/date';
import { TypeMismatchError } from '../errors';

export type TimeUnit =
  | 'year'
  | 'month'
  | 'day'
  | 'hour'
  | 'minute'
  | 'second';

export class DateAccessor {
  private readonly _series: Series<Date>;

  constructor(series: Series<Date>) {
    if (series.dtype !== DType.Date) {
      throw new TypeMismatchError(
        `DateAccessor requires Series with dtype Date, got '${series.dtype}'`,
      );
    }
    this._series = series;
  }

  year(): Series<number> {
    return this._mapNumber((d) => d.getFullYear());
  }

  month(): Series<number> {
    return this._mapNumber((d) => d.getMonth() + 1);
  }

  day(): Series<number> {
    return this._mapNumber((d) => d.getDate());
  }

  hour(): Series<number> {
    return this._mapNumber((d) => d.getHours());
  }

  minute(): Series<number> {
    return this._mapNumber((d) => d.getMinutes());
  }

  second(): Series<number> {
    return this._mapNumber((d) => d.getSeconds());
  }

  dayOfWeek(): Series<number> {
    return this._mapNumber((d) => d.getDay());
  }

  dayOfYear(): Series<number> {
    return this._mapNumber((d) => {
      const start = new Date(d.getFullYear(), 0, 0);
      const diff = d.getTime() - start.getTime();
      const oneDay = 1000 * 60 * 60 * 24;
      return Math.floor(diff / oneDay);
    });
  }

  weekNumber(): Series<number> {
    return this._mapNumber((d) => {
      const target = new Date(d.getTime());
      target.setHours(0, 0, 0, 0);
      // ISO week: Thursday in current week decides the year
      target.setDate(target.getDate() + 3 - ((target.getDay() + 6) % 7));
      const jan4 = new Date(target.getFullYear(), 0, 4);
      const dayDiff = (target.getTime() - jan4.getTime()) / (1000 * 60 * 60 * 24);
      return 1 + Math.round((dayDiff - 3 + ((jan4.getDay() + 6) % 7)) / 7);
    });
  }

  quarter(): Series<number> {
    return this._mapNumber((d) => Math.floor(d.getMonth() / 3) + 1);
  }

  timestamp(): Series<number> {
    return this._mapNumber((d) => d.getTime());
  }

  format(pattern: string): Series<string> {
    const results: (string | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(formatDate(val, pattern));
      }
    }
    return new Series<string>(this._series.name, Utf8Column.from(results));
  }

  truncate(unit: TimeUnit): Series<Date> {
    const results: (Date | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(truncateDate(val, unit));
      }
    }
    return new Series<Date>(this._series.name, DateColumn.from(results));
  }

  private _mapNumber(fn: (value: Date) => number): Series<number> {
    const results: (number | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(fn(val));
      }
    }
    return new Series<number>(this._series.name, Float64Column.from(results));
  }
}

function formatDate(date: Date, pattern: string): string {
  const pad2 = (n: number): string => String(n).padStart(2, '0');
  const pad4 = (n: number): string => String(n).padStart(4, '0');

  return pattern
    .replace('YYYY', pad4(date.getFullYear()))
    .replace('MM', pad2(date.getMonth() + 1))
    .replace('DD', pad2(date.getDate()))
    .replace('HH', pad2(date.getHours()))
    .replace('mm', pad2(date.getMinutes()))
    .replace('ss', pad2(date.getSeconds()));
}

function truncateDate(date: Date, unit: TimeUnit): Date {
  switch (unit) {
    case 'year':
      return new Date(date.getFullYear(), 0, 1);
    case 'month':
      return new Date(date.getFullYear(), date.getMonth(), 1);
    case 'day':
      return new Date(date.getFullYear(), date.getMonth(), date.getDate());
    case 'hour':
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
      );
    case 'minute':
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
        date.getMinutes(),
      );
    case 'second':
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
        date.getMinutes(),
        date.getSeconds(),
      );
  }
}
