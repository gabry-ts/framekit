import { Expr } from './expr';
import { StringExprAccessor } from './string-expr';
import { DateExprAccessor } from './date-expr';

// Augment the Expr class with .str and .dt getters
declare module './expr' {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface Expr<T> {
    readonly str: StringExprAccessor;
    readonly dt: DateExprAccessor;
  }
}

Object.defineProperty(Expr.prototype, 'str', {
  get(this: Expr<string>) {
    return new StringExprAccessor(this);
  },
  configurable: true,
});

Object.defineProperty(Expr.prototype, 'dt', {
  get(this: Expr<Date>) {
    return new DateExprAccessor(this);
  },
  configurable: true,
});
