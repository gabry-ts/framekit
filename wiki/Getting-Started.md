# Getting Started

## Install

```bash
npm install framekit
```

## First Pipeline

```ts
import { DataFrame, col } from 'framekit';

const df = DataFrame.fromRows([
  { city: 'Rome', temp: 26 },
  { city: 'Milan', temp: 31 },
  { city: 'Turin', temp: 28 },
]);

const out = df.filter(col<number>('temp').gt(27)).sortBy('temp', 'desc');

console.log(out.toArray());
```

## Next Steps

- Use `docs/guides/lazy-vs-eager.md` for execution strategy.
- Use `docs/reference/dataframe-api.md` for full DataFrame methods.
- Use `docs/guides/migration-arquero.md` for migration mapping.
